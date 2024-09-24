const AWS = require('aws-sdk');
const yauzl = require('yauzl');
 
const s3 = new AWS.S3({
    accessKeyId: '',//accessKeyId
    secretAccessKey: '',//secretAccessKey
    region: 'ap-south-1'
});
 
const sqs = new AWS.SQS({
    region: 'ap-south-1'
});
 
const bucketName = 'pearlcruit-resume-files';
const sqsQueueUrl = '';//sqsQueueUrl
 
module.exports.handler = async (event) => {
    console.log("Event received:", event);
    console.log("key:", event.Records[0].s3.object.key);
 
    const record = event.Records[0];
    const zipFileName = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
 
    console.log("Zip file name:", zipFileName);
 
    try {
        const { uploadedFiles, createdBy ,organization,processId }  = await extractAndUpload(zipFileName);
        console.log('Extraction and upload completed');
        const messageObject = {
            uploadedFiles: uploadedFiles,
            createdBy: createdBy,
            organization : organization,
            zipFileName: zipFileName,
            processId: processId
        };
        await sendMessageToSQS(messageObject);
    } catch (err) {
        console.error('Error during extraction and upload:', err);
    }
};
 
async function extractAndUpload(zipFileName) {
    const uploadedFiles = [];
    let createdBy = null;
    let organization = null;
    let processId = null;
    return new Promise((resolve, reject) => {
        s3.getObject({ Bucket: 'pearlcruit-zipfile-bucket', Key: zipFileName }, (err, data) => {
            if (err) {
                console.error("Error occurred while reading zip file from S3:", err);
                reject(err);
                return;
            }
            if (data.Metadata) {
                createdBy = data.Metadata?.createdby;
                organization = data.Metadata?.organization;
                processId = data.Metadata?.processid;
               
                console.log("Retrieved createdBy,processId and organization from metadata:",
                    createdBy,processId, organization);
            } else {
                console.log("createdBy and organization metadata not found.",data.Metadata);
            }
 
            yauzl.fromBuffer(data.Body, { lazyEntries: true }, (err, zipfile) => {
                if (err) {
                    console.error("Error occurred while opening zip file:", err);
                    reject(err);
                    return;
                }
 
                zipfile.readEntry();
                zipfile.on('entry', (entry) => {
                    if (/\/$/.test(entry.fileName)) {
                        zipfile.readEntry();
                    } else {
                        zipfile.openReadStream(entry, (err, readStream) => {
                            if (err) {
                                console.error("Error occurred while reading file from zip:", err);
                                reject(err);
                                return;
                            }
 
                            const params = {
                                Bucket: bucketName,
                                Key: entry.fileName,
                                Body: readStream
                            };
                            // if (entry.fileName.endsWith('.pdf')) {
                            //     params.Metadata = {
                            //         'createdby': createdBy
                            //     };
                            // }
 
                            s3.upload(params, (err, data) => {
                                if (err) {
                                    console.error("Error occurred while uploading file to S3:", err);
                                    reject(err);
                                } else {
                                    console.log("File uploaded successfully:", data.Location);
                                    uploadedFiles.push(entry.fileName);
                                    zipfile.readEntry();
                                }
                            });
                        });
                    }
                });
 
                zipfile.on('end', () => {
                    console.log("All entries read, extraction complete");
                    resolve({uploadedFiles,createdBy,organization,processId});
                });
 
                zipfile.on('error', (err) => {
                    console.error("Error occurred while extracting files:", err);
                    reject(err);
                });
            });
        });
    });
}
 
async function sendMessageToSQS(uploadedFiles) {
    const params = {
        MessageBody: JSON.stringify(uploadedFiles),
        QueueUrl: sqsQueueUrl
    };
 
    return new Promise((resolve, reject) => {
        sqs.sendMessage(params, (err, data) => {
            if (err) {
                console.error("Error sending message to SQS:", err);
                reject(err);
            } else {
                console.log("Successfully sent message to SQS:", data.MessageId);
                resolve(data);
            }
        });
    });
}