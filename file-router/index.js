import { getDbPool } from './db.js';

export const fileRouter = async (event) => {
  try {
    console.log("Received event");

    // Step 1: Decode Pub/Sub message
    const pubsubMessage = event.data;
    const decodedMessage = Buffer.from(pubsubMessage, 'base64').toString();
    const gcsEvent = JSON.parse(decodedMessage);

    console.log("Parsed GCS Event:", gcsEvent);

    const fileName = gcsEvent.name;
    const sourceBucket = gcsEvent.bucket;
    const metadata = gcsEvent.metadata || {};
    const vendor = metadata.vendor;

    console.log("File:", fileName);
    console.log("Source Bucket:", sourceBucket);
    console.log("Vendor Metadata:", vendor);

    // We will add routing logic here next

    return;
  } catch (err) {
    console.error("Unhandled error:", err);
    throw err; // System error → allow retry
  }
};