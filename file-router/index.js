import { getDbPool } from './db.js';
import { Storage } from '@google-cloud/storage';
import { parse } from 'csv-parse/sync';

const storage = new Storage();

export const fileRouter = async (event) => {
  try {
    console.log("Received event");

    // Decode Pub/Sub message
    const gcsEvent = event.data;

    const fileName = gcsEvent.name;
    const sourceBucket = gcsEvent.bucket;
    const metadata = gcsEvent.metadata || {};
    const vendor = metadata.vendor;

    // Guard against malformed event
    if (!fileName || !sourceBucket) {
      console.error("Invalid GCS event payload");
      throw new Error("Malformed GCS event");
    }

    console.log("File:", fileName);
    console.log("Vendor:", vendor);

    const pool = await getDbPool();

    // Step 1: Idempotency check
    const existingRecord = await pool.query(
      'SELECT id FROM file_audit WHERE file_name = $1',
      [fileName]
    );

    if (existingRecord.rows.length > 0) {
      console.log("File already processed. Skipping:", fileName);
      return;
    }

    // Step 2: Metadata validation
    if (!vendor) {
      console.log("Missing vendor metadata. Routing to quarantine.");

      const quarantineBucket = "quarantine-files-krisha-zalaria-1770704432";

      await storage
        .bucket(sourceBucket)
        .file(fileName)
        .copy(storage.bucket(quarantineBucket).file(fileName));

      await pool.query(
        `INSERT INTO file_audit 
          (file_name, source_bucket, destination_bucket, status, error_message)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          fileName,
          sourceBucket,
          quarantineBucket,
          "FAILED_METADATA",
          "vendor metadata missing"
        ]
      );

      return; // Business error → no retry
    }

    // Step 3: Read routing config
    const configBucket = "config-files-krisha-zalaria-1770704432";
    const configFileName = "routing-config.csv";

    const [configFile] = await storage
      .bucket(configBucket)
      .file(configFileName)
      .download();

    const configContent = configFile.toString();

    const records = parse(configContent, {
      columns: true,
      skip_empty_lines: true,
    });

    // Case-insensitive match
    const matchedRule = records.find(
      (record) =>
        record.vendor.trim().toLowerCase() === vendor.trim().toLowerCase()
    );

    // Step 4: No matching rule
    if (!matchedRule) {
      console.log("No routing rule found. Routing to unmapped.");

      const unmappedBucket = "unmapped-files-krisha-zalaria-1770704432";

      await storage
        .bucket(sourceBucket)
        .file(fileName)
        .copy(storage.bucket(unmappedBucket).file(fileName));

      await pool.query(
        `INSERT INTO file_audit 
          (file_name, source_bucket, destination_bucket, status, error_message)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          fileName,
          sourceBucket,
          unmappedBucket,
          "FAILED_CONFIG",
          "No routing rule found"
        ]
      );

      return; // Business error → no retry
    }

    // Step 5: Successful routing
    const processedBucket = "processed-files-krisha-zalaria-1770704432";
    const destinationFolder = matchedRule.destination_folder;
    const destinationPath = `${destinationFolder}/${fileName}`;
    const fullDestinationPath = `${processedBucket}/${destinationFolder}`;

    const startTime = Date.now();

    try {
      await storage
        .bucket(sourceBucket)
        .file(fileName)
        .copy(storage.bucket(processedBucket).file(destinationPath));

      const processingTime = Date.now() - startTime;

      await pool.query(
        `INSERT INTO file_audit 
          (file_name, source_bucket, destination_bucket, status, processing_time_ms)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          fileName,
          sourceBucket,
          fullDestinationPath,
          "SUCCESS",
          processingTime
        ]
      );

      console.log("File processed successfully:", fileName);

    } catch (err) {
      console.error("Copy or DB insert failed:", err);
      throw err; // System error → allow retry
    }

  } catch (err) {
    console.error("Unhandled error:", err);
    throw err; // Ensure system errors trigger retry
  }
};