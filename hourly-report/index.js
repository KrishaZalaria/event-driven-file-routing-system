import { getDbPool } from './db.js';
import { Storage } from '@google-cloud/storage';

const storage = new Storage();
const REPORTS_BUCKET = 'hourly-reports-krisha-zalaria-1770704432';

export const hourlyReport = async (req, res) => {
  try {
    console.log("Hourly report triggered");

    const pool = await getDbPool();

    // Summary query
    const summaryResult = await pool.query(`
      SELECT
        COUNT(*) AS total_files,
        COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success_count,
        COALESCE(SUM(CASE WHEN status != 'SUCCESS' THEN 1 ELSE 0 END), 0) AS failure_count
      FROM public.file_audit
      WHERE processed_at >= NOW() - INTERVAL '60 minutes';
    `);

    // Breakdown query
    const breakdownResult = await pool.query(`
      SELECT
        destination_bucket,
        COUNT(*) AS file_count
      FROM public.file_audit
      WHERE processed_at >= NOW() - INTERVAL '60 minutes'
      GROUP BY destination_bucket;
    `);

    const summary = summaryResult.rows[0];
    const breakdown = breakdownResult.rows;

    // Generate CSV content
    let csvContent = '';
    const now = new Date();

    csvContent += `report_generated_at,${now.toISOString()}\n\n`;
    csvContent += `total_files,${summary.total_files}\n`;
    csvContent += `success_count,${summary.success_count}\n`;
    csvContent += `failure_count,${summary.failure_count}\n\n`;
    csvContent += `destination_bucket,file_count\n`;

    breakdown.forEach(row => {
      csvContent += `${row.destination_bucket},${row.file_count}\n`;
    });

    // -------- New Naming Strategy --------

    const executionTimestamp = now.toISOString().replace(/:/g, '-');
    const hourPrefix = now.toISOString().slice(0, 13); // YYYY-MM-DDTHH

    const uniqueFilename = `report-${executionTimestamp}.csv`;
    const latestFilename = `report-${hourPrefix}-latest.csv`;

    // Upload unique file (history preserved)
    await storage
      .bucket(REPORTS_BUCKET)
      .file(uniqueFilename)
      .save(csvContent, { contentType: 'text/csv' });

    console.log(`Unique report uploaded: ${uniqueFilename}`);

    // Upload / overwrite latest file (stable reference)
    await storage
      .bucket(REPORTS_BUCKET)
      .file(latestFilename)
      .save(csvContent, { contentType: 'text/csv' });

    console.log(`Latest report updated: ${latestFilename}`);

    res.status(200).json({
      message: 'Report generated successfully',
      unique_file: uniqueFilename,
      latest_file: latestFilename,
      summary,
      breakdown
    });

  } catch (error) {
    console.error("Error generating hourly report:", error);
    res.status(500).send("Internal error");
  }
};