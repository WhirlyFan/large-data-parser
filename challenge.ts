import path from "path";
import {
  createDatabase,
  createDir,
  downloadFile,
  extractFile,
  getFileNameWithoutExtension,
} from "./utils";
import { DUMP_DOWNLOAD_URL, SQLITE_DB_PATH } from "./resources";
import { Knex as KnexTypes } from "knex";

/**
 * Process the data dump: download the file, extract it, and log completion messages.
 */
export async function processDataDump() {
  const __filename = new URL(import.meta.url).pathname;
  const __dirname = path.dirname(__filename);
  const fileUrl = DUMP_DOWNLOAD_URL;
  const downloadPath = path.join(__dirname, "tmp", "dump.tar.gz");
  const extractPath = path.join(__dirname, "tmp");
  const csvPath = path.join(
    __dirname,
    "tmp",
    getFileNameWithoutExtension(downloadPath)
  );
  const knexConfig: KnexTypes.Config = {
    client: "sqlite3",
    connection: {
      filename: path.join(__dirname, SQLITE_DB_PATH),
    },
    // Necessary to avoid a warning when using SQLite3
    useNullAsDefault: false,
  };

  try {
    // Create the 'tmp' directory if it doesn't exist
    await createDir(__dirname, "tmp");
    // Download the file
    console.log(`Downloading file from ${fileUrl}`);
    const downloadResponse = await downloadFile(fileUrl, downloadPath);
    console.log(downloadResponse);
    // Extract the file
    console.log(`Extracting file to ${extractPath}`);
    const extractResponse = await extractFile(downloadPath, extractPath);
    console.log(extractResponse);
    // Create the 'out' directory if it doesn't exist
    await createDir(__dirname, "out");
    // Create the SQLITE database using Knex and CSV Files
    console.log("Creating database...");
    await createDatabase(__dirname, csvPath, knexConfig);
  } catch (error) {
    console.error(error);
  }
}
