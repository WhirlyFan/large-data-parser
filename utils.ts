import fs from "fs";
import https from "https";
import path from "path";
import zlib from "zlib";
import tar from "tar";
import Knex, { Knex as KnexTypes } from "knex";
import fastcsv from "fast-csv";

/**
 * Create a directory with the given name at the specified path if it doesn't already exist.
 * @param {string} baseDir - The base directory path.
 * @param {string} name - The name of the directory to create.
 * @returns {Promise<string | undefined>} - A Promise that resolves when the directory is created.
 */
export const createDir = (
  baseDir: string,
  name: string
): Promise<string | undefined> => {
  const dir = path.join(baseDir, name);
  return fs.promises.mkdir(dir, { recursive: true });
};

/**
 * Download a file from the given URL and save it to the specified path.
 * @param {string} fileUrl - The URL of the file to download.
 * @param {string} downloadPath - The path where the downloaded file should be saved.
 * @returns {Promise<string>} - A Promise that resolves when the file is downloaded.
 */
export const downloadFile = (
  fileUrl: string,
  downloadPath: string
): Promise<string> => {
  return new Promise((resolve, reject) => {
    // Check if the file already exists
    if (fs.existsSync(downloadPath)) {
      resolve(`File already exists at ${downloadPath} Skipping download.`);
      return;
    }
    // Create a write stream to save the file
    const fileStream = fs.createWriteStream(downloadPath);

    // Download the file using HTTPS
    https
      .get(fileUrl, (response) => {
        if (response.statusCode !== 200) {
          reject(new Error(`HTTPS Status Code ${response.statusCode}`));
          return;
        }

        response.pipe(fileStream).on("finish", () => {
          resolve(`Download complete. File saved to ${downloadPath}`);
        });
      })
      .on("error", (err) => {
        reject(err);
      });
  });
};

/**
 * Get the file name without the extension (e.g. "dump.tar.gz" -> "dump")
 * @param {string} filePath - The path to the file
 * @returns The file name without the extension
 */
export const getFileNameWithoutExtension = (filePath: string) => {
  const baseName = path.basename(filePath, path.extname(filePath)); // removes extension
  const fileNameWithoutTarGz = baseName.replace(".tar", ""); // removes ".tar"
  return fileNameWithoutTarGz;
};

/**
 * Extract a GZIP-compressed TAR file to the specified directory.
 * @param {string} sourcePath - The path to the GZIP-compressed TAR file.
 * @param {string} destinationPath - The path where the contents of the TAR file should be extracted.
 * @returns {Promise<string>} - A Promise that resolves when the extraction is complete.
 */
export const extractFile = (
  sourcePath: string,
  destinationPath: string
): Promise<string> => {
  return new Promise((resolve, reject) => {
    const extractedDirName =
      destinationPath + "/" + getFileNameWithoutExtension(sourcePath);
    // Check if the destination directory already exists
    if (fs.existsSync(extractedDirName)) {
      resolve(
        `Destination directory already exists at ${extractedDirName} Skipping extraction.`
      );
      return;
    }
    fs.createReadStream(sourcePath)
      // Decompress GZIP part
      .pipe(zlib.createGunzip())
      // Extract TAR archive
      .pipe(tar.extract({ cwd: destinationPath }))
      .on("finish", () => {
        resolve(`Extraction complete. Files saved to ${destinationPath}`);
      })
      .on("error", (err) => {
        reject(err);
      });
  });
};

/**
 * This is a helper function that returns a list of CSV files in a directory.
 * @param {string} dir - The directory to search for CSV files.
 * @returns {Promise<string[]>} A list of CSV files in the directory.
 */
export const getCSVFilesInDir = async (dir: string): Promise<string[]> => {
  try {
    // Read the directory
    const files = await fs.promises.readdir(dir);

    // Filter files with ".csv" extension and exclude files starting with a dot
    const csvFiles = files.filter((file) => {
      const isCSVFile = path.extname(file).toLowerCase() === ".csv";
      const doesNotStartWithDot = !file.startsWith(".");
      return isCSVFile && doesNotStartWithDot;
    });

    return csvFiles;
  } catch (error) {
    console.error(`Error reading directory ${dir}:`, error);
    return [];
  }
};

/**
 * This is a helper function that returns the headers of a CSV file.
 * This in combination with pandas-js would be able to create a table with the correct data types after parsing the file for types
 * However, this is not implemented in this challenge as it's inefficient to parse the file twice and we know the data types of the columns
 * @param csvFilePath - The path to the CSV file
 * @param fileName - The name of the CSV file
 * @returns A list of headers in the CSV file
 */
export const getCSVHeaders = (csvFilePath: string): Promise<string[]> => {
  return new Promise((resolve, reject) => {
    const stream = fs
      .createReadStream(csvFilePath)
      .pipe(fastcsv.parse())
      .on("data", (data) => {
        // Once the first row is parsed, we have the headers
        // Stop parsing the rest of the file
        stream.destroy();
        resolve(data);
      })
      .on("error", (error) => {
        reject(error);
      })
      .on("end", () => {
        reject(new Error("CSV file is empty or does not contain headers."));
      });

    // Handle the case where the stream is not destroyed (e.g., empty CSV)
    stream.on("close", () => {
      reject(new Error("CSV file is empty or does not contain headers."));
    });
  });
};

/**
 * This is a helper function that inserts the CSV data into the database.
 * @param {KnexTypes} db - The database connection
 * @param {string} csvFilePath - The path to the CSV file
 * @param {string} tableName - The name of the table to insert the data into
 * @param batchSize - The number of rows to insert at a time
 * @returns {Promise<string>} A Promise that resolves when the data is inserted
 */
const processCSV = async (
  db: KnexTypes,
  csvFilePath: string,
  tableName: string,
  batchSize: number = 100
) => {
  return new Promise((resolve, reject) => {
    // Use an array to keep track of the number of rows inserted to avoid using let
    const count = [0];
    const batch: object[] = [];
    const stream = fs
      .createReadStream(csvFilePath)
      .pipe(fastcsv.parse({ headers: true }))
      .on("error", (error) => reject(error))
      .on("data", async (data) => {
        // Convert Subscription Date to a Date object
        if (data["Subscription Date"]) {
          data["Subscription Date"] = new Date(data["Subscription Date"]);
        }
        // Convert Number of Employees to an integer
        if (data["Number of Employees"]) {
          data["Number of Employees"] = parseInt(data["Number of Employees"]);
        }
        // Insert the batch when it reaches the specified size (batchSize)
        batch.push(data);
        if (batch.length >= batchSize) {
          // IMPORTANT: This allows for scaling to large files by pausing the stream while the batch is inserted
          // This approach increases time complexity, but allows to scale to large files without running out of memory
          // This can be improved by increasing the batch size and/or using a more efficient database like PostgreSQL
          stream.pause();
          try {
            const currentBatch = batch.splice(0, batchSize);
            await db.batchInsert(tableName, currentBatch);
            count[0] += currentBatch.length;
            console.log(`Inserted ${count[0]} rows into table "${tableName}"`);
            // Resume the stream to continue parsing the file
            stream.resume();
          } catch (error) {
            reject(error);
          }
        }
      })
      .on("end", async () => {
        // Insert any remaining rows
        if (batch.length > 0) {
          try {
            await db.batchInsert(tableName, batch);
            resolve("Data inserted successfully.");
          } catch (error) {
            reject(error);
          }
        } else {
          resolve("Data inserted successfully.");
        }
      });
  });
};

/**
 * This is the main function that creates the database.
 * @param {string} __dirname - The directory name of the current module
 * @param {string} csvPath - The path to the directory containing the CSV files
 * @param {KnexTypes.Config} knexConfig - The Knex configuration object
 * @returns {Promise<void>} A Promise that resolves when the database is created
 */
export const createDatabase = async (
  __dirname: string,
  csvPath: string,
  knexConfig: KnexTypes.Config
): Promise<void> => {
  const db: KnexTypes = Knex(knexConfig);
  try {
    const csvFiles = await getCSVFilesInDir(csvPath);
    // Use Promise.all to wait for all async operations to complete
    await Promise.all(
      csvFiles.map(async (file) => {
        const csvFilePath = path.join(csvPath, file);
        const tableName = getFileNameWithoutExtension(file);
        const headers = await getCSVHeaders(csvFilePath);
        await db.schema.dropTableIfExists(tableName);

        // Create the table if it doesn't already exist with the headers as columns
        console.log(`Creating table "${tableName}"`);
        await db.schema.createTable(tableName, (table) => {
          // Add an auto-incrementing primary key column
          // Note: Database should have and id and Index column to preserve use of all headers in CSV and have an id column per the instructions
          table.increments("id").primary();
          headers.forEach((header) => {
            // tableName and headers are known so we can hardcode the data types
            // This could be done dynamically by parsing the CSV file for the data types
            // I've opted not to do this as it's inefficient to parse the file twice especially for large files
            if (tableName === "customers") {
              switch (header) {
                case "Index":
                  table.integer(header).notNullable();
                  break;
                case "Subscription Date":
                  table.date(header).notNullable();
                  break;
                default:
                  table.string(header).notNullable();
                  break;
              }
            }
            if (tableName === "organizations") {
              switch (header) {
                case "Index":
                  table.integer(header).notNullable();
                  break;
                case "Number of Employees":
                  table.integer(header).notNullable();
                  break;
                default:
                  table.string(header).notNullable();
                  break;
              }
            }
          });
        });

        // Insert the CSV data into the table using the helper function
        await processCSV(db, csvFilePath, tableName);
      })
    );

    console.log("Database Successfully Created");
  } catch (err) {
    console.error("Error creating the database:", err);
  } finally {
    // Close the database connection
    db.destroy();
  }
};
