# Challenge 1: Processing Pipeline

## Project Purpose

This project addresses the task of creating a data downloading and processing pipeline. The primary goal is to download a large `.tar.gz` file containing lists of organizations and customers, decompress and unzip the CSVs within, parse them, and finally, add the data to an SQLite database.

## Context

In this project, we are dealing with a specific task related to data processing. The focus is on utilizing Node.js's Streams API for efficiency in handling large files. The project involves downloading a dataset from a cloud storage location, extracting it, and populating an SQLite database with the extracted data.

## Key Features

- **Data Downloading:** Utilizes Node.js's Streams API to download a `.tar.gz` file from a specified URL.
- **Data Extraction:** Uses streaming APIs to decompress the GZIP part of the file and extract the TAR archive.
- **Database Setup:** Utilizes the `knex` library to set up an SQLite database with appropriate tables.
- **CSV Parsing and Database Insertion:** Uses streaming APIs to read CSV files, converting and inserting data into the SQLite database, while being compatible with large files.

## Constraints

- **Language:** TypeScript is used with explicit typing for variables and functions.
- **Streams API:** Node.js's Streams API is a requirement for downloading and processing files efficiently.
- **Functional Style:** Adopts a functional, immutable code style using `const` wherever possible.
- **Dependency Management:** New libraries can be installed via `npm install` with a focus on the recommended ones in the instructions.

## Getting Started

To run the project, install the necessary dependencies:

```sh
npm install
npm install --global tsx
```

Ensure that Node version 18 or greater is being used.

## Testing
To test the code, run:

```sh
tsx runner.ts
```

Additionally, run npx tsc to ensure that the code passes all TypeScript compiler checks.

## Database Requirements
- Automatically manage SQL table creation and column definitions.
- Include a unique, auto-incrementing primary key column named id.
- Ensure all columns are non-nullable.
