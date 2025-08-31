/**
 * Drive Tree Scanner -> Sheet/CSV
 * - Recursively scans a Drive folder and subfolders
 * - Collects: Path, Name, FileId, MimeType, SizeBytes, ModifiedTime
 * - Writes to Google Sheet (append/replace) or to a local CSV
 *
 * Usage (locally with ADC):
 *   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json  (if not on Cloud Run)
 *   export ROOT_FOLDER_ID=xxxxxxxxxxxxxxxxxxxxxxxx
 *   export SHEET_ID=yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy
 *   export SHEET_NAME=ImportScan
 *   export WRITE_MODE=sheet   # or csv
 *   export CSV_PATH=./drive_inventory.csv
 *   node scanDrive.js
 */

const { google } = require("googleapis");
const fs = require("fs");
const path = require("path");

// ===== Config via env =====
const ROOT_FOLDER_ID = process.env.ROOT_FOLDER_ID || "";
const SHEET_ID = process.env.SHEET_ID || "";
const SHEET_NAME = process.env.SHEET_NAME || "ImportScan";
const WRITE_MODE = (process.env.WRITE_MODE || "sheet").toLowerCase(); // "sheet" | "csv"
const CSV_PATH = process.env.CSV_PATH || path.join(process.cwd(), "drive_inventory.csv");

// Behavior: replace sheet content? If false, it appends below the header.
const REPLACE_SHEET_CONTENT = true;

// Google-native folder MIME type
const FOLDER_MIME = "application/vnd.google-apps.folder";

// Backoff helpers
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Auth (ADC: works on Cloud Run; locally use GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default login)
async function getAuth() {
  const auth = await google.auth.getClient({
    scopes: [
      "https://www.googleapis.com/auth/drive.readonly",
      "https://www.googleapis.com/auth/spreadsheets",
    ],
  });
  return auth;
}

async function listChildren(drive, parentId, pageToken) {
  // List direct children of a folder (files + folders)
  // We include supportsAllDrives: true in case the parent is in a Shared Drive or shared file
  const q = `'${parentId}' in parents and trashed = false`;
  const resp = await drive.files.list({
    q,
    fields: "nextPageToken, files(id, name, mimeType, size, modifiedTime)",
    pageSize: 1000,
    pageToken,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    corpora: "allDrives",
  });
  return resp.data;
}

async function robustListChildren(drive, parentId) {
  // Wraps listChildren with basic retry for 429/5xx
  const results = [];
  let pageToken = undefined;

  while (true) {
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        const data = await listChildren(drive, parentId, pageToken);
        (data.files || []).forEach((f) => results.push(f));
        pageToken = data.nextPageToken || undefined;
        break; // success
      } catch (err) {
        const status = err?.code || err?.response?.status;
        if (status === 429 || (status >= 500 && status < 600)) {
          const delay = 500 * Math.pow(2, attempt);
          console.warn(`listChildren retry (${attempt + 1}) after ${delay}ms: ${status || err.message}`);
          await sleep(delay);
          continue;
        }
        throw err;
      }
    }
    if (!pageToken) break;
  }

  return results;
}

async function scanTree(drive, rootId) {
  // BFS traversal to avoid deep recursion limits
  const queue = [{ id: rootId, path: "/" }];
  const filesOut = [];

  while (queue.length) {
    const node = queue.shift();
    const items = await robustListChildren(drive, node.id);

    for (const item of items) {
      const isFolder = item.mimeType === FOLDER_MIME;
      const childPath = path.posix.join(node.path, item.name);

      if (isFolder) {
        queue.push({ id: item.id, path: childPath });
      } else {
        filesOut.push({
          Path: childPath,
          Name: item.name || "",
          FileId: item.id || "",
          MimeType: item.mimeType || "",
          SizeBytes: item.size ? String(item.size) : "",
          ModifiedTime: item.modifiedTime || "",
        });
      }
    }
  }

  return filesOut;
}

function toCsv(rows) {
  const headers = ["Path", "Name", "FileId", "MimeType", "SizeBytes", "ModifiedTime"];
  const esc = (s) => {
    const str = String(s ?? "");
    if (/[,"\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
    return str;
  };
  const lines = [headers.join(",")];
  for (const r of rows) {
    lines.push(
      [r.Path, r.Name, r.FileId, r.MimeType, r.SizeBytes, r.ModifiedTime].map(esc).join(",")
    );
  }
  return lines.join("\n");
}

async function writeToSheet(auth, rows) {
  if (!SHEET_ID) throw new Error("SHEET_ID is required for WRITE_MODE=sheet");

  const sheets = google.sheets({ version: "v4", auth });

  // Ensure header row
  const header = [["Path", "Name", "FileId", "MimeType", "SizeBytes", "ModifiedTime"]];
  const bodyRows = rows.map((r) => [r.Path, r.Name, r.FileId, r.MimeType, r.SizeBytes, r.ModifiedTime]);

  if (REPLACE_SHEET_CONTENT) {
    // Clear the tab, write header + data
    // 1) Clear
    await sheets.spreadsheets.values.clear({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A:Z`,
    });

    // 2) Write header + data
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1`,
      valueInputOption: "RAW",
      requestBody: {
        values: header.concat(bodyRows),
      },
    });
  } else {
    // Append below existing data (creates header only if the sheet is empty)
    // Try to detect if empty:
    const existing = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1:A1`,
      valueRenderOption: "UNFORMATTED_VALUE",
    });
    const hasHeader = (existing.data.values && existing.data.values.length > 0);

    const payload = hasHeader ? bodyRows : header.concat(bodyRows);

    await sheets.spreadsheets.values.append({
      spreadsheetId: SHEET_ID,
      range: `${SHEET_NAME}!A1`,
      valueInputOption: "RAW",
      insertDataOption: "INSERT_ROWS",
      requestBody: { values: payload },
    });
  }
}

async function main() {
  if (!ROOT_FOLDER_ID) {
    throw new Error("ROOT_FOLDER_ID is required");
  }

  const auth = await getAuth();
  const drive = google.drive({ version: "v3", auth });

  console.log("Scanning Drive starting at folder:", ROOT_FOLDER_ID);
  const rows = await scanTree(drive, ROOT_FOLDER_ID);
  console.log(`Found ${rows.length} files.`);

  if (WRITE_MODE === "csv") {
    const csv = toCsv(rows);
    fs.writeFileSync(CSV_PATH, csv, "utf8");
    console.log("CSV written to:", CSV_PATH);
  } else {
    await writeToSheet(auth, rows);
    console.log(`Wrote ${rows.length} rows to Sheet '${SHEET_NAME}' in ${SHEET_ID}`);
  }

  console.log("Done.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
