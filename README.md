# Google Drive Browser

A cross-platform desktop app for browsing, downloading, and permanently deleting files in Google Drive — including orphaned files with no parent folder. Built to handle drives with 50,000+ files.

## Features

- Full Drive Index — Scans every file and folder, cached in SQLite for instant queries
- Orphaned File Detection — Finds files whose parent was deleted or missing
- Virtual Scrolling — Renders only visible rows, so 50k+ file lists stay smooth
- Multi-Select — Click, Shift+click (range), Ctrl/Cmd+click (toggle), Select All
- Download — Native formats (Docs→docx, Sheets→xlsx, Slides→pptx, Drawings→png, others→PDF)
- Permanent Delete — Bypasses trash, permanently removes files
- Search & Sort — Filter by name, sort by name/size/type/date
- Folder Tree — Expandable sidebar with lazy-loaded subfolders
- Cross-Platform — Windows, macOS, Linux

## Prerequisites

- Python 3.10+ — python.org/downloads
- Node.js 18+ — nodejs.org
- Google Cloud Project with Drive API enabled

## Google Cloud Setup

### 1. Create a Google Cloud Project
1. Go to console.cloud.google.com
2. Click project dropdown → New Project → name it "Drive Browser" → Create

### 2. Enable the Google Drive API
1. APIs & Services → Library → search "Google Drive API" → Enable

### 3. Configure OAuth Consent Screen
1. APIs & Services → OAuth consent screen → External → Create
2. Fill in app name, support email, developer email → Save through remaining steps
3. Add your Google account as a test user

### 4. Create OAuth Credentials
1. APIs & Services → Credentials → + Create Credentials → OAuth client ID
2. Application type: Desktop app → name it → Create
3. Download JSON → save as `credentials.json` in project root
4. Ensure redirect_uris includes `http://localhost:8085`

## Installation & Running

### Windows
```
start.bat
```

### macOS / Linux
```bash
chmod +x start.sh
./start.sh
```

### Manual
```bash
pip install -r backend/requirements.txt
npm install
npx electron .
```

## Architecture

```
Electron (UI) <-> FastAPI (localhost:5000) <-> Google Drive API v3
                        |
                  SQLite (file cache)
```

## Security Notes

- OAuth tokens stored locally in token.json (gitignored)
- Backend runs only on 127.0.0.1
- credentials.json contains your client secret — don't commit it
- Full drive scope required for permanent delete

## License
MIT
