#!/bin/bash
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
echo "=== Google Drive Browser ==="
if ! command -v python3 &>/dev/null; then echo "ERROR: python3 not found."; exit 1; fi
if ! command -v node &>/dev/null; then echo "ERROR: node not found."; exit 1; fi
if ! python3 -c "import fastapi" 2>/dev/null; then echo "Installing Python dependencies..."; pip3 install -r backend/requirements.txt; fi
if [ ! -d "node_modules" ]; then echo "Installing Node dependencies..."; npm install; fi
if [ ! -f "credentials.json" ]; then echo "WARNING: credentials.json not found! See README.md"; fi
echo "Starting app..."
npx electron .
