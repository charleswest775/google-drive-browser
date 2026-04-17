@echo off
cd /d "%~dp0"
echo === Google Drive Browser ===
where python >nul 2>nul || (echo ERROR: python not found. & pause & exit /b 1)
where node >nul 2>nul || (echo ERROR: node not found. & pause & exit /b 1)
python -c "import fastapi" 2>nul || (echo Installing Python deps... & pip install -r backend\requirements.txt)
if not exist "node_modules" (echo Installing Node deps... & npm install)
if not exist "credentials.json" (echo WARNING: credentials.json not found! See README.md)
echo Starting app...
npx electron .
