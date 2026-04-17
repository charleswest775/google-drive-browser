const { app, BrowserWindow, ipcMain, dialog } = require("electron");
const path = require("path");
const fs = require("fs");
const { spawn } = require("child_process");
const http = require("http");

const BACKEND_PORT = 5055;
const BACKEND_URL = `http://127.0.0.1:${BACKEND_PORT}`;

let mainWindow;
let backendProcess;

function createWindow() {
  mainWindow = new BrowserWindow({
    width: 1400, height: 900, minWidth: 900, minHeight: 600,
    title: "Google Drive Browser",
    webPreferences: {
      preload: path.join(__dirname, "preload.js"),
      contextIsolation: true, nodeIntegration: false,
    },
  });
  mainWindow.loadFile(path.join(__dirname, "renderer", "index.html"));
  mainWindow.on("closed", () => { mainWindow = null; });
}

function startBackend() {
  const venvPython = process.platform === "win32"
    ? path.join(__dirname, ".venv", "Scripts", "python.exe")
    : path.join(__dirname, ".venv", "bin", "python3");
  const pythonCmd = fs.existsSync(venvPython)
    ? venvPython
    : (process.platform === "win32" ? "python" : "python3");
  const serverPath = path.join(__dirname, "backend", "server.py");
  const env = { ...process.env, BACKEND_PORT: String(BACKEND_PORT) };
  backendProcess = spawn(pythonCmd, [serverPath], {
    cwd: path.join(__dirname, "backend"), env, stdio: ["pipe", "pipe", "pipe"],
  });
  backendProcess.stdout.on("data", (data) => console.log(`[backend] ${data.toString().trim()}`));
  backendProcess.stderr.on("data", (data) => console.log(`[backend] ${data.toString().trim()}`));
  backendProcess.on("close", (code) => console.log(`[backend] exited with code ${code}`));
  backendProcess.on("error", (err) => {
    console.error(`[backend] failed to start:`, err.message);
    dialog.showErrorBox("Backend Error",
      `Could not start the Python backend.\n\nMake sure Python 3.10+ is installed and in your PATH.\n\nError: ${err.message}`
    );
  });
}

function waitForBackend(maxRetries = 30, interval = 1000) {
  return new Promise((resolve, reject) => {
    let retries = 0;
    const check = () => {
      const req = http.get(`${BACKEND_URL}/health`, (res) => {
        if (res.statusCode === 200) resolve(); else retry();
      });
      req.on("error", () => retry());
      req.setTimeout(2000, () => { req.destroy(); retry(); });
    };
    const retry = () => {
      retries++;
      if (retries >= maxRetries) reject(new Error("Backend did not start in time"));
      else setTimeout(check, interval);
    };
    check();
  });
}

ipcMain.handle("select-directory", async () => {
  const result = await dialog.showOpenDialog(mainWindow, {
    properties: ["openDirectory", "createDirectory"], title: "Choose download location",
  });
  if (result.canceled) return null;
  return result.filePaths[0];
});

ipcMain.handle("get-backend-url", () => BACKEND_URL);

app.whenReady().then(async () => {
  startBackend();
  try { await waitForBackend(); console.log("[main] Backend is ready"); } catch (e) { console.error("[main]", e.message); }
  createWindow();
  app.on("activate", () => { if (BrowserWindow.getAllWindows().length === 0) createWindow(); });
});

app.on("window-all-closed", () => { if (backendProcess) backendProcess.kill(); if (process.platform !== "darwin") app.quit(); });
app.on("before-quit", () => { if (backendProcess) backendProcess.kill(); });
