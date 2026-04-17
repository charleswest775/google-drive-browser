(function () {
  "use strict";

  const ROW_HEIGHT = 36;
  const OVERSCAN = 10;
  const SEARCH_DEBOUNCE = 300;

  let API = "http://127.0.0.1:5000";
  let currentView = "folder";
  let currentFolderId = "root";
  let currentSearch = "";
  let sortBy = "name";
  let sortDir = "ASC";
  let allFiles = [];
  let totalCount = 0;
  let selectedIds = new Set();
  let lastClickedIndex = -1;
  let isIndexing = false;
  let indexPollTimer = null;

  const $ = (sel) => document.querySelector(sel);
  const $$ = (sel) => document.querySelectorAll(sel);

  const authScreen = $("#auth-screen"), appScreen = $("#app-screen");
  const loginBtn = $("#login-btn"), authError = $("#auth-error"), userEmail = $("#user-email");
  const searchInput = $("#search-input"), refreshBtn = $("#refresh-btn"), logoutBtn = $("#logout-btn");
  const breadcrumb = $("#breadcrumb"), selectionCount = $("#selection-count");
  const selectAllBtn = $("#select-all-btn"), deselectBtn = $("#deselect-btn");
  const downloadBtn = $("#download-btn"), deleteBtn = $("#delete-btn");
  const fileViewport = $("#file-viewport"), fileScrollSpacer = $("#file-scroll-spacer"), fileRows = $("#file-rows");
  const loadingState = $("#loading-state"), emptyState = $("#empty-state");
  const statusText = $("#status-text"), headerCheckbox = $("#header-checkbox");
  const folderTree = $("#folder-tree"), orphanCountBadge = $("#orphan-count");
  const deleteModal = $("#delete-modal"), deleteCountSpan = $("#delete-count");
  const deleteCancelBtn = $("#delete-cancel"), deleteConfirmBtn = $("#delete-confirm");
  const downloadModal = $("#download-modal"), dlProgressBar = $("#dl-progress-bar");
  const dlProgressText = $("#dl-progress-text"), dlItemsList = $("#dl-items-list"), dlCloseBtn = $("#dl-close");
  const indexingOverlay = $("#indexing-overlay"), indexingCount = $("#indexing-count");
  const indexingPages = $("#indexing-pages"), indexingBar = $("#indexing-bar");
  const statTotal = $("#stat-total"), statFolders = $("#stat-folders"), statOrphans = $("#stat-orphans");

  async function api(path, options = {}) {
    const resp = await fetch(API + path, { headers: { "Content-Type": "application/json" }, ...options });
    if (!resp.ok) throw new Error(`API ${resp.status}: ${await resp.text()}`);
    return resp.json();
  }

  async function init() {
    try { API = await window.electronAPI.getBackendUrl(); } catch {}
    await checkAuth();
    bindEvents();
  }

  async function checkAuth() {
    try { const data = await api("/auth/status"); if (data.authenticated) showApp(data.user); else showAuth(); } catch { showAuth(); }
  }

  function showAuth() { authScreen.style.display = "flex"; appScreen.style.display = "none"; }

  async function showApp(user) {
    authScreen.style.display = "none"; appScreen.style.display = "";
    if (user) userEmail.textContent = user.email || "";
    const status = await api("/index/status");
    if (status.cache_stats.total_files === 0 && !status.is_indexing) startIndexing();
    else if (status.is_indexing) { showIndexingUI(status); pollIndexStatus(); }
    else { loadCurrentView(); updateStats(); }
  }

  function bindEvents() {
    loginBtn.addEventListener("click", doLogin);
    logoutBtn.addEventListener("click", doLogout);
    refreshBtn.addEventListener("click", () => startIndexing());
    searchInput.addEventListener("input", debounce(onSearch, SEARCH_DEBOUNCE));
    selectAllBtn.addEventListener("click", doSelectAll);
    deselectBtn.addEventListener("click", doDeselect);
    downloadBtn.addEventListener("click", onDownload);
    deleteBtn.addEventListener("click", onDeleteClick);
    deleteCancelBtn.addEventListener("click", () => (deleteModal.style.display = "none"));
    deleteConfirmBtn.addEventListener("click", doDelete);
    dlCloseBtn.addEventListener("click", () => (downloadModal.style.display = "none"));
    $$(".sortable").forEach((el) => {
      el.addEventListener("click", () => {
        const col = el.dataset.sort;
        if (sortBy === col) sortDir = sortDir === "ASC" ? "DESC" : "ASC";
        else { sortBy = col; sortDir = "ASC"; }
        updateSortUI(); loadCurrentView();
      });
    });
    headerCheckbox.addEventListener("change", () => { if (headerCheckbox.checked) doSelectAll(); else doDeselect(); });
    fileViewport.addEventListener("scroll", renderVisibleRows);
    $$(".sidebar-item").forEach((item) => {
      item.addEventListener("click", () => {
        const type = item.dataset.type;
        $$(".sidebar-item").forEach((i) => i.classList.remove("active"));
        item.classList.add("active");
        if (type === "root") { currentView = "folder"; currentFolderId = "root"; currentSearch = ""; searchInput.value = ""; loadCurrentView(); }
        else if (type === "orphans") { currentView = "orphans"; currentSearch = ""; searchInput.value = ""; loadCurrentView(); }
      });
    });
  }

  async function doLogin() {
    loginBtn.disabled = true; loginBtn.textContent = "Signing in..."; authError.style.display = "none";
    try {
      const data = await api("/auth/login", { method: "POST" });
      if (data.success) await checkAuth();
      else { authError.textContent = data.error || "Login failed"; authError.style.display = ""; }
    } catch (e) { authError.textContent = e.message; authError.style.display = ""; }
    finally { loginBtn.disabled = false; loginBtn.textContent = "Sign in with Google"; }
  }

  async function doLogout() { await api("/auth/logout", { method: "POST" }); showAuth(); }

  async function startIndexing() {
    try { await api("/index/start", { method: "POST" }); showIndexingUI({ total_files: 0, pages_fetched: 0 }); pollIndexStatus(); }
    catch (e) { statusText.textContent = "Index error: " + e.message; }
  }

  function showIndexingUI(status) { isIndexing = true; indexingOverlay.style.display = "flex"; updateIndexingUI(status); }

  function updateIndexingUI(status) {
    indexingCount.textContent = (status.total_files || 0).toLocaleString();
    indexingPages.textContent = status.pages_fetched || 0;
    indexingBar.style.width = "100%"; indexingBar.style.opacity = "0.6";
    indexingBar.style.animation = "pulse 1.5s ease-in-out infinite";
  }

  function pollIndexStatus() {
    clearInterval(indexPollTimer);
    indexPollTimer = setInterval(async () => {
      try {
        const status = await api("/index/status");
        if (status.is_indexing) updateIndexingUI(status);
        else {
          clearInterval(indexPollTimer); isIndexing = false; indexingOverlay.style.display = "none";
          loadCurrentView(); updateStats();
          statusText.textContent = status.error ? "Index error: " + status.error : `Indexed ${status.cache_stats.total_files.toLocaleString()} files`;
        }
      } catch {}
    }, 1500);
  }

  async function updateStats() {
    try {
      const s = await api("/stats");
      statTotal.textContent = s.total_files.toLocaleString();
      statFolders.textContent = s.total_folders.toLocaleString();
      statOrphans.textContent = s.total_orphans.toLocaleString();
      orphanCountBadge.textContent = s.total_orphans.toLocaleString();
    } catch {}
  }

  async function loadCurrentView() {
    showLoading(true); selectedIds.clear(); updateSelectionUI();
    try {
      let endpoint;
      const params = new URLSearchParams({ offset: "0", limit: "50000", sort_by: sortBy, sort_dir: sortDir });
      if (currentView === "folder") { params.set("parent_id", currentFolderId); if (currentSearch) params.set("search", currentSearch); endpoint = "/files/children?" + params; }
      else if (currentView === "orphans") { if (currentSearch) params.set("search", currentSearch); endpoint = "/files/orphans?" + params; }
      else if (currentView === "search") { params.set("q", currentSearch); endpoint = "/files/search?" + params; }
      const data = await api(endpoint);
      allFiles = data.files || []; totalCount = data.total || allFiles.length;
      showLoading(false);
      if (allFiles.length === 0) { emptyState.style.display = "flex"; fileViewport.style.display = "none"; $("#column-headers").style.display = "none"; }
      else { emptyState.style.display = "none"; fileViewport.style.display = ""; $("#column-headers").style.display = ""; setupVirtualScroll(); }
      updateBreadcrumb(); statusText.textContent = `${totalCount.toLocaleString()} items`;
    } catch (e) { showLoading(false); statusText.textContent = "Error: " + e.message; }
  }

  function showLoading(show) { loadingState.style.display = show ? "flex" : "none"; if (show) { emptyState.style.display = "none"; fileViewport.style.display = "none"; $("#column-headers").style.display = "none"; } }

  function setupVirtualScroll() { fileScrollSpacer.style.height = allFiles.length * ROW_HEIGHT + "px"; fileViewport.scrollTop = 0; renderVisibleRows(); }

  function renderVisibleRows() {
    const scrollTop = fileViewport.scrollTop, viewHeight = fileViewport.clientHeight;
    const startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
    const endIndex = Math.min(allFiles.length, Math.ceil((scrollTop + viewHeight) / ROW_HEIGHT) + OVERSCAN);
    fileRows.style.top = startIndex * ROW_HEIGHT + "px";
    let html = "";
    for (let i = startIndex; i < endIndex; i++) {
      const f = allFiles[i], isFolder = f.is_folder === 1, isSelected = selectedIds.has(f.id);
      const icon = getFileIcon(f.mime_type, isFolder), size = isFolder ? "—" : formatSize(f.size);
      const type = formatMimeType(f.mime_type), modified = formatDate(f.modified_time), owner = f.owner || "";
      html += `<div class="file-row ${isFolder ? "folder-row" : ""} ${isSelected ? "selected" : ""}" data-index="${i}" data-id="${f.id}">
        <div class="col-check"><input type="checkbox" ${isSelected ? "checked" : ""} /></div>
        <div class="col-icon"><span class="file-icon">${icon}</span></div>
        <div class="col-name" title="${escapeHtml(f.name)}">${escapeHtml(f.name)}</div>
        <div class="col-size">${size}</div>
        <div class="col-type" title="${type}">${type}</div>
        <div class="col-modified">${modified}</div>
        <div class="col-owner" title="${escapeHtml(owner)}">${escapeHtml(owner)}</div>
      </div>`;
    }
    fileRows.innerHTML = html;
    fileRows.querySelectorAll(".file-row").forEach((row) => {
      const index = parseInt(row.dataset.index), id = row.dataset.id, f = allFiles[index];
      row.addEventListener("click", (e) => { if (e.target.tagName === "INPUT") return; handleRowClick(index, id, e); });
      row.addEventListener("dblclick", () => { if (f.is_folder === 1) navigateToFolder(f.id, f.name); });
      row.querySelector("input[type=checkbox]").addEventListener("change", () => toggleSelection(id));
    });
  }

  function handleRowClick(index, id, e) {
    if (e.shiftKey && lastClickedIndex >= 0) { const s = Math.min(lastClickedIndex, index), end = Math.max(lastClickedIndex, index); for (let i = s; i <= end; i++) selectedIds.add(allFiles[i].id); }
    else if (e.ctrlKey || e.metaKey) toggleSelection(id);
    else { selectedIds.clear(); selectedIds.add(id); }
    lastClickedIndex = index; updateSelectionUI(); renderVisibleRows();
  }

  function toggleSelection(id) { if (selectedIds.has(id)) selectedIds.delete(id); else selectedIds.add(id); updateSelectionUI(); renderVisibleRows(); }
  function doSelectAll() { allFiles.forEach((f) => selectedIds.add(f.id)); updateSelectionUI(); renderVisibleRows(); }
  function doDeselect() { selectedIds.clear(); lastClickedIndex = -1; updateSelectionUI(); renderVisibleRows(); }

  function updateSelectionUI() {
    const count = selectedIds.size, has = count > 0;
    selectionCount.style.display = has ? "" : "none"; selectionCount.textContent = `${count} selected`;
    deselectBtn.style.display = has ? "" : "none"; downloadBtn.style.display = has ? "" : "none"; deleteBtn.style.display = has ? "" : "none";
    headerCheckbox.checked = count === allFiles.length && count > 0; headerCheckbox.indeterminate = count > 0 && count < allFiles.length;
  }

  function navigateToFolder(folderId) {
    currentView = "folder"; currentFolderId = folderId; currentSearch = ""; searchInput.value = "";
    $$(".sidebar-item").forEach((i) => i.classList.toggle("active", i.dataset.id === "root" && folderId === "root"));
    loadCurrentView(); loadSubTree(folderId);
  }

  async function updateBreadcrumb() {
    if (currentView === "orphans") { breadcrumb.innerHTML = `<span class="breadcrumb-current">Orphaned Files</span>`; return; }
    if (currentView === "search") { breadcrumb.innerHTML = `<span class="breadcrumb-current">Search: "${escapeHtml(currentSearch)}"</span>`; return; }
    if (currentFolderId === "root") { breadcrumb.innerHTML = `<span class="breadcrumb-current">My Drive</span>`; return; }
    try {
      const pathItems = await api(`/files/${currentFolderId}/path`);
      let html = `<span class="breadcrumb-item" data-id="root">My Drive</span>`;
      for (let i = 0; i < pathItems.length; i++) {
        html += `<span class="breadcrumb-sep">/</span>`;
        html += i === pathItems.length - 1 ? `<span class="breadcrumb-current">${escapeHtml(pathItems[i].name)}</span>` : `<span class="breadcrumb-item" data-id="${pathItems[i].id}">${escapeHtml(pathItems[i].name)}</span>`;
      }
      breadcrumb.innerHTML = html;
      breadcrumb.querySelectorAll(".breadcrumb-item").forEach((el) => el.addEventListener("click", () => navigateToFolder(el.dataset.id)));
    } catch { breadcrumb.innerHTML = `<span class="breadcrumb-current">...</span>`; }
  }

  async function loadSubTree() {}

  async function loadRootFolders() {
    try {
      const data = await api(`/files/children?parent_id=root&limit=500&sort_by=name&sort_dir=ASC`);
      renderTree(folderTree, data.files.filter((f) => f.is_folder === 1));
    } catch {}
  }

  function renderTree(container, folders) {
    let html = "";
    for (const f of folders) html += `<div class="tree-node" data-id="${f.id}"><div class="tree-item" data-id="${f.id}"><span class="tree-toggle">▶</span><span>📁 ${escapeHtml(f.name)}</span></div><div class="tree-children" style="display:none"></div></div>`;
    container.innerHTML = html;
    container.querySelectorAll(".tree-item").forEach((el) => {
      el.addEventListener("click", async () => {
        navigateToFolder(el.dataset.id);
        const node = el.parentElement, children = node.querySelector(".tree-children"), toggle = el.querySelector(".tree-toggle");
        if (children.style.display === "none") {
          children.style.display = ""; toggle.textContent = "▼";
          if (!children.dataset.loaded) {
            const data = await api(`/files/children?parent_id=${el.dataset.id}&limit=500&sort_by=name&sort_dir=ASC`);
            renderTree(children, data.files.filter((f) => f.is_folder === 1));
            children.dataset.loaded = "true";
          }
        } else { children.style.display = "none"; toggle.textContent = "▶"; }
      });
    });
  }

  function onSearch() {
    const q = searchInput.value.trim();
    if (!q) { if (currentView === "search") { currentView = "folder"; currentFolderId = "root"; } currentSearch = ""; loadCurrentView(); return; }
    if (currentView === "folder" || currentView === "orphans") { currentSearch = q; loadCurrentView(); }
    else { currentView = "search"; currentSearch = q; loadCurrentView(); }
  }

  async function onDownload() {
    const destDir = await window.electronAPI.selectDirectory();
    if (!destDir) return;
    const files = allFiles.filter((f) => selectedIds.has(f.id) && f.is_folder !== 1);
    if (!files.length) { statusText.textContent = "No files selected (folders are skipped)"; return; }
    try {
      await api("/download", { method: "POST", body: JSON.stringify({ files: files.map((f) => ({ id: f.id, name: f.name, mime_type: f.mime_type, size: f.size })), dest_dir: destDir }) });
      downloadModal.style.display = "flex"; dlCloseBtn.style.display = ""; pollDownloadProgress();
    } catch (e) { statusText.textContent = "Download error: " + e.message; }
  }

  function pollDownloadProgress() {
    const timer = setInterval(async () => {
      try {
        const prog = await api("/download/progress");
        const total = prog.total || 0, completed = prog.completed || 0, failed = prog.failed || 0;
        dlProgressBar.style.width = (total > 0 ? ((completed + failed) / total) * 100 : 0) + "%";
        dlProgressText.textContent = `${completed} / ${total} completed` + (failed > 0 ? ` (${failed} failed)` : "");
        dlItemsList.innerHTML = (prog.items || []).slice(-20).map((item) => `<div class="dl-item"><span class="dl-item-name">${escapeHtml(item.file_name)}</span><span class="dl-item-status ${item.status}">${item.status === "downloading" ? Math.round((item.progress || 0) * 100) + "%" : item.status}</span></div>`).join("");
        if (completed + failed >= total && total > 0) { clearInterval(timer); dlCloseBtn.style.display = ""; statusText.textContent = `Downloaded ${completed} files`; }
      } catch {}
    }, 800);
  }

  function onDeleteClick() { if (!selectedIds.size) return; deleteCountSpan.textContent = selectedIds.size; deleteModal.style.display = "flex"; }

  async function doDelete() {
    deleteModal.style.display = "none";
    const ids = Array.from(selectedIds); statusText.textContent = `Deleting ${ids.length} files...`;
    try {
      const result = await api("/delete", { method: "POST", body: JSON.stringify({ file_ids: ids }) });
      const successes = result.results.filter((r) => r.success).length, failures = result.results.filter((r) => !r.success).length;
      const deletedIds = new Set(result.results.filter((r) => r.success).map((r) => r.id));
      allFiles = allFiles.filter((f) => !deletedIds.has(f.id)); totalCount -= successes; selectedIds.clear();
      setupVirtualScroll(); updateSelectionUI(); updateStats();
      statusText.textContent = `Deleted ${successes} files` + (failures > 0 ? `, ${failures} failed` : "");
    } catch (e) { statusText.textContent = "Delete error: " + e.message; }
  }

  function updateSortUI() { $$(".sortable").forEach((el) => { el.classList.toggle("active", el.dataset.sort === sortBy); const a = el.querySelector(".sort-arrow"); if (a) a.textContent = sortDir === "ASC" ? "▲" : "▼"; }); }

  function getFileIcon(m, isFolder) {
    if (isFolder) return "📁"; if (!m) return "📄";
    if (m.includes("image")) return "🖼️"; if (m.includes("video")) return "🎬"; if (m.includes("audio")) return "🎵";
    if (m.includes("pdf")) return "📕"; if (m.includes("spreadsheet") || m.includes("excel")) return "📊";
    if (m.includes("presentation") || m.includes("powerpoint")) return "📽️";
    if (m.includes("document") || m.includes("word")) return "📝";
    if (m.includes("zip") || m.includes("archive") || m.includes("compressed")) return "📦";
    if (m.includes("text")) return "📃"; if (m.includes("google-apps.form")) return "📋";
    if (m.includes("google-apps.drawing")) return "🎨"; return "📄";
  }

  function formatSize(bytes) {
    if (!bytes) return "—";
    const units = ["B", "KB", "MB", "GB", "TB"]; let i = 0, size = bytes;
    while (size >= 1024 && i < units.length - 1) { size /= 1024; i++; }
    return size.toFixed(i > 0 ? 1 : 0) + " " + units[i];
  }

  function formatMimeType(mime) {
    if (!mime) return "Unknown";
    const map = {"application/vnd.google-apps.folder":"Folder","application/vnd.google-apps.document":"Google Doc","application/vnd.google-apps.spreadsheet":"Google Sheet","application/vnd.google-apps.presentation":"Google Slides","application/vnd.google-apps.form":"Google Form","application/vnd.google-apps.drawing":"Google Drawing","application/pdf":"PDF","image/jpeg":"JPEG Image","image/png":"PNG Image","video/mp4":"MP4 Video","text/plain":"Text File","text/csv":"CSV File","application/zip":"ZIP Archive","application/json":"JSON File"};
    if (map[mime]) return map[mime];
    const parts = mime.split("/"); return parts.length > 1 ? parts[1].replace("vnd.", "").substring(0, 20) : mime;
  }

  function formatDate(d) { if (!d) return "—"; try { return new Date(d).toLocaleDateString(undefined, { year: "numeric", month: "short", day: "numeric" }); } catch { return d; } }
  function escapeHtml(s) { return s ? s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;") : ""; }
  function debounce(fn, ms) { let t; return function (...a) { clearTimeout(t); t = setTimeout(() => fn.apply(this, a), ms); }; }

  const style = document.createElement("style");
  style.textContent = `@keyframes pulse{0%,100%{opacity:.4}50%{opacity:.8}}`;
  document.head.appendChild(style);

  init().then(() => { if (appScreen.style.display !== "none") loadRootFolders(); });
})();
