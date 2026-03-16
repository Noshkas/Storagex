const KEY_STORAGE_KEY = "sx_key";
const LIBRARY_CACHE_KEY = "sx_library_cache_v1";
const KEY_PATTERN = /^\d{24}$/;
const POLL_INTERVAL_MS = 1000;
const CONNECT_URL = "/auth/youtube/start";
const FLASH_DISMISS_MS = 4200;

const state = {
  busy: false,
  library: {
    configured: false,
    connected: false,
    files: [],
    connect_url: CONNECT_URL,
  },
  settings: {
    configured: false,
    client_id: "",
    has_client_secret: false,
    source: "none",
  },
  keyResolver: null,
  settingsOpen: false,
  previousFocus: null,
  flashTimer: null,
  renderedLibrarySignature: "",
};

const dataTable = document.querySelector("#data-table");
const filesList = document.querySelector("#files-list");
const emptyState = document.querySelector("#empty-state");
const emptyTitle = document.querySelector("#empty-title");
const uploadButton = document.querySelector("#upload-button");
const uploadIcon = document.querySelector("#upload-icon");
const uploadInput = document.querySelector("#upload-input");
const settingsButton = document.querySelector("#settings-button");
const settingsModal = document.querySelector("#settings-modal");
const settingsBackdrop = settingsModal.querySelector(".modal__backdrop");
const settingsForm = document.querySelector("#settings-form");
const settingsKey = document.querySelector("#settings-key");
const settingsClientId = document.querySelector("#settings-client-id");
const settingsClientSecret = document.querySelector("#settings-client-secret");
const settingsError = document.querySelector("#settings-error");
const settingsClose = document.querySelector("#settings-close");
const settingsCancel = document.querySelector("#settings-cancel");
const disconnectButton = document.querySelector("#disconnect-button");
const resetButton = document.querySelector("#reset-button");
const flashMessage = document.querySelector("#flash-message");
const jobPanel = document.querySelector("#job-panel");
const jobTitle = document.querySelector("#job-title");
const jobProgressLabel = document.querySelector("#job-progress-label");
const jobProgressValue = document.querySelector("#job-progress-value");
const keyModal = document.querySelector("#key-modal");
const keyBackdrop = keyModal.querySelector(".modal__backdrop");
const keyForm = document.querySelector("#key-form");
const keyInput = document.querySelector("#key-input");
const keyError = document.querySelector("#key-error");
const keyCancel = document.querySelector("#key-cancel");

settingsButton.addEventListener("click", openSettingsModal);
settingsForm.addEventListener("submit", submitSettings);
settingsClose.addEventListener("click", closeSettingsModal);
settingsCancel.addEventListener("click", closeSettingsModal);
settingsBackdrop.addEventListener("click", closeSettingsModal);
disconnectButton.addEventListener("click", disconnectYouTube);
if (resetButton) {
  resetButton.addEventListener("click", resetYouTubeSetup);
}
settingsKey.addEventListener("input", () => {
  settingsKey.value = settingsKey.value.replace(/\D/g, "").slice(0, 24);
  hideSettingsError();
});
uploadButton.addEventListener("click", handlePrimaryAction);
uploadInput.addEventListener("change", () => handleFileSelection(uploadInput.files?.[0]));
filesList.addEventListener("click", handleFileAction);
keyForm.addEventListener("submit", submitKeyPrompt);
keyCancel.addEventListener("click", () => resolveKeyPrompt(null));
keyBackdrop.addEventListener("click", () => resolveKeyPrompt(null));
keyInput.addEventListener("input", () => {
  keyInput.value = keyInput.value.replace(/\D/g, "").slice(0, 24);
  hideKeyError();
});
document.addEventListener("keydown", (event) => {
  if (event.key !== "Escape") {
    return;
  }
  if (!settingsModal.hidden) {
    event.preventDefault();
    closeSettingsModal();
    return;
  }
  if (!keyModal.hidden) {
    event.preventDefault();
    resolveKeyPrompt(null);
  }
});

void bootstrap();

async function bootstrap() {
  renderPrimaryAction();
  renderSettingsActions();
  showFlashFromQuery();
  hydrateLibraryFromCache();

  try {
    await refreshSettings();
    await refreshLibrary();
  } catch (error) {
    setFlash("error", error.message || "Could not load StorageX.");
  }
}

async function refreshSettings() {
  const payload = await fetchJson("/api/settings/youtube", undefined, "Could not load YouTube settings.");
  state.settings = {
    configured: Boolean(payload.configured),
    client_id: payload.client_id || "",
    has_client_secret: Boolean(payload.has_client_secret),
    source: payload.source || "none",
  };
}

async function refreshLibrary(options = {}) {
  const payload = await fetchJson("/api/library", undefined, "Could not load files.");
  const fetchedFiles = Array.isArray(payload.files) ? payload.files.slice() : [];
  const preserveExistingFiles = Boolean(
    options.preserveExistingFiles !== false &&
    fetchedFiles.length === 0 &&
    state.library.files.length > 0 &&
    !payload.connected,
  );
  const files = preserveExistingFiles ? state.library.files.slice() : fetchedFiles;

  if (options.fallbackFile && !files.some((item) => item.video_id === options.fallbackFile.video_id)) {
    files.unshift(options.fallbackFile);
  }

  const nextLibrary = {
    configured: Boolean(payload.configured),
    connected: Boolean(payload.connected),
    connect_url: payload.connect_url || CONNECT_URL,
    files,
  };

  state.library = nextLibrary;
  syncLibraryCache(nextLibrary, {
    preserveExistingFiles,
    clear: Boolean(options.clearCache),
  });

  renderLibrary(payload.error || null);
}

function renderLibrary(libraryError) {
  renderPrimaryAction();
  renderSettingsActions();

  if (libraryError) {
    setFlash("error", libraryError, "library");
  } else {
    clearFlash("library");
  }

  renderFiles();
}

function renderPrimaryAction() {
  const actionLabel = state.library.connected ? "Upload" : "Connect";
  uploadButton.disabled = state.busy;
  uploadButton.classList.toggle("is-disabled", state.busy);
  uploadButton.setAttribute("aria-label", actionLabel);
  uploadButton.setAttribute("title", actionLabel);
  uploadIcon.innerHTML = state.library.connected ? uploadIconMarkup() : connectIconMarkup();
  uploadInput.disabled = state.busy || !state.library.connected;
}

function renderSettingsActions() {
  disconnectButton.hidden = !state.library.connected;
  if (resetButton) {
    resetButton.hidden = !(state.settings.source === "runtime" || state.library.connected);
  }
}

function renderFiles() {
  const signature = buildLibrarySignature(state.library);
  if (signature === state.renderedLibrarySignature) {
    return;
  }

  state.renderedLibrarySignature = signature;

  if (state.library.files.length === 0) {
    dataTable.hidden = true;
    filesList.innerHTML = "";
    emptyState.hidden = false;

    if (!state.library.configured) {
      emptyTitle.textContent = "No settings";
    } else if (!state.library.connected) {
      emptyTitle.textContent = "Not connected";
    } else {
      emptyTitle.textContent = "No files";
    }
    return;
  }

  dataTable.hidden = false;
  emptyState.hidden = true;
  filesList.innerHTML = state.library.files.map((file) => renderFileRow(file)).join("");
}

function renderFileRow(file) {
  const name = file?.original_filename || "";
  return `
    <article class="file-row">
      <div class="file-cell">
        <div class="file-name" title="${escapeHtml(name)}">${escapeHtml(displayName(name))}</div>
      </div>
      <div class="file-value">${escapeHtml(fileTypeLabel(file))}</div>
      <div class="file-value">${escapeHtml(formatBytes(file.original_size))}</div>
      <div class="file-value">${escapeHtml(formatDate(file.uploaded_at))}</div>
      <div class="file-download-cell">
        <button
          class="file-link"
          type="button"
          title="Download"
          aria-label="Download"
          data-action="download"
          data-video-id="${escapeHtml(file.video_id)}"
        >
          ${downloadIconMarkup()}
        </button>
      </div>
    </article>
  `;
}

function displayName(filename) {
  const name = String(filename || "");
  const match = /^(.*?)(\.[^.]+)?$/.exec(name);
  const base = (match?.[1] || "")
    .replaceAll("_", " ")
    .replace(/\s+/g, " ")
    .trim();
  const extension = match?.[2] || "";
  return `${base}${extension}`;
}

function handlePrimaryAction(event) {
  event.preventDefault();
  if (state.busy) {
    return;
  }

  if (state.library.connected) {
    uploadInput.click();
    return;
  }

  if (!state.settings.configured) {
    setFlash("error", "Save a YouTube client ID and client secret first.");
    openSettingsModal();
    return;
  }

  window.location.assign(state.library.connect_url || CONNECT_URL);
}

function handleFileAction(event) {
  const target = event.target instanceof HTMLElement ? event.target.closest("[data-action]") : null;
  if (!(target instanceof HTMLElement)) {
    return;
  }

  const action = target.dataset.action;
  if (action !== "download" || state.busy) {
    return;
  }

  const videoId = target.dataset.videoId || "";
  if (!videoId) {
    setFlash("error", "Missing YouTube file id.");
    return;
  }

  void handleRemoteDownload(videoId);
}

async function handleFileSelection(file) {
  if (!file) {
    return;
  }

  uploadInput.value = "";

  if (!state.library.connected) {
    setFlash("error", "Connect YouTube.");
    return;
  }

  let key = await resolveOperationalKey();
  if (!key) {
    return;
  }

  state.busy = true;
  renderPrimaryAction();
  clearFlash();
  setJobProgress(2, "Preparing");

  try {
    const jobIdPromise = startUpload(file, key);
    key = "";
    const jobId = await jobIdPromise;
    const job = await pollJob(jobId);
    const fallbackFile = job.metadata?.remote_file || null;
    setJobProgress(100, "Done");
    setFlash("success", job.metadata?.original_filename || file.name);
    await refreshLibrary({ fallbackFile });
    window.setTimeout(hideJobProgress, 1000);
  } catch (error) {
    hideJobProgress();
    setFlash("error", error.message || "Upload failed.");
  } finally {
    key = "";
    state.busy = false;
    renderPrimaryAction();
  }
}

async function handleRemoteDownload(videoId) {
  if (!state.library.connected) {
    setFlash("error", "Connect YouTube.");
    return;
  }

  let key = await resolveOperationalKey();
  if (!key) {
    return;
  }

  state.busy = true;
  renderPrimaryAction();
  clearFlash();
  setJobProgress(2, "Preparing");

  try {
    const jobIdPromise = startRemoteDownload(videoId, key);
    key = "";
    const jobId = await jobIdPromise;
    const job = await pollJob(jobId);
    setJobProgress(100, "Done");
    await startBrowserDownload(
      job.artifacts.recovered_file,
      job.metadata?.original_filename || "recovered.bin",
    );
    setFlash("success", job.metadata?.original_filename || "Download complete");
    window.setTimeout(hideJobProgress, 1000);
  } catch (error) {
    hideJobProgress();
    setFlash("error", error.message || "Download failed.");
  } finally {
    key = "";
    state.busy = false;
    renderPrimaryAction();
  }
}

async function startUpload(file, key) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("key", key);

  const payload = await fetchJson("/api/files", {
    method: "POST",
    body: formData,
  }, "Upload failed.");

  return payload.job_id;
}

async function startRemoteDownload(videoId, key) {
  const formData = new FormData();
  formData.append("key", key);

  const payload = await fetchJson(`/api/files/${encodeURIComponent(videoId)}/download`, {
    method: "POST",
    body: formData,
  }, "Download failed.");

  return payload.job_id;
}

async function pollJob(jobId) {
  while (true) {
    const payload = await fetchJson(`/api/jobs/${jobId}`, undefined, "Could not fetch job status.");

    setJobProgress(payload.progress, payload.message);

    if (payload.status === "completed") {
      return payload;
    }
    if (payload.status === "failed") {
      throw new Error(payload.error || "Job failed.");
    }

    await delay(POLL_INTERVAL_MS);
  }
}

async function disconnectYouTube() {
  try {
    await fetchJson("/api/auth/disconnect", { method: "POST" }, "Could not disconnect.");
    clearLibraryCache();
    setFlash("success", "Disconnected");
    await refreshLibrary({ preserveExistingFiles: false, clearCache: true });
  } catch (error) {
    setFlash("error", error.message || "Could not disconnect.");
  }
}

async function resetYouTubeSetup() {
  const confirmed = window.confirm("Reset the saved YouTube client config and local login on this Mac?");
  if (!confirmed) {
    return;
  }

  try {
    const payload = await fetchJson("/api/auth/reset", { method: "POST" }, "Could not reset local YouTube setup.");
    clearLibraryCache();
    state.settings = {
      configured: Boolean(payload.settings?.configured),
      client_id: payload.settings?.client_id || "",
      has_client_secret: Boolean(payload.settings?.has_client_secret),
      source: payload.settings?.source || "none",
    };
    closeSettingsModal();
    setFlash("success", "Reset local YouTube setup");
    await refreshLibrary({ preserveExistingFiles: false, clearCache: true });
  } catch (error) {
    setFlash("error", error.message || "Could not reset local YouTube setup.");
  }
}

function openSettingsModal() {
  state.previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  state.settingsOpen = true;
  settingsModal.hidden = false;
  document.body.classList.add("is-modal-open");
  settingsKey.value = getStoredKey() || "";
  settingsClientId.value = state.settings.client_id || "";
  settingsClientSecret.value = "";
  settingsClientSecret.placeholder = state.settings.has_client_secret ? "Saved" : "";
  renderSettingsActions();
  hideSettingsError();

  window.setTimeout(() => settingsKey.focus(), 20);
}

function closeSettingsModal() {
  state.settingsOpen = false;
  settingsModal.hidden = true;
  document.body.classList.remove("is-modal-open");
  settingsClientSecret.value = "";
  hideSettingsError();

  if (state.previousFocus) {
    state.previousFocus.focus();
  }
  state.previousFocus = null;
}

async function submitSettings(event) {
  event.preventDefault();

  const keyValue = settingsKey.value.trim();
  const clientId = settingsClientId.value.trim();
  const clientSecret = settingsClientSecret.value.trim();
  const settingsChanged = clientSecret !== "" || clientId !== (state.settings.client_id || "");

  if (keyValue && !KEY_PATTERN.test(keyValue)) {
    showSettingsError("Encryption key must be exactly 24 digits or left empty.");
    settingsKey.focus();
    return;
  }

  if (settingsChanged) {
    if (!clientId || !clientSecret) {
      showSettingsError("Client ID and client secret are required to update YouTube settings.");
      return;
    }

    try {
      const payload = await fetchJson("/api/settings/youtube", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          client_id: clientId,
          client_secret: clientSecret,
        }),
      }, "Could not save settings.");

      state.settings = {
        configured: Boolean(payload.configured),
        client_id: payload.client_id || "",
        has_client_secret: Boolean(payload.has_client_secret),
        source: payload.source || "runtime",
      };
    } catch (error) {
      showSettingsError(error.message || "Could not save settings.");
      return;
    }
  }

  if (keyValue) {
    setStoredKey(keyValue);
  }

  closeSettingsModal();

  if (!settingsChanged && !keyValue) {
    return;
  }

  setFlash("success", "Settings saved");

  if (settingsChanged) {
    await refreshLibrary();
  }
}

function showSettingsError(message) {
  settingsError.hidden = false;
  settingsError.textContent = message;
}

function hideSettingsError() {
  settingsError.hidden = true;
  settingsError.textContent = "";
}

function setJobProgress(progress, message) {
  const clamped = Math.max(0, Math.min(100, Number(progress) || 0));
  jobPanel.hidden = false;
  jobTitle.textContent = message || "Processing";
  jobProgressLabel.textContent = `${clamped}%`;
  jobProgressValue.style.width = `${clamped}%`;
}

function hideJobProgress() {
  jobPanel.hidden = true;
  jobTitle.textContent = "Processing";
  jobProgressLabel.textContent = "0%";
  jobProgressValue.style.width = "0%";
}

async function startBrowserDownload(url, filename) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error("Could not download the recovered file.");
  }

  const blob = await response.blob();
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = filename;
  document.body.append(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(objectUrl), 1000);
}

function showFlashFromQuery() {
  const url = new URL(window.location.href);
  const status = url.searchParams.get("youtube");
  const reason = url.searchParams.get("reason");

  if (status === "connected") {
    setFlash("success", "Connected", "oauth");
  } else if (status === "error") {
    setFlash("error", reason || "Connection failed", "oauth");
  }

  if (status) {
    url.searchParams.delete("youtube");
    url.searchParams.delete("reason");
    window.history.replaceState({}, "", url.pathname + url.search + url.hash);
  }
}

async function resolveOperationalKey() {
  const storedKey = getStoredKey();
  if (storedKey) {
    return storedKey;
  }
  return promptForKey();
}

function getStoredKey() {
  try {
    const value = sessionStorage.getItem(KEY_STORAGE_KEY)?.trim() || "";
    return KEY_PATTERN.test(value) ? value : null;
  } catch {
    return null;
  }
}

function hydrateLibraryFromCache() {
  const cachedLibrary = getCachedLibrary();
  if (!cachedLibrary) {
    return;
  }

  state.library = cachedLibrary;
  renderLibrary();
}

function getCachedLibrary() {
  try {
    const raw = localStorage.getItem(LIBRARY_CACHE_KEY);
    if (!raw) {
      return null;
    }

    const payload = JSON.parse(raw);
    const files = Array.isArray(payload?.files) ? payload.files : [];
    if (files.length === 0) {
      return null;
    }

    return {
      configured: Boolean(payload.configured),
      connected: Boolean(payload.connected),
      connect_url: payload.connect_url || CONNECT_URL,
      files,
    };
  } catch {
    return null;
  }
}

function syncLibraryCache(library, options = {}) {
  if (options.clear) {
    clearLibraryCache();
    return;
  }

  if (library.files.length > 0) {
    try {
      localStorage.setItem(LIBRARY_CACHE_KEY, JSON.stringify({
        configured: library.configured,
        connected: library.connected,
        connect_url: library.connect_url,
        files: library.files,
      }));
    } catch {
      return;
    }
    return;
  }

  if (library.connected || !library.configured || options.preserveExistingFiles === false) {
    clearLibraryCache();
  }
}

function clearLibraryCache() {
  try {
    localStorage.removeItem(LIBRARY_CACHE_KEY);
  } catch {
    return;
  }
}

function setStoredKey(value) {
  try {
    sessionStorage.setItem(KEY_STORAGE_KEY, value);
  } catch {
    return;
  }
}

function promptForKey() {
  if (state.keyResolver) {
    resolveKeyPrompt(null);
  }

  state.previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  keyModal.hidden = false;
  document.body.classList.add("is-modal-open");
  keyInput.value = "";
  hideKeyError();

  return new Promise((resolve) => {
    state.keyResolver = resolve;
    window.setTimeout(() => keyInput.focus(), 20);
  });
}

function submitKeyPrompt(event) {
  event.preventDefault();
  const key = keyInput.value.trim();
  if (!KEY_PATTERN.test(key)) {
    showKeyError("Key must be exactly 24 digits.");
    return;
  }

  resolveKeyPrompt(key);
}

function resolveKeyPrompt(value) {
  if (!state.keyResolver) {
    return;
  }

  const resolver = state.keyResolver;
  state.keyResolver = null;
  keyInput.value = "";
  hideKeyError();
  keyModal.hidden = true;

  if (state.settingsOpen) {
    document.body.classList.add("is-modal-open");
  } else {
    document.body.classList.remove("is-modal-open");
  }

  if (state.previousFocus) {
    state.previousFocus.focus();
  }
  state.previousFocus = null;
  resolver(value);
}

function showKeyError(message) {
  keyError.hidden = false;
  keyError.textContent = message;
}

function hideKeyError() {
  keyError.hidden = true;
  keyError.textContent = "";
}

function setFlash(type, message, source = "general") {
  clearFlashTimer();
  flashMessage.hidden = false;
  flashMessage.textContent = message;
  flashMessage.className = `flash-message flash-message--${type}`;
  flashMessage.dataset.source = source;

  if (type === "success") {
    state.flashTimer = window.setTimeout(() => clearFlash(source), FLASH_DISMISS_MS);
  }
}

function clearFlash(source = null) {
  if (source && flashMessage.dataset.source !== source) {
    return;
  }

  clearFlashTimer();
  flashMessage.hidden = true;
  flashMessage.textContent = "";
  flashMessage.className = "flash-message";
  delete flashMessage.dataset.source;
}

function formatBytes(value) {
  const bytes = Number(value) || 0;
  if (bytes < 1024) {
    return `${bytes} B`;
  }

  const units = ["KB", "MB", "GB"];
  let current = bytes / 1024;
  let unitIndex = 0;
  while (current >= 1024 && unitIndex < units.length - 1) {
    current /= 1024;
    unitIndex += 1;
  }
  return `${current.toFixed(current >= 100 ? 0 : 1)} ${units[unitIndex]}`;
}

function formatDate(value) {
  if (!value) {
    return "Unknown";
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }

  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

function extensionLabel(filename) {
  const match = /\.([^.]+)$/.exec(filename || "");
  if (!match) {
    return "BIN";
  }
  return match[1].slice(0, 4);
}

function fileTypeLabel(file) {
  const extension = extensionLabel(file?.original_filename || "");
  if (extension !== "BIN") {
    return extension.toUpperCase();
  }

  const mediaType = String(file?.media_type || "");
  if (!mediaType) {
    return "FILE";
  }

  const category = mediaType.split("/")[0] || mediaType;
  return category.slice(0, 4).toUpperCase() || "FILE";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function delay(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function clearFlashTimer() {
  if (state.flashTimer) {
    window.clearTimeout(state.flashTimer);
    state.flashTimer = null;
  }
}

function buildLibrarySignature(library) {
  return JSON.stringify({
    configured: Boolean(library?.configured),
    connected: Boolean(library?.connected),
    files: Array.isArray(library?.files)
      ? library.files.map((file) => [
          file.video_id || "",
          file.original_filename || "",
          Number(file.original_size) || 0,
          file.uploaded_at || "",
          file.media_type || "",
        ])
      : [],
  });
}

async function fetchJson(url, options, fallbackMessage) {
  const response = await fetch(url, options);
  let payload = {};

  try {
    payload = await response.json();
  } catch {
    payload = {};
  }

  if (!response.ok) {
    throw new Error(payload.detail || fallbackMessage);
  }

  return payload;
}

function uploadIconMarkup() {
  return `
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M12 17V7.5M8.5 10.75 12 7.25l3.5 3.5M6.5 18.5h11" />
    </svg>
  `;
}

function connectIconMarkup() {
  return `
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.85" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M10 13a4 4 0 0 1 0-5.66l1.17-1.17a4 4 0 0 1 5.66 5.66l-1.16 1.17" />
      <path d="M14 11a4 4 0 0 1 0 5.66l-1.17 1.17a4 4 0 1 1-5.66-5.66l1.16-1.17" />
    </svg>
  `;
}

function downloadIconMarkup() {
  return `
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M12 4v10" />
      <path d="M8.5 10.5 12 14l3.5-3.5" />
      <path d="M5 18.5h14" />
    </svg>
  `;
}
