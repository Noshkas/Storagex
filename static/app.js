const KEY_STORAGE_KEY = "sx_key";
const LIBRARY_CACHE_KEY = "sx_library_cache_v2";
const VIEW_STATE_KEY = "sx_library_view_v1";
const FOLDER_COLORS_KEY = "sx_folder_colors";
const KEY_PATTERN = /^\d{24}$/;
const POLL_INTERVAL_MS = 1000;
const CONNECT_URL = "/auth/youtube/start";
const ROOT_FOLDER_ID = "root";
const FLASH_DISMISS_MS = 4200;

const FOLDER_COLORS = [
  { id: "rose",   bg: "#fde8e8", border: "#f0b8b8" },
  { id: "coral",  bg: "#fee8e0", border: "#f5a888" },
  { id: "amber",  bg: "#fef3e0", border: "#f5cc80" },
  { id: "lime",   bg: "#eaf5e8", border: "#a8d8a4" },
  { id: "mint",   bg: "#e0f8ec", border: "#80d4a8" },
  { id: "teal",   bg: "#e0f5f2", border: "#88d8ce" },
  { id: "sky",    bg: "#e0eeff", border: "#90bcf5" },
  { id: "iris",   bg: "#ece8ff", border: "#b4a4f0" },
  { id: "pink",   bg: "#fde8f5", border: "#f0a8d8" },
];

const state = {
  busy: false,
  library: {
    configured: false,
    connected: false,
    connect_url: CONNECT_URL,
    files: [],
    folders: [{ id: ROOT_FOLDER_ID, name: "All files", parent_id: null }],
    index_recovered: false,
  },
  settings: {
    configured: false,
    client_id: "",
    has_client_secret: false,
    source: "none",
    public_app_url: "",
  },
  currentFolderId: ROOT_FOLDER_ID,
  selectedVideoId: "",
  editingVideoId: "",
  keyResolver: null,
  confirmResolver: null,
  previousFocus: null,
  confirmPreviousFocus: null,
  settingsOpen: false,
  organizationOpen: false,
  confirmOpen: false,
  organizationAction: null,
  flashTimer: null,
  renamePending: false,
  folderDropdownOpen: false,
  newFolderColorId: FOLDER_COLORS[0].id,
  searchQuery: "",
  sortKey: "uploaded_at",
  sortDir: "desc",
  pendingUpload: null,
  links: [],
  linksLoading: false,
  linksOpen: false,
  linksPollTimer: null,
  extendingShareToken: "",
  shareLinkOpen: false,
  sharingVideoId: "",
  sharingStartTime: 0,
  downloadingVideoId: "",
  downloadStartTime: 0,
  timerInterval: null,
};

const dataTable = document.querySelector("#data-table");
const dataTableHead = document.querySelector(".data-table__head");
const filesList = document.querySelector("#files-list");
const emptyState = document.querySelector("#empty-state");
const emptyTitle = document.querySelector("#empty-title");
const uploadButton = document.querySelector("#upload-button");
const uploadIcon = document.querySelector("#upload-icon");
const uploadInput = document.querySelector("#upload-input");
const settingsButton = document.querySelector("#settings-button");
const sharesButton = document.querySelector("#shares-button");
const settingsModal = document.querySelector("#settings-modal");
const settingsBackdrop = settingsModal.querySelector(".modal__backdrop");
const settingsForm = document.querySelector("#settings-form");
const settingsKey = document.querySelector("#settings-key");
const settingsClientId = document.querySelector("#settings-client-id");
const settingsClientSecret = document.querySelector("#settings-client-secret");
const settingsPublicAppUrl = document.querySelector("#settings-public-app-url");
const settingsCreatePublicUrl = document.querySelector("#settings-create-public-url");
const settingsError = document.querySelector("#settings-error");
const settingsClose = document.querySelector("#settings-close");
const settingsCancel = document.querySelector("#settings-cancel");
const disconnectButton = document.querySelector("#disconnect-button");
const resetButton = document.querySelector("#reset-button");
const appbarSearch = document.querySelector("#appbar-search");
const searchToggle = document.querySelector("#search-toggle");
const searchInput = document.querySelector("#search-input");
const folderDropdown = document.querySelector("#folder-dropdown");
const folderDropdownBtn = document.querySelector("#folder-dropdown-btn");
const folderDropdownLabel = document.querySelector("#folder-dropdown-label");
const folderDropdownPanel = document.querySelector("#folder-dropdown-panel");
const folderColorField = document.querySelector("#folder-color-field");
const folderColorPicker = document.querySelector("#folder-color-picker");
const newFolderButton = document.querySelector("#new-folder-button");
const deleteFolderButton = document.querySelector("#delete-folder-button");
const flashMessage = document.querySelector("#flash-message");
const jobPanel = document.querySelector("#job-panel");
const jobTitle = document.querySelector("#job-title");
const jobProgressLabel = document.querySelector("#job-progress-label");
const jobProgressValue = document.querySelector("#job-progress-value");
const organizeModal = document.querySelector("#organize-modal");
const organizeBackdrop = organizeModal.querySelector(".modal__backdrop");
const organizeForm = document.querySelector("#organize-form");
const organizeTitle = document.querySelector("#organize-title");
const organizeClose = document.querySelector("#organize-close");
const organizeCancel = document.querySelector("#organize-cancel");
const organizeSubmit = document.querySelector("#organize-submit");
const organizeNameField = document.querySelector("#organize-name-field");
const organizeNameLabel = document.querySelector("#organize-name-label");
const organizeName = document.querySelector("#organize-name");
const organizeError = document.querySelector("#organize-error");
const keyModal = document.querySelector("#key-modal");
const keyBackdrop = keyModal.querySelector(".modal__backdrop");
const keyForm = document.querySelector("#key-form");
const keyInput = document.querySelector("#key-input");
const keyError = document.querySelector("#key-error");
const keyCancel = document.querySelector("#key-cancel");
const confirmModal = document.querySelector("#confirm-modal");
const confirmBackdrop = confirmModal.querySelector(".modal__backdrop");
const confirmTitle = document.querySelector("#confirm-title");
const confirmMessage = document.querySelector("#confirm-message");
const confirmClose = document.querySelector("#confirm-close");
const confirmCancel = document.querySelector("#confirm-cancel");
const confirmSubmit = document.querySelector("#confirm-submit");
const shareLinkModal = document.querySelector("#share-link-modal");
const shareLinkBackdrop = shareLinkModal.querySelector(".modal__backdrop");
const shareLinkInput = document.querySelector("#share-link-input");
const shareLinkCopyBtn = document.querySelector("#share-link-copy-btn");
const shareLinkClose = document.querySelector("#share-link-close");
const sharesModal = document.querySelector("#shares-modal");
const sharesBackdrop = sharesModal.querySelector(".modal__backdrop");
const sharesClose = document.querySelector("#shares-close");
const sharesError = document.querySelector("#shares-error");
const sharesEmpty = document.querySelector("#shares-empty");
const sharesList = document.querySelector("#shares-list");

settingsButton.addEventListener("click", openSettingsModal);
sharesButton.addEventListener("click", openSharesModal);
settingsForm.addEventListener("submit", submitSettings);
settingsClose.addEventListener("click", closeSettingsModal);
settingsCancel.addEventListener("click", closeSettingsModal);
settingsBackdrop.addEventListener("click", closeSettingsModal);
settingsCreatePublicUrl.addEventListener("click", createPublicUrl);
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
searchToggle.addEventListener("click", openSearch);
dataTableHead.addEventListener("click", handleSortClick);
searchInput.addEventListener("input", handleSearchInput);
searchInput.addEventListener("blur", handleSearchBlur);
folderDropdownBtn.addEventListener("click", toggleFolderDropdown);
folderDropdownPanel.addEventListener("click", handleFolderOptionClick);
folderColorPicker.addEventListener("click", handleColorSwatchClick);
document.addEventListener("click", handleOutsideClick);
filesList.addEventListener("click", handleFilesListClick);
filesList.addEventListener("dblclick", handleFilesListDoubleClick);
filesList.addEventListener("keydown", handleFilesListKeydown);
filesList.addEventListener("focusout", handleFilesListFocusOut);
newFolderButton.addEventListener("click", () => openOrganizationModal({ type: "create-folder" }));
deleteFolderButton.addEventListener("click", handleDeleteFolder);
organizeForm.addEventListener("submit", submitOrganizationAction);
organizeClose.addEventListener("click", closeOrganizationModal);
organizeCancel.addEventListener("click", closeOrganizationModal);
organizeBackdrop.addEventListener("click", closeOrganizationModal);
organizeName.addEventListener("input", hideOrganizationError);
keyForm.addEventListener("submit", submitKeyPrompt);
keyCancel.addEventListener("click", () => resolveKeyPrompt(null));
keyBackdrop.addEventListener("click", () => resolveKeyPrompt(null));
keyInput.addEventListener("input", () => {
  keyInput.value = keyInput.value.replace(/\D/g, "").slice(0, 24);
  hideKeyError();
});
confirmClose.addEventListener("click", () => resolveConfirm(false));
confirmCancel.addEventListener("click", () => resolveConfirm(false));
confirmBackdrop.addEventListener("click", () => resolveConfirm(false));
confirmSubmit.addEventListener("click", () => resolveConfirm(true));
shareLinkClose.addEventListener("click", closeShareLinkModal);
shareLinkBackdrop.addEventListener("click", closeShareLinkModal);
shareLinkCopyBtn.addEventListener("click", handleShareLinkCopy);
sharesClose.addEventListener("click", closeSharesModal);
sharesBackdrop.addEventListener("click", closeSharesModal);
sharesList.addEventListener("click", handleSharesListClick);
document.addEventListener("keydown", handleGlobalKeydown);

void bootstrap();

async function bootstrap() {
  hydrateViewState();
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
  const [youtubePayload, appPayload] = await Promise.all([
    fetchJson("/api/settings/youtube", undefined, "Could not load YouTube settings."),
    fetchJson("/api/settings/app", undefined, "Could not load app settings."),
  ]);
  state.settings = {
    configured: Boolean(youtubePayload.configured),
    client_id: youtubePayload.client_id || "",
    has_client_secret: Boolean(youtubePayload.has_client_secret),
    source: youtubePayload.source || "none",
    public_app_url: appPayload.public_app_url || "",
  };
}

async function refreshLibrary(options = {}) {
  const payload = await fetchJson("/api/library", undefined, "Could not load files.");
  const fetchedFolders = normalizeFolders(payload.folders);
  const fetchedFiles = normalizeFiles(payload.files, fetchedFolders);
  const preserveExistingFiles = Boolean(
    options.preserveExistingFiles !== false &&
    fetchedFiles.length === 0 &&
    state.library.files.length > 0 &&
    !payload.connected,
  );
  const files = preserveExistingFiles ? state.library.files.slice() : fetchedFiles;

  if (options.fallbackFile && !files.some((file) => file.video_id === options.fallbackFile.video_id)) {
    files.unshift(normalizeFile(options.fallbackFile, fetchedFolders));
  }

  state.library = {
    configured: Boolean(payload.configured),
    connected: Boolean(payload.connected),
    connect_url: payload.connect_url || CONNECT_URL,
    files,
    folders: fetchedFolders,
    index_recovered: Boolean(payload.index_recovered),
  };

  normalizeViewState();
  syncLibraryCache(state.library, {
    preserveExistingFiles,
    clear: Boolean(options.clearCache),
  });
  renderLibraryState(payload.error || null);
}

function renderLibraryState(libraryError = null) {
  renderPrimaryAction();
  renderSettingsActions();

  if (libraryError) {
    setFlash("error", libraryError, "library");
    clearFlash("index");
  } else {
    clearFlash("library");
    if (state.library.index_recovered) {
      setFlash("error", "Rebuilt the local folder index.", "index");
    } else {
      clearFlash("index");
    }
  }

  renderViews();
}

function renderViews() {
  normalizeViewState();
  renderFolderSelect();
  renderFolderActions();
  renderSortHeaders();
  renderFiles();
  persistViewState();
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

function renderFolderActions() {
  const isRoot = state.currentFolderId === ROOT_FOLDER_ID;
  deleteFolderButton.disabled = isRoot;
  deleteFolderButton.setAttribute("aria-disabled", isRoot ? "true" : "false");
}

function renderFolderSelect() {
  const currentFolder = getCurrentFolder();
  const color = getFolderColor(state.currentFolderId);

  folderDropdownLabel.textContent = currentFolder?.name || "All files";
  folderDropdownBtn.style.background = color ? color.bg : "";
  folderDropdownBtn.style.borderColor = color ? color.border : "";

  applyFolderTheme(color);

  const childMap = buildFolderChildMap();
  const rootFolder = getFolderById(ROOT_FOLDER_ID);
  folderDropdownPanel.innerHTML = rootFolder ? renderFolderDropdownOptions(rootFolder, -1, childMap) : "";
}

function renderFolderDropdownOptions(folder, depth, childMap) {
  const isActive = folder.id === state.currentFolderId;
  const color = getFolderColor(folder.id);
  const dotStyle = color
    ? `background:${color.bg};border-color:${color.border}`
    : `background:var(--surface-2);border-color:var(--border)`;
  const indent = depth > 0 ? `style="padding-left:${10 + depth * 14}px"` : "";

  let markup = `
    <button class="folder-option${isActive ? " is-active" : ""}" type="button"
      role="option" aria-selected="${isActive}" data-folder-id="${escapeHtml(folder.id)}" ${indent}>
      <span class="folder-option__dot" style="${dotStyle}"></span>
      <span class="folder-option__name">${escapeHtml(folder.name)}</span>
    </button>`;

  for (const child of childMap.get(folder.id) || []) {
    markup += renderFolderDropdownOptions(child, depth + 1, childMap);
  }
  return markup;
}

function applyFolderTheme(color) {
  const headEl = document.querySelector(".data-table__head");
  if (!headEl) return;
  headEl.style.background = color ? color.bg : "";
  headEl.style.borderBottomColor = color ? color.border : "";
}

function toggleFolderDropdown() {
  state.folderDropdownOpen ? closeFolderDropdown() : openFolderDropdown();
}

function openFolderDropdown() {
  state.folderDropdownOpen = true;
  folderDropdownPanel.hidden = false;
  folderDropdownBtn.setAttribute("aria-expanded", "true");
}

function closeFolderDropdown() {
  state.folderDropdownOpen = false;
  folderDropdownPanel.hidden = true;
  folderDropdownBtn.setAttribute("aria-expanded", "false");
}

function handleFolderOptionClick(event) {
  const target = event.target instanceof Element ? event.target.closest("[data-folder-id]") : null;
  if (!target) return;

  const folderId = target.dataset.folderId || ROOT_FOLDER_ID;
  closeFolderDropdown();
  if (folderId === state.currentFolderId) return;

  state.currentFolderId = folderId;
  state.editingVideoId = "";
  closeSearch();
  renderViews();
}

function handleOutsideClick(event) {
  if (!state.folderDropdownOpen) return;
  if (folderDropdown.contains(event.target)) return;
  closeFolderDropdown();
}

function openSearch() {
  const w = folderDropdownBtn.offsetWidth;
  appbarSearch.style.setProperty("--search-width", `${w}px`);
  appbarSearch.classList.add("is-open");
  window.setTimeout(() => searchInput.focus(), 20);
}

function closeSearch() {
  state.searchQuery = "";
  searchInput.value = "";
  appbarSearch.classList.remove("is-open");
  renderFiles();
}

function handleSearchInput() {
  state.searchQuery = searchInput.value;
  renderFiles();
}

function handleSearchBlur() {
  if (!state.searchQuery) {
    closeSearch();
  }
}

function getFolderColorId(folderId) {
  if (!folderId || folderId === ROOT_FOLDER_ID) return null;
  try {
    const raw = localStorage.getItem(FOLDER_COLORS_KEY);
    if (!raw) return null;
    return JSON.parse(raw)[folderId] || null;
  } catch { return null; }
}

function setFolderColorId(folderId, colorId) {
  if (!folderId || folderId === ROOT_FOLDER_ID) return;
  try {
    const raw = localStorage.getItem(FOLDER_COLORS_KEY);
    const map = raw ? JSON.parse(raw) : {};
    if (colorId) { map[folderId] = colorId; } else { delete map[folderId]; }
    localStorage.setItem(FOLDER_COLORS_KEY, JSON.stringify(map));
  } catch {}
}

function removeFolderColors(folderIds) {
  if (!Array.isArray(folderIds) || folderIds.length === 0) return;
  try {
    const raw = localStorage.getItem(FOLDER_COLORS_KEY);
    if (!raw) return;
    const map = JSON.parse(raw);
    for (const folderId of folderIds) {
      delete map[folderId];
    }
    localStorage.setItem(FOLDER_COLORS_KEY, JSON.stringify(map));
  } catch {}
}

function getFolderColor(folderId) {
  const colorId = getFolderColorId(folderId);
  return colorId ? (FOLDER_COLORS.find((c) => c.id === colorId) || null) : null;
}

function handleColorSwatchClick(event) {
  const swatch = event.target instanceof Element ? event.target.closest("[data-color]") : null;
  if (!swatch) return;
  state.newFolderColorId = swatch.dataset.color;
  updateColorPickerUI();
}

async function handleDeleteFolder() {
  if (state.currentFolderId === ROOT_FOLDER_ID || state.busy) {
    return;
  }

  const currentFolder = getCurrentFolder();
  if (!currentFolder) {
    return;
  }

  const confirmed = await promptConfirm({
    title: "Delete folder",
    message: `Delete "${currentFolder.name}" and move its files to All files?`,
    confirmLabel: "Delete",
    destructive: true,
  });
  if (!confirmed) {
    return;
  }

  try {
    const payload = await fetchJson(
      `/api/library/folders/${encodeURIComponent(currentFolder.id)}`,
      {
        method: "DELETE",
      },
      "Could not delete folder.",
    );
    removeFolderColors(payload.result?.deleted_folder_ids || [currentFolder.id]);
    state.currentFolderId = ROOT_FOLDER_ID;
    closeFolderDropdown();
    setFlash("success", "Folder deleted");
    await refreshLibrary();
  } catch (error) {
    setFlash("error", error.message || "Could not delete folder.");
  }
}

function updateColorPickerUI() {
  for (const swatch of folderColorPicker.querySelectorAll("[data-color]")) {
    swatch.classList.toggle("is-selected", swatch.dataset.color === state.newFolderColorId);
  }
}

function renderFiles() {
  const visibleFiles = getVisibleFiles();
  const showPending = state.pendingUpload !== null && state.pendingUpload.folderId === state.currentFolderId;
  const hasTimers = showPending || state.downloadingVideoId !== "" || state.sharingVideoId !== "";

  if (visibleFiles.length === 0 && !showPending) {
    stopTimerInterval();
    dataTable.hidden = true;
    filesList.innerHTML = "";
    emptyState.hidden = false;

    if (state.library.files.length === 0) {
      if (!state.library.configured) {
        emptyTitle.textContent = "No settings";
      } else if (!state.library.connected) {
        emptyTitle.textContent = "Not connected";
      } else {
        emptyTitle.textContent = "No files";
      }
    } else if (state.currentFolderId === ROOT_FOLDER_ID) {
      emptyTitle.textContent = "No files";
    } else {
      emptyTitle.textContent = "No files here";
    }
    return;
  }

  dataTable.hidden = false;
  emptyState.hidden = true;
  const pendingHtml = showPending ? renderPendingRow(state.pendingUpload) : "";
  filesList.innerHTML = pendingHtml + visibleFiles.map((file) => renderFileRow(file)).join("");

  if (hasTimers) {
    startTimerInterval();
  } else {
    stopTimerInterval();
  }
}

function renderPendingRow(pending) {
  const name = pending.name || "Uploading…";
  const startTime = pending.startTime;
  return `
    <article class="file-row file-row--pending" data-video-id="__pending__">
      <div class="file-cell">
        <div class="file-name" title="${escapeHtml(name)}">${escapeHtml(displayName(name))}</div>
      </div>
      <div class="file-value">—</div>
      <div class="file-value">—</div>
      <div class="file-value">—</div>
      <div class="file-download-cell">
        <span class="runtime-timer" data-start-time="${startTime}">${formatRuntime(Date.now() - startTime)}</span>
      </div>
    </article>
  `;
}

function renderFileRow(file) {
  const rawName = file?.original_filename || "";
  const shownName = file?.display_name || rawName;
  const isEditing = file.video_id === state.editingVideoId;
  const isSelected = file.video_id === state.selectedVideoId;
  const isDownloading = file.video_id === state.downloadingVideoId;
  const isSharing = file.video_id === state.sharingVideoId;
  const renameValue = file.display_name_override || rawName;
  let primaryBtn;
  if (isDownloading) {
    primaryBtn = `<button class="file-link file-link--timer" type="button" disabled aria-label="Downloading"><span class="runtime-timer" data-start-time="${state.downloadStartTime}">${formatRuntime(Date.now() - state.downloadStartTime)}</span></button>`;
  } else if (isSelected) {
    primaryBtn = `<button class="file-link file-link--danger" type="button" title="Delete" aria-label="Delete" data-action="delete" data-video-id="${escapeHtml(file.video_id)}">${deleteIconMarkup()}</button>`;
  } else {
    primaryBtn = `<button class="file-link" type="button" title="Download" aria-label="Download" data-action="download" data-video-id="${escapeHtml(file.video_id)}">${downloadIconMarkup()}</button>`;
  }
  const shareBtn = isSharing
    ? `<button class="file-link file-link--timer" type="button" disabled aria-label="Sharing"><span class="runtime-timer" data-start-time="${state.sharingStartTime}">${formatRuntime(Date.now() - state.sharingStartTime)}</span></button>`
    : `<button class="file-link" type="button" title="Share" aria-label="Share" data-action="share" data-video-id="${escapeHtml(file.video_id)}">${shareIconMarkup()}</button>`;
  const actionsCell = `
    <div class="file-actions">
      ${primaryBtn}
      ${shareBtn}
    </div>
  `;
  return `
    <article class="file-row${isSelected ? " is-selected" : ""}" data-video-id="${escapeHtml(file.video_id)}">
      <div class="file-cell">
        ${
          isEditing
            ? `
              <input
                class="file-name-input"
                type="text"
                value="${escapeHtml(renameValue)}"
                data-action="rename-input"
                data-video-id="${escapeHtml(file.video_id)}"
                spellcheck="false"
                autocomplete="off"
              />
            `
            : `<div class="file-name" title="${escapeHtml(rawName)}">${escapeHtml(displayName(shownName))}</div>`
        }
      </div>
      <div class="file-value">${escapeHtml(fileTypeLabel(file))}</div>
      <div class="file-value">${escapeHtml(formatBytes(file.original_size))}</div>
      <div class="file-value">${escapeHtml(formatDate(file.uploaded_at))}</div>
      <div class="file-download-cell">${actionsCell}</div>
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


function handleFilesListClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) {
    return;
  }

  const actionTarget = target.closest("[data-action='download'], [data-action='delete'], [data-action='share']");
  if (actionTarget instanceof HTMLElement) {
    if (state.busy) {
      return;
    }

    const videoId = actionTarget.dataset.videoId || "";
    if (!videoId || videoId === "__pending__") {
      return;
    }

    if (actionTarget.dataset.action === "delete") {
      void handleDeleteFile(videoId);
      return;
    }

    if (actionTarget.dataset.action === "share") {
      void handleShareFile(videoId);
      return;
    }

    void handleRemoteDownload(videoId);
    return;
  }

  if (target.closest("[data-action='rename-input']") || state.renamePending) {
    return;
  }

  const row = target.closest(".file-row");
  if (!(row instanceof HTMLElement)) {
    return;
  }

  const videoId = row.dataset.videoId || "";
  if (!videoId || videoId === "__pending__") {
    return;
  }

  if (state.editingVideoId && state.editingVideoId !== videoId) {
    return;
  }

  state.selectedVideoId = state.selectedVideoId === videoId ? "" : videoId;
  renderFiles();
}

function handleFilesListDoubleClick(event) {
  const target = event.target instanceof Element ? event.target : null;
  if (!target) {
    return;
  }

  if (
    target.closest("[data-action='download']") ||
    target.closest("[data-action='delete']") ||
    target.closest("[data-action='share']") ||
    target.closest("[data-action='rename-input']")
  ) {
    return;
  }

  const row = target.closest(".file-row");
  if (!(row instanceof HTMLElement)) {
    return;
  }

  const videoId = row.dataset.videoId || "";
  if (!videoId || videoId === "__pending__") {
    return;
  }

  startInlineRename(videoId);
}

function handleFilesListKeydown(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) || target.dataset.action !== "rename-input") {
    return;
  }

  if (event.key === "Enter") {
    event.preventDefault();
    void submitInlineRename(target.dataset.videoId || "", target.value);
    return;
  }

  if (event.key === "Escape") {
    event.preventDefault();
    cancelInlineRename();
  }
}

function handleFilesListFocusOut(event) {
  const target = event.target;
  if (!(target instanceof HTMLInputElement) || target.dataset.action !== "rename-input") {
    return;
  }

  window.setTimeout(() => {
    if (state.editingVideoId !== (target.dataset.videoId || "")) {
      return;
    }
    void submitInlineRename(target.dataset.videoId || "", target.value);
  }, 0);
}

function handleGlobalKeydown(event) {
  if (event.key !== "Escape") {
    return;
  }

  if (!confirmModal.hidden) {
    event.preventDefault();
    resolveConfirm(false);
    return;
  }

  if (!shareLinkModal.hidden) {
    event.preventDefault();
    closeShareLinkModal();
    return;
  }

  if (!sharesModal.hidden) {
    event.preventDefault();
    closeSharesModal();
    return;
  }

  if (state.searchQuery || appbarSearch.classList.contains("is-open")) {
    event.preventDefault();
    closeSearch();
    return;
  }

  if (state.folderDropdownOpen) {
    event.preventDefault();
    closeFolderDropdown();
    return;
  }

  if (!keyModal.hidden) {
    event.preventDefault();
    resolveKeyPrompt(null);
    return;
  }

  if (state.editingVideoId) {
    event.preventDefault();
    cancelInlineRename();
    return;
  }

  if (!organizeModal.hidden) {
    event.preventDefault();
    closeOrganizationModal();
    return;
  }

  if (!settingsModal.hidden) {
    event.preventDefault();
    closeSettingsModal();
  }
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
  state.pendingUpload = { name: file.name, folderId: state.currentFolderId, startTime: Date.now() };
  renderPrimaryAction();
  renderFiles();
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
    state.pendingUpload = null;
    await refreshLibrary({ fallbackFile });
    window.setTimeout(hideJobProgress, 1000);
  } catch (error) {
    hideJobProgress();
    state.pendingUpload = null;
    setFlash("error", error.message || "Upload failed.");
    renderFiles();
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
  state.downloadingVideoId = videoId;
  state.downloadStartTime = Date.now();
  renderPrimaryAction();
  renderFiles();
  clearFlash();
  setJobProgress(2, "Preparing");

  try {
    const jobIdPromise = startRemoteDownload(videoId, key);
    key = "";
    const jobId = await jobIdPromise;
    const job = await pollJob(jobId);
    const downloadName = job.metadata?.display_filename || job.metadata?.original_filename || "recovered.bin";
    setJobProgress(100, "Done");
    await startBrowserDownload(job.artifacts.recovered_file, downloadName);
    setFlash("success", downloadName);
    window.setTimeout(hideJobProgress, 1000);
  } catch (error) {
    hideJobProgress();
    setFlash("error", error.message || "Download failed.");
  } finally {
    key = "";
    state.busy = false;
    state.downloadingVideoId = "";
    state.downloadStartTime = 0;
    renderPrimaryAction();
    renderFiles();
  }
}

async function handleShareFile(videoId) {
  if (!state.library.connected) {
    setFlash("error", "Connect YouTube.");
    return;
  }

  if (!state.settings.public_app_url) {
    setFlash("error", "Create or save a Public App URL before sharing files.");
    openSettingsModal();
    return;
  }

  let key = await resolveOperationalKey();
  if (!key) {
    return;
  }

  state.busy = true;
  state.sharingVideoId = videoId;
  state.sharingStartTime = Date.now();
  renderPrimaryAction();
  renderFiles();
  clearFlash();
  setJobProgress(2, "Preparing");

  try {
    const jobId = await createShareRequest(videoId, key);
    key = "";
    const job = await pollJob(jobId);
    const shareUrl = job.metadata?.share_url || "";
    const copied = await copyToClipboard(shareUrl).catch(() => false);
    void refreshShares().catch(() => {});
    openShareLinkModal(shareUrl, copied);
    window.setTimeout(hideJobProgress, 1000);
  } catch (error) {
    hideJobProgress();
    setFlash("error", error.message || "Could not share file.");
  } finally {
    key = "";
    state.busy = false;
    state.sharingVideoId = "";
    state.sharingStartTime = 0;
    renderPrimaryAction();
    renderFiles();
  }
}

async function handleDeleteFile(videoId) {
  const file = state.library.files.find((item) => item.video_id === videoId);
  if (!file || state.busy) {
    return;
  }

  const confirmed = await promptConfirm({
    title: "Delete file",
    message: `Delete "${file.display_name || file.original_filename}" from StorageX and YouTube?`,
    confirmLabel: "Delete",
    destructive: true,
  });
  if (!confirmed) {
    return;
  }

  state.busy = true;
  renderPrimaryAction();
  clearFlash();

  try {
    await fetchJson(
      `/api/library/files/${encodeURIComponent(videoId)}`,
      {
        method: "DELETE",
      },
      "Could not delete file.",
    );
    state.selectedVideoId = "";
    state.editingVideoId = "";
    setFlash("success", "File deleted");
    await refreshLibrary();
  } catch (error) {
    setFlash("error", error.message || "Could not delete file.");
  } finally {
    state.busy = false;
    renderPrimaryAction();
  }
}

async function startUpload(file, key) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("key", key);
  formData.append("folder_id", state.currentFolderId || ROOT_FOLDER_ID);

  const payload = await fetchJson(
    "/api/files",
    {
      method: "POST",
      body: formData,
    },
    "Upload failed.",
  );

  return payload.job_id;
}

async function startRemoteDownload(videoId, key) {
  const formData = new FormData();
  formData.append("key", key);

  const payload = await fetchJson(
    `/api/files/${encodeURIComponent(videoId)}/download`,
    {
      method: "POST",
      body: formData,
    },
    "Download failed.",
  );

  return payload.job_id;
}

async function createShareRequest(videoId, key) {
  const payload = await fetchJson(
    `/api/library/files/${encodeURIComponent(videoId)}/share`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        key,
      }),
    },
    "Could not share file.",
  );
  return payload.job_id;
}

async function startShareExtension(token, key) {
  return fetchJson(
    `/api/library/shares/${encodeURIComponent(token)}/extend`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        key,
      }),
    },
    "Could not extend link.",
  );
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
  const confirmed = await promptConfirm({
    title: "Reset local YouTube setup",
    message: "Reset the saved YouTube client config and local login on this Mac?",
    confirmLabel: "Reset",
    destructive: true,
  });
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
      public_app_url: state.settings.public_app_url || "",
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
  settingsPublicAppUrl.value = state.settings.public_app_url || "";
  renderSettingsActions();
  hideSettingsError();
  window.setTimeout(() => settingsKey.focus(), 20);
}

function closeSettingsModal() {
  state.settingsOpen = false;
  settingsModal.hidden = true;
  settingsClientSecret.value = "";
  settingsPublicAppUrl.value = "";
  hideSettingsError();
  syncBodyModalState();
  restorePreviousFocus();
}

async function submitSettings(event) {
  event.preventDefault();

  const keyValue = settingsKey.value.trim();
  const clientId = settingsClientId.value.trim();
  const clientSecret = settingsClientSecret.value.trim();
  const publicAppUrl = settingsPublicAppUrl.value.trim();
  const settingsChanged = clientSecret !== "" || clientId !== (state.settings.client_id || "");
  const appSettingsChanged = publicAppUrl !== (state.settings.public_app_url || "");

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
      const payload = await fetchJson(
        "/api/settings/youtube",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            client_id: clientId,
            client_secret: clientSecret,
          }),
        },
        "Could not save settings.",
      );

      state.settings = {
        configured: Boolean(payload.configured),
        client_id: payload.client_id || "",
        has_client_secret: Boolean(payload.has_client_secret),
        source: payload.source || "runtime",
        public_app_url: state.settings.public_app_url || "",
      };
    } catch (error) {
      showSettingsError(error.message || "Could not save settings.");
      return;
    }
  }

  if (appSettingsChanged) {
    try {
      const payload = await fetchJson(
        "/api/settings/app",
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            public_app_url: publicAppUrl,
          }),
        },
        "Could not save settings.",
      );

      state.settings.public_app_url = payload.public_app_url || "";
    } catch (error) {
      showSettingsError(error.message || "Could not save settings.");
      return;
    }
  }

  if (keyValue) {
    setStoredKey(keyValue);
  }

  closeSettingsModal();

  if (!settingsChanged && !appSettingsChanged && !keyValue) {
    return;
  }

  setFlash("success", "Settings saved");

  if (settingsChanged || appSettingsChanged) {
    await refreshLibrary();
  }
}

async function createPublicUrl() {
  settingsCreatePublicUrl.disabled = true;
  hideSettingsError();

  try {
    const payload = await fetchJson(
      "/api/settings/app/public-url/quick-tunnel",
      {
        method: "POST",
      },
      "Could not create a public URL.",
    );

    state.settings.public_app_url = payload.public_app_url || "";
    settingsPublicAppUrl.value = state.settings.public_app_url;
    setFlash("success", "Public URL created");
  } catch (error) {
    showSettingsError(error.message || "Could not create a public URL.");
  } finally {
    settingsCreatePublicUrl.disabled = false;
  }
}

async function openSharesModal() {
  state.previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  state.linksOpen = true;
  sharesModal.hidden = false;
  document.body.classList.add("is-modal-open");
  hideSharesError();
  renderSharesModal();

  try {
    await refreshShares();
    startSharesPolling();
  } catch (error) {
    showSharesError(error.message || "Could not load links.");
  }
}

function closeSharesModal() {
  state.linksOpen = false;
  stopSharesPolling();
  sharesModal.hidden = true;
  hideSharesError();
  syncBodyModalState();
  restorePreviousFocus();
}

async function refreshShares() {
  state.linksLoading = true;
  renderSharesModal();

  try {
    const payload = await fetchJson("/api/library/shares", undefined, "Could not load links.");
    state.links = Array.isArray(payload.shares) ? payload.shares : [];
    hideSharesError();
  } finally {
    state.linksLoading = false;
    renderSharesModal();
  }
}

function renderSharesModal() {
  const visibleShares = getVisibleShares();

  if (state.linksLoading) {
    sharesEmpty.hidden = true;
    sharesList.innerHTML = `<div class="links-empty">Loading</div>`;
    return;
  }

  if (visibleShares.length === 0) {
    sharesList.innerHTML = "";
    sharesEmpty.hidden = false;
    return;
  }

  sharesEmpty.hidden = true;
  sharesList.innerHTML = visibleShares.map((share) => renderShareAdminCard(share)).join("");
}

function getVisibleShares() {
  if (!Array.isArray(state.links)) {
    return [];
  }

  return state.links.filter((share) => ["active", "pending", "used"].includes(share?.status || ""));
}

function startSharesPolling() {
  stopSharesPolling();
  state.linksPollTimer = window.setInterval(() => {
    if (!state.linksOpen || state.linksLoading) {
      return;
    }
    void refreshShares().catch(() => {});
  }, 3000);
}

function stopSharesPolling() {
  if (!state.linksPollTimer) {
    return;
  }
  window.clearInterval(state.linksPollTimer);
  state.linksPollTimer = null;
}

function renderShareAdminCard(share) {
  const downloads = Array.isArray(share.downloads) ? share.downloads : [];
  const canCopy = Boolean(share.share_url) && ["active", "pending"].includes(share.status);
  const canRevoke = ["active", "pending"].includes(share.status);
  const isInactive = ["expired", "revoked", "invalid"].includes(share.status);
  const dlCount = share.download_count || 0;
  const metaParts = [
    formatShortDate(share.created_at || ""),
    `Exp. ${formatShortDate(share.expires_at || "")}`,
    `${dlCount} ${dlCount === 1 ? "download" : "downloads"}`,
  ];

  return `
    <article class="link-card${isInactive ? " link-card--inactive" : ""}" data-share-token="${escapeHtml(share.token || "")}">
      <div class="link-card__head">
        <div class="link-card__info">
          <span class="link-card__name">${escapeHtml(share.display_name || share.original_filename || "Shared file")}</span>
          <span class="link-card__meta">${escapeHtml(metaParts.join(" · "))}</span>
        </div>
        ${renderShareAdminStatus(share)}
      </div>

      <div class="link-card__url">
        <input class="field-input link-card__url-input" type="url" readonly value="${escapeHtml(share.share_url || "")}" />
        <button class="btn btn--ghost" type="button" data-action="copy-share-link" data-token="${escapeHtml(share.token || "")}" ${canCopy ? "" : "disabled"}>Copy</button>
        ${canRevoke ? `<button class="btn btn--danger" type="button" data-action="revoke-share" data-token="${escapeHtml(share.token || "")}">Revoke</button>` : ""}
      </div>

      ${downloads.length ? `<div class="link-card__history">${downloads.map((d) => renderShareDownloadRow(d)).join("")}</div>` : ""}
    </article>
  `;
}

function renderShareAdminStatus(share) {
  const status = share.status || "invalid";
  const label = shareStatusLabel(status);

  if (status !== "used") {
    return `<span class="share-admin-status share-admin-status--${escapeHtml(status)}">${escapeHtml(label)}</span>`;
  }

  const disabled = state.busy || state.extendingShareToken === share.token;
  const ariaLabel = `Allow one more download for ${share.display_name || share.original_filename || "this file"}`;

  return `
    <button
      class="share-admin-status share-admin-status--used share-admin-status--action"
      type="button"
      data-action="extend-share"
      data-token="${escapeHtml(share.token || "")}"
      aria-label="${escapeHtml(ariaLabel)}"
      title="Allow one more download"
      ${disabled ? "disabled" : ""}
    >${escapeHtml(label)}</button>
  `;
}

function renderShareDownloadRow(download) {
  return `
    <div class="link-history-row">
      <span class="link-history-row__time">${escapeHtml(formatDate(download.downloaded_at || ""))}</span>
      <span class="link-history-row__ip">${escapeHtml(download.ip_address || "—")}</span>
      <span class="link-history-row__ua">${escapeHtml(download.user_agent || "Unknown")}</span>
    </div>
  `;
}

function shareStatusLabel(status) {
  const labels = {
    active: "Active",
    pending: "Pending",
    revoked: "Revoked",
    expired: "Expired",
    used: "Used",
    invalid: "Invalid",
  };
  return labels[status] || "Unknown";
}

async function handleSharesListClick(event) {
  const actionTarget = event.target instanceof Element ? event.target.closest("[data-action][data-token]") : null;
  if (!(actionTarget instanceof HTMLElement)) {
    return;
  }

  const token = actionTarget.dataset.token || "";
  const share = state.links.find((item) => item.token === token);
  if (!share) {
    return;
  }

  if (actionTarget.dataset.action === "copy-share-link") {
    const copied = await copyToClipboard(share.share_url || "").catch(() => false);
    if (copied) {
      actionTarget.textContent = "Copied!";
      actionTarget.classList.add("is-copied");
      actionTarget.disabled = true;
      window.setTimeout(() => {
        if (actionTarget.isConnected) {
          actionTarget.textContent = "Copy";
          actionTarget.classList.remove("is-copied");
          actionTarget.disabled = false;
        }
      }, 2000);
      return;
    }
    openShareLinkModal(share.share_url || "", false);
    return;
  }

  if (actionTarget.dataset.action === "extend-share") {
    await handleExtendShare(share);
    return;
  }

  if (actionTarget.dataset.action === "revoke-share") {
    await handleRevokeShare(share);
  }
}

async function handleExtendShare(share) {
  if (state.busy) {
    return;
  }

  let key = await resolveOperationalKey();
  if (!key) {
    return;
  }

  state.busy = true;
  state.extendingShareToken = share.token;
  hideSharesError();
  clearFlash();
  renderPrimaryAction();
  renderSharesModal();
  setJobProgress(2, "Preparing");

  try {
    const payload = await startShareExtension(share.token, key);
    if (payload.share) {
      state.links = state.links.map((item) => (item.token === share.token ? payload.share : item));
      renderSharesModal();
    }

    key = "";
    await pollJob(payload.job_id);
    await refreshShares();
    setFlash("success", "Link extended");
    window.setTimeout(hideJobProgress, 1000);
  } catch (error) {
    hideJobProgress();
    await refreshShares().catch(() => {});
    showSharesError(error.message || "Could not extend link.");
  } finally {
    key = "";
    state.busy = false;
    state.extendingShareToken = "";
    renderPrimaryAction();
    renderSharesModal();
  }
}

async function handleRevokeShare(share) {
  const confirmed = await promptConfirm({
    title: "Revoke link",
    message: `Revoke "${share.display_name || share.original_filename}"?`,
    confirmLabel: "Revoke",
    destructive: true,
  });
  if (!confirmed) {
    return;
  }

  try {
    await fetchJson(
      `/api/library/shares/${encodeURIComponent(share.token)}/revoke`,
      {
        method: "POST",
      },
      "Could not revoke link.",
    );
    setFlash("success", "Link revoked");
    await refreshShares();
  } catch (error) {
    showSharesError(error.message || "Could not revoke link.");
  }
}

function showSharesError(message) {
  sharesError.hidden = false;
  sharesError.textContent = message;
}

function hideSharesError() {
  sharesError.hidden = true;
  sharesError.textContent = "";
}

function startInlineRename(videoId) {
  if (state.busy || state.renamePending) {
    return;
  }

  const file = state.library.files.find((item) => item.video_id === videoId);
  if (!file) {
    return;
  }

  state.editingVideoId = videoId;
  state.selectedVideoId = videoId;
  renderFiles();
  window.setTimeout(() => {
    const input = filesList.querySelector(`[data-action="rename-input"][data-video-id="${CSS.escape(videoId)}"]`);
    if (input instanceof HTMLInputElement) {
      input.focus();
      input.select();
    }
  }, 20);
}

function cancelInlineRename() {
  if (!state.editingVideoId) {
    return;
  }

  state.editingVideoId = "";
  renderFiles();
}

async function submitInlineRename(videoId, nextValue) {
  if (!videoId || state.editingVideoId !== videoId || state.renamePending) {
    return;
  }

  const file = state.library.files.find((item) => item.video_id === videoId);
  if (!file) {
    cancelInlineRename();
    return;
  }

  const trimmedName = String(nextValue || "").trim();
  if (!trimmedName) {
    cancelInlineRename();
    return;
  }

  const currentName = file.display_name_override || file.original_filename;
  if (trimmedName === currentName) {
    cancelInlineRename();
    return;
  }

  const payload = {
    display_name: trimmedName === file.original_filename ? null : trimmedName,
  };

  try {
    state.renamePending = true;
    const response = await fetchJson(
      `/api/library/files/${encodeURIComponent(videoId)}`,
      {
        method: "PATCH",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(payload),
      },
      "Could not rename file.",
    );

    const entry = response.file || {};
    const override = typeof entry.display_name === "string" && entry.display_name.trim() ? entry.display_name.trim() : null;
    file.display_name_override = override;
    file.display_name = override || file.original_filename;
    state.editingVideoId = "";
    state.renamePending = false;
    syncLibraryCache(state.library);
    renderFiles();
  } catch (error) {
    state.renamePending = false;
    setFlash("error", error.message || "Could not rename file.");
    window.setTimeout(() => {
      const input = filesList.querySelector(`[data-action="rename-input"][data-video-id="${CSS.escape(videoId)}"]`);
      if (input instanceof HTMLInputElement) {
        input.focus();
        input.select();
      }
    }, 20);
  }
}

function openOrganizationModal(action) {
  if (!action) {
    return;
  }

  if (action.type !== "create-folder") {
    return;
  }

  state.previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  state.organizationAction = action;
  state.organizationOpen = true;
  organizeModal.hidden = false;
  document.body.classList.add("is-modal-open");
  hideOrganizationError();

  organizeName.value = "";
  organizeTitle.textContent = "New folder";
  organizeNameLabel.textContent = "Folder name";
  organizeSubmit.textContent = "Create";
  organizeNameField.hidden = false;
  folderColorField.hidden = false;
  state.newFolderColorId = FOLDER_COLORS[0].id;
  updateColorPickerUI();
  window.setTimeout(() => organizeName.focus(), 20);
}

function closeOrganizationModal() {
  state.organizationOpen = false;
  state.organizationAction = null;
  organizeModal.hidden = true;
  organizeName.value = "";
  folderColorField.hidden = true;
  hideOrganizationError();
  syncBodyModalState();
  restorePreviousFocus();
}

async function submitOrganizationAction(event) {
  event.preventDefault();
  const action = state.organizationAction;
  if (!action) {
    return;
  }

  try {
    if (action.type === "create-folder") {
      const name = organizeName.value.trim();
      if (!name) {
        showOrganizationError("Folder name is required.");
        return;
      }
      const response = await createFolderRequest(name, state.currentFolderId);
      const newFolderId = response?.folder?.id;
      if (newFolderId && state.newFolderColorId) {
        setFolderColorId(newFolderId, state.newFolderColorId);
      }
      closeOrganizationModal();
      setFlash("success", "Folder created");
      await refreshLibrary();
      return;
    }
  } catch (error) {
    showOrganizationError(error.message || "Could not save changes.");
  }
}

async function createFolderRequest(name, parentId, allowRetry = true) {
  try {
    return await fetchJson(
      "/api/library/folders",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name,
          parent_id: parentId || ROOT_FOLDER_ID,
        }),
      },
      "Could not create folder.",
    );
  } catch (error) {
    if (allowRetry && error instanceof Error && error.message === "Folder not found.") {
      await refreshLibrary();
      const fallbackParentId = getCurrentFolder()?.id || ROOT_FOLDER_ID;
      return createFolderRequest(name, fallbackParentId, false);
    }
    throw error;
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

function showOrganizationError(message) {
  organizeError.hidden = false;
  organizeError.textContent = message;
}

function hideOrganizationError() {
  organizeError.hidden = true;
  organizeError.textContent = "";
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

async function copyToClipboard(value) {
  if (!value) {
    throw new Error("Could not copy the share link.");
  }

  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value);
      return true;
    } catch {
      // Fall through to legacy copy methods when the browser blocks Clipboard API access.
    }
  }

  const field = document.createElement("textarea");
  field.value = value;
  field.setAttribute("readonly", "true");
  field.style.position = "fixed";
  field.style.opacity = "0";
  document.body.append(field);
  field.select();
  field.setSelectionRange(0, field.value.length);
  let copied = false;
  try {
    copied = document.execCommand("copy");
  } catch {
    copied = false;
  }
  field.remove();

  if (!copied) {
    return false;
  }

  return true;
}

function openShareLinkModal(url, alreadyCopied = false) {
  state.shareLinkOpen = true;
  shareLinkModal.hidden = false;
  shareLinkInput.value = url;
  document.body.classList.add("is-modal-open");
  shareLinkCopyBtn.disabled = false;
  shareLinkCopyBtn.textContent = alreadyCopied ? "Copied!" : "Copy";
  if (alreadyCopied) {
    window.setTimeout(() => {
      if (!shareLinkModal.hidden) {
        shareLinkCopyBtn.textContent = "Copy";
      }
    }, 2000);
  }
  window.setTimeout(() => {
    shareLinkInput.focus();
    shareLinkInput.select();
  }, 20);
}

function closeShareLinkModal() {
  state.shareLinkOpen = false;
  shareLinkModal.hidden = true;
  shareLinkInput.value = "";
  shareLinkCopyBtn.textContent = "Copy";
  syncBodyModalState();
}

async function handleShareLinkCopy() {
  const url = shareLinkInput.value;
  const copied = await copyToClipboard(url).catch(() => false);
  if (copied) {
    shareLinkCopyBtn.textContent = "Copied!";
    window.setTimeout(() => {
      if (!shareLinkModal.hidden) {
        shareLinkCopyBtn.textContent = "Copy";
      }
    }, 2000);
  }
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

function promptConfirm(options = {}) {
  if (state.confirmResolver) {
    resolveConfirm(false);
  }

  state.confirmPreviousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
  state.confirmOpen = true;
  confirmModal.hidden = false;
  document.body.classList.add("is-modal-open");
  confirmTitle.textContent = options.title || "Confirm";
  confirmMessage.textContent = options.message || "";
  confirmSubmit.textContent = options.confirmLabel || "Confirm";
  confirmSubmit.classList.toggle("btn--danger", options.destructive !== false);
  confirmSubmit.classList.toggle("btn--primary", options.destructive === false);

  return new Promise((resolve) => {
    state.confirmResolver = resolve;
    window.setTimeout(() => confirmSubmit.focus(), 20);
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
  syncBodyModalState();
  restorePreviousFocus();
  resolver(value);
}

function resolveConfirm(value) {
  if (!state.confirmResolver) {
    return;
  }

  const resolver = state.confirmResolver;
  state.confirmResolver = null;
  state.confirmOpen = false;
  confirmModal.hidden = true;
  syncBodyModalState();
  if (state.confirmPreviousFocus) {
    state.confirmPreviousFocus.focus();
  }
  state.confirmPreviousFocus = null;
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

function hydrateLibraryFromCache() {
  const cachedLibrary = getCachedLibrary();
  if (!cachedLibrary) {
    return;
  }

  state.library = cachedLibrary;
  normalizeViewState();
  renderLibraryState();
}

function getCachedLibrary() {
  try {
    const raw = localStorage.getItem(LIBRARY_CACHE_KEY);
    if (!raw) {
      return null;
    }

    const payload = JSON.parse(raw);
    const folders = normalizeFolders(payload?.folders);
    const files = normalizeFiles(payload?.files, folders);
    const hasMeaningfulFolders = folders.some((folder) => folder.id !== ROOT_FOLDER_ID);
    if (files.length === 0 && !hasMeaningfulFolders) {
      return null;
    }

    return {
      configured: Boolean(payload?.configured),
      connected: Boolean(payload?.connected),
      connect_url: payload?.connect_url || CONNECT_URL,
      files,
      folders,
      index_recovered: Boolean(payload?.index_recovered),
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

  const hasMeaningfulFolders = (library.folders || []).some((folder) => folder.id !== ROOT_FOLDER_ID);
  if (library.files.length > 0 || hasMeaningfulFolders) {
    try {
      localStorage.setItem(
        LIBRARY_CACHE_KEY,
        JSON.stringify({
          configured: library.configured,
          connected: library.connected,
          connect_url: library.connect_url,
          files: library.files,
          folders: library.folders,
          index_recovered: library.index_recovered,
        }),
      );
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

function hydrateViewState() {
  try {
    const raw = localStorage.getItem(VIEW_STATE_KEY);
    if (!raw) {
      return;
    }

    const payload = JSON.parse(raw);
    state.currentFolderId = typeof payload?.current_folder_id === "string" ? payload.current_folder_id : ROOT_FOLDER_ID;
  } catch {
    state.currentFolderId = ROOT_FOLDER_ID;
  }
}

function persistViewState() {
  try {
    localStorage.setItem(
      VIEW_STATE_KEY,
      JSON.stringify({
        current_folder_id: state.currentFolderId,
      }),
    );
  } catch {
    return;
  }
}

function normalizeViewState() {
  if (!getFolderById(state.currentFolderId)) {
    state.currentFolderId = ROOT_FOLDER_ID;
  }

  if (state.editingVideoId && !state.library.files.some((file) => file.video_id === state.editingVideoId)) {
    state.editingVideoId = "";
  }

  if (
    state.selectedVideoId &&
    !state.library.files.some((file) => file.video_id === state.selectedVideoId && file.folder_id === state.currentFolderId)
  ) {
    state.selectedVideoId = "";
  }
}

function normalizeFolders(folders) {
  const records = Array.isArray(folders) ? folders : [];
  const folderMap = new Map();
  folderMap.set(ROOT_FOLDER_ID, {
    id: ROOT_FOLDER_ID,
    name: "All files",
    parent_id: null,
  });

  for (const folder of records) {
    if (!folder || typeof folder.id !== "string" || !folder.id) {
      continue;
    }
    if (folder.id === ROOT_FOLDER_ID) {
      continue;
    }
    const name = String(folder.name || "").trim();
    const parentId = typeof folder.parent_id === "string" && folder.parent_id ? folder.parent_id : ROOT_FOLDER_ID;
    if (!name) {
      continue;
    }
    folderMap.set(folder.id, {
      id: folder.id,
      name,
      parent_id: parentId,
    });
  }

  return Array.from(folderMap.values()).sort((left, right) => {
    if (left.id === ROOT_FOLDER_ID) {
      return -1;
    }
    if (right.id === ROOT_FOLDER_ID) {
      return 1;
    }
    if (left.parent_id !== right.parent_id) {
      return String(left.parent_id || "").localeCompare(String(right.parent_id || ""));
    }
    return left.name.localeCompare(right.name, undefined, { sensitivity: "base" });
  });
}

function normalizeFiles(files, folders) {
  const folderIds = new Set((folders || []).map((folder) => folder.id));
  const records = Array.isArray(files) ? files : [];
  return records.map((file) => normalizeFile(file, folderIds));
}

function normalizeFile(file, folders) {
  const folderIds = folders instanceof Set ? folders : new Set((folders || []).map((folder) => folder.id));
  const originalName = String(file?.original_filename || "");
  const displayNameOverride = typeof file?.display_name_override === "string" && file.display_name_override.trim()
    ? file.display_name_override.trim()
    : null;
  const folderId = typeof file?.folder_id === "string" && folderIds.has(file.folder_id) ? file.folder_id : ROOT_FOLDER_ID;

  return {
    ...file,
    video_id: String(file?.video_id || ""),
    original_filename: originalName,
    media_type: String(file?.media_type || ""),
    original_size: Number(file?.original_size) || 0,
    uploaded_at: file?.uploaded_at || "",
    folder_id: folderId,
    display_name_override: displayNameOverride,
    display_name: displayNameOverride || originalName,
  };
}

function getFolderById(folderId) {
  return state.library.folders.find((folder) => folder.id === folderId) || null;
}

function getCurrentFolder() {
  return getFolderById(state.currentFolderId) || getFolderById(ROOT_FOLDER_ID);
}

function buildFolderChildMap() {
  const childMap = new Map();
  for (const folder of state.library.folders) {
    const parentId = folder.parent_id || null;
    if (!childMap.has(parentId)) {
      childMap.set(parentId, []);
    }
    childMap.get(parentId).push(folder);
  }

  for (const children of childMap.values()) {
    children.sort((left, right) => {
      if (left.id === ROOT_FOLDER_ID) {
        return -1;
      }
      if (right.id === ROOT_FOLDER_ID) {
        return 1;
      }
      return left.name.localeCompare(right.name, undefined, { sensitivity: "base" });
    });
  }

  return childMap;
}

function getVisibleFiles() {
  const files = state.library.files.filter((file) => file.folder_id === state.currentFolderId);
  const q = state.searchQuery.trim().toLowerCase();
  const filtered = q ? files.filter((file) => (file.display_name || file.original_filename).toLowerCase().includes(q)) : files;
  return sortFiles(filtered);
}

function sortFiles(files) {
  const { sortKey, sortDir } = state;
  const dir = sortDir === "asc" ? 1 : -1;
  return [...files].sort((a, b) => {
    switch (sortKey) {
      case "name": {
        const an = (a.display_name || a.original_filename).toLowerCase();
        const bn = (b.display_name || b.original_filename).toLowerCase();
        return dir * an.localeCompare(bn);
      }
      case "type":
        return dir * fileTypeLabel(a).localeCompare(fileTypeLabel(b));
      case "size":
        return dir * (a.original_size - b.original_size);
      case "uploaded_at":
        return dir * (a.uploaded_at || "").localeCompare(b.uploaded_at || "");
      default:
        return 0;
    }
  });
}

function handleSortClick(event) {
  const btn = event.target instanceof Element ? event.target.closest(".col-sort") : null;
  if (!btn) return;
  const key = btn.dataset.sort;
  if (!key) return;
  if (state.sortKey === key) {
    state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
  } else {
    state.sortKey = key;
    state.sortDir = key === "uploaded_at" ? "desc" : "asc";
  }
  renderSortHeaders();
  renderFiles();
}

function renderSortHeaders() {
  for (const btn of dataTableHead.querySelectorAll(".col-sort")) {
    const isActive = btn.dataset.sort === state.sortKey;
    btn.classList.toggle("is-active", isActive);
    btn.classList.toggle("is-asc", isActive && state.sortDir === "asc");
  }
}

function formatRuntime(ms) {
  const totalSeconds = Math.floor(ms / 1000);
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function startTimerInterval() {
  if (state.timerInterval) return;
  state.timerInterval = window.setInterval(tickTimers, 1000);
}

function stopTimerInterval() {
  if (!state.timerInterval) return;
  window.clearInterval(state.timerInterval);
  state.timerInterval = null;
}

function tickTimers() {
  const now = Date.now();
  for (const el of filesList.querySelectorAll(".runtime-timer[data-start-time]")) {
    const start = Number(el.dataset.startTime) || 0;
    el.textContent = formatRuntime(now - start);
  }
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

function formatShortDate(value) {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric" }).format(date);
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

function syncBodyModalState() {
  const anyModalOpen = state.settingsOpen || state.organizationOpen || state.confirmOpen || state.shareLinkOpen || state.linksOpen || !keyModal.hidden;
  document.body.classList.toggle("is-modal-open", anyModalOpen);
}

function restorePreviousFocus() {
  if (state.previousFocus) {
    state.previousFocus.focus();
  }
  state.previousFocus = null;
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

function shareIconMarkup() {
  return `
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/>
      <path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>
    </svg>
  `;
}

function deleteIconMarkup() {
  return `
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.85" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
      <path d="M3 6h18" />
      <path d="M8 6V4h8v2" />
      <path d="M19 6l-1 14H6L5 6" />
      <path d="M10 10v6M14 10v6" />
    </svg>
  `;
}
