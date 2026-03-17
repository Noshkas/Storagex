const page = window.__SHARE_PAGE__ || {};
const share = page.share || null;

const shareBadge = document.querySelector("#share-badge");
const shareName = document.querySelector("#share-name");
const shareFileIcon = document.querySelector("#share-file-icon");
const shareType = document.querySelector("#share-type");
const shareSize = document.querySelector("#share-size");
const shareExpiry = document.querySelector("#share-expiry");
const shareDownload = document.querySelector("#share-download");
const shareMessage = document.querySelector("#share-message");

renderPage();

function renderPage() {
  const status = page.status || "invalid";

  if (share) {
    shareName.textContent = share.display_name || share.original_filename || "Shared file";
    shareType.textContent = fileTypeLabel(share.original_filename || share.display_name || "", share.media_type || "");
    shareSize.textContent = formatBytes(share.original_size || 0);
    shareExpiry.textContent = formatDate(share.expires_at || "");
  } else {
    shareName.textContent = "Unavailable";
    shareType.textContent = "?";
    shareSize.textContent = "—";
    shareExpiry.textContent = "—";
  }

  applyStatus(status);

  if (status === "active") {
    shareDownload.href = share.download_url || "#";
    shareDownload.removeAttribute("aria-disabled");
    hideMessage();
    return;
  }

  shareDownload.setAttribute("aria-disabled", "true");
  shareDownload.removeAttribute("href");

  if (status === "used") {
    shareDownload.classList.add("share-download-btn--used");
    shareDownload.innerHTML = `
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 6 9 17l-5-5"/></svg>
      Already claimed
    `;
    showMessage("muted", "This link has already been used. Share links are single-use only.");
  } else {
    shareDownload.classList.add("is-disabled");
    shareFileIcon.classList.add("is-unavailable");
    if (status === "pending") {
      showMessage("info", page.message || "This file is being prepared. Refresh in a moment.");
    } else {
      showMessage("error", page.message || "This share link is not available.");
    }
  }
}

function applyStatus(status) {
  const badgeCopy = {
    active: "Ready",
    pending: "Preparing",
    used: "Used",
    expired: "Expired",
    revoked: "Closed",
    invalid: "Invalid",
  };
  shareBadge.textContent = badgeCopy[status] || "Share";
  shareBadge.className = `share-badge share-badge--${status || "invalid"}`;
}

function showMessage(type, value) {
  shareMessage.hidden = false;
  shareMessage.textContent = value;
  shareMessage.className = `share-message share-message--${type}`;
}

function hideMessage() {
  shareMessage.hidden = true;
  shareMessage.textContent = "";
  shareMessage.className = "share-message";
}

function fileTypeLabel(filename, mediaType) {
  const match = /\.([^.]+)$/.exec(filename || "");
  if (match) {
    return match[1].slice(0, 4).toUpperCase();
  }
  if (mediaType) {
    return (mediaType.split("/")[0] || "FILE").slice(0, 4).toUpperCase();
  }
  return "FILE";
}

function formatBytes(value) {
  const bytes = Number(value) || 0;
  if (bytes < 1024) {
    return `${bytes} B`;
  }

  const units = ["KB", "MB", "GB", "TB"];
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
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}
