from pathlib import Path

APP_MAGIC = "storagex-bit-video"
FORMAT_VERSION = 2
FRAME_MAGIC = b"BVF1"

FRAME_WIDTH = 1280
FRAME_HEIGHT = 720
CELL_SIZE = 4
QUIET_MARGIN = 16
FPS = 24

GRID_COLS = (FRAME_WIDTH - (QUIET_MARGIN * 2)) // CELL_SIZE
GRID_ROWS = (FRAME_HEIGHT - (QUIET_MARGIN * 2)) // CELL_SIZE

FINDER_SIZE = 7
TIMING_INDEX = 8

MAX_UPLOAD_SIZE = 10 * 1024 * 1024
MAX_DECODE_UPLOAD_SIZE = 250 * 1024 * 1024
JOB_TTL_SECONDS = 24 * 60 * 60

FRAME_PATTERN = "frame_%06d.png"
VIDEO_NAME = "output/video.webm"
YOUTUBE_VIDEO_NAME = "output/youtube-upload.mp4"
MANIFEST_NAME = "output/manifest.json"
FRAMES_ARCHIVE_NAME = "output/frames.zip"
RECOVERED_MANIFEST_NAME = "output/recovered_manifest.json"

DATA_DIR = Path("data")
JOBS_DIR = DATA_DIR / "jobs"

ALLOWED_INPUT_EXTENSIONS = {
    ".txt",
    ".mp4",
    ".pdf",
    ".jpeg",
    ".jpg",
    ".png",
}
ALLOWED_DECODE_EXTENSIONS = {".webm"}
