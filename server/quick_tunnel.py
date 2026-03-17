from __future__ import annotations

import re
import shutil
import subprocess
import threading
import time

QUICK_TUNNEL_URL_PATTERN = re.compile(r"https://[a-z0-9-]+\.trycloudflare\.com", re.IGNORECASE)


class QuickTunnelError(RuntimeError):
    pass


class QuickTunnelManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._process: subprocess.Popen[str] | None = None
        self._public_url: str | None = None
        self._reader_thread: threading.Thread | None = None

    def ensure_started(self, *, local_url: str) -> str:
        with self._lock:
            if self._process is not None and self._process.poll() is None and self._public_url:
                return self._public_url

            executable = shutil.which("cloudflared")
            if not executable:
                raise QuickTunnelError("cloudflared is not installed.")

            self.stop()

            process = subprocess.Popen(
                [executable, "tunnel", "--url", local_url],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            if process.stdout is None:
                process.terminate()
                raise QuickTunnelError("cloudflared did not expose tunnel output.")

            public_url_ready = threading.Event()
            tunnel_url: dict[str, str] = {}

            def _reader() -> None:
                assert process.stdout is not None
                for line in process.stdout:
                    match = QUICK_TUNNEL_URL_PATTERN.search(line)
                    if match and "url" not in tunnel_url:
                        tunnel_url["url"] = match.group(0)
                        public_url_ready.set()

            reader_thread = threading.Thread(target=_reader, daemon=True)
            reader_thread.start()

            if not public_url_ready.wait(timeout=20):
                process.terminate()
                raise QuickTunnelError("cloudflared did not return a public URL in time.")

            self._process = process
            self._public_url = tunnel_url["url"]
            self._reader_thread = reader_thread
            return self._public_url

    def stop(self) -> None:
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self._process.kill()
                    self._process.wait(timeout=5)
            self._process = None
            self._public_url = None
            self._reader_thread = None
