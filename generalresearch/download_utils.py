import logging
import os
from email.utils import formatdate, parsedate_to_datetime
from pathlib import Path
from uuid import uuid4

import requests

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def download_if_not_exists(url: str, local_path: str | Path) -> bool:
    os.makedirs(local_path.parent, exist_ok=True)
    if os.path.exists(local_path):
        logger.info("Local file exists. Not downloading.")
        return False
    _download(url, local_path)
    return True


def download_if_modified(url: str, local_path: str | Path) -> bool:
    local_path = Path(local_path)
    os.makedirs(local_path.parent, exist_ok=True)

    head = requests.head(url)
    try:
        head.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Failed to HEAD: {e}, Downloading.")
        _download(url, local_path)
        return True

    last_modified = head.headers.get("Last-Modified")
    if not last_modified:
        logger.info("Remote file has no Last-Modified header. Downloading.")
        _download(url, local_path)
        return True

    remote_dt = parsedate_to_datetime(last_modified)

    if os.path.exists(local_path):
        local_ts = os.path.getmtime(local_path)
        local_dt = parsedate_to_datetime(formatdate(local_ts, usegmt=True))
        logger.info(f"Local file found, last modified: {local_dt}")
        if remote_dt.timestamp() <= local_ts + 1:
            logger.info(f"Remote file hasn't changed since {remote_dt}. Skipping.")
            return False
        else:
            logger.info(f"Remote file is newer! {remote_dt}. Downloading.")
    else:
        logger.info("Local file not found, Downloading.")

    _download(url, local_path)
    return True


def _download(url: str, local_path: str | Path):
    local_path = Path(local_path)
    local_path_tmp = local_path.with_name(f"{local_path.name}.{uuid4().hex}.tmp")

    try:
        with requests.get(
            url,
            stream=True,
            timeout=(10, 30),  # 10s connect, 30s waiting for next chunk
        ) as resp:
            resp.raise_for_status()

            with open(local_path_tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 256):
                    if chunk:
                        f.write(chunk)

        os.replace(local_path_tmp, local_path)

    except Exception:
        try:
            local_path_tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise
