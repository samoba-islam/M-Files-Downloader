import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import queue
import re
import threading
import time
import uuid
import zipfile
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from flask import Flask, flash, redirect, render_template, request, send_file, session, url_for

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me-in-production")
MFWS_VIEW_LIMIT = int(os.environ.get("MFWS_VIEW_LIMIT", "50000"))
DOWNLOAD_ROOT = os.environ.get("DOWNLOAD_DIR", os.path.join(os.path.dirname(__file__), "downloads"))

DOWNLOAD_JOBS: Dict[str, Dict] = {}
DOWNLOAD_JOBS_LOCK = threading.Lock()


class MFilesError(Exception):
    pass


class MFilesClient:
    def __init__(self, base_url: str, token: Optional[str] = None, timeout: int = 60):
        self.base_url = self._normalize_base_url(base_url)
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if token:
            self.session.headers["X-Authentication"] = token

    @staticmethod
    def _normalize_base_url(base_url: str) -> str:
        base = base_url.strip().rstrip("/")
        if not base.lower().endswith("/rest"):
            base = f"{base}/REST"
        return base

    def _url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    def authenticate(self, username: str, password: str, vault_guid: str) -> str:
        payload = {
            "Username": username,
            "Password": password,
            "VaultGuid": vault_guid,
        }
        response = self.session.post(
            self._url("server/authenticationtokens"),
            json=payload,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise MFilesError(f"Authentication failed: {response.status_code} {response.text}")

        token = ""
        if "application/json" in response.headers.get("Content-Type", ""):
            raw = response.json()
            if isinstance(raw, str):
                token = raw
            elif isinstance(raw, dict):
                token = raw.get("Value") or raw.get("value") or ""
        else:
            token = response.text.strip().strip('"')

        token = token.strip().strip('"')
        if not token:
            raise MFilesError("Authentication token was empty.")

        self.session.headers["X-Authentication"] = token
        return token

    def get_view_contents(self, path: str = "") -> Dict:
        endpoint = f"views/{path.strip('/')}/items" if path else "views/items"
        endpoint = f"{endpoint}?limit={MFWS_VIEW_LIMIT}"
        response = self.session.get(self._url(endpoint), timeout=self.timeout)
        if response.status_code >= 400:
            raise MFilesError(f"Failed to read view contents: {response.status_code} {response.text}")
        return response.json()

    def get_object_files(self, obj_type: int, obj_id: int, version: str) -> List[Dict]:
        requested_version = str(version or "latest").strip() or "latest"
        versions_to_try = [requested_version]
        if requested_version.lower() != "latest":
            versions_to_try.append("latest")

        last_error = None
        for ver in versions_to_try:
            endpoints = [
                f"objects/{obj_type}/{obj_id}/{ver}/files",
                f"objects/{obj_type}/{obj_id}/{ver}/files.aspx",
            ]
            for endpoint in endpoints:
                response = self.session.get(self._url(endpoint), timeout=self.timeout)
                if response.status_code >= 400:
                    last_error = (
                        f"Failed to read object files for {obj_type}:{obj_id}:{ver}: "
                        f"{response.status_code} {response.text}"
                    )
                    continue

                data = response.json()
                if isinstance(data, list):
                    files = data
                elif isinstance(data, dict) and isinstance(data.get("Items"), list):
                    files = data["Items"]
                else:
                    files = []

                if files:
                    return files

            # Fallback: object-version payload may include Files collection.
            try:
                object_info = self.get_object_version(obj_type, obj_id, ver)
            except Exception as ex:
                last_error = str(ex)
                object_info = {}
            if isinstance(object_info, dict):
                files = object_info.get("Files")
                if isinstance(files, list) and files:
                    return files

        if last_error:
            raise MFilesError(last_error)
        return []

    def get_object_version(self, obj_type: int, obj_id: int, version: str) -> Dict:
        endpoints = [
            f"objects/{obj_type}/{obj_id}/{version}",
            f"objects/{obj_type}/{obj_id}/{version}.aspx",
        ]
        last_error = None
        for endpoint in endpoints:
            response = self.session.get(self._url(endpoint), timeout=self.timeout)
            if response.status_code < 400:
                data = response.json()
                if isinstance(data, dict):
                    return data
                return {}
            last_error = (
                f"Failed to read object version {obj_type}:{obj_id}:{version}: "
                f"{response.status_code} {response.text}"
            )
        if last_error:
            raise MFilesError(last_error)
        return {}

    def get_related_objects(self, obj_type: int, obj_id: int, version: str) -> List[Dict]:
        requested_version = str(version or "latest").strip() or "latest"
        versions_to_try = [requested_version]
        if requested_version.lower() != "latest":
            versions_to_try.append("latest")

        last_error = None
        for ver in versions_to_try:
            endpoints = [
                f"objects/{obj_type}/{obj_id}/{ver}/relationships",
                f"objects/{obj_type}/{obj_id}/{ver}/relationships.aspx",
            ]
            for endpoint in endpoints:
                response = self.session.get(self._url(endpoint), timeout=self.timeout)
                if response.status_code < 400:
                    data = response.json()
                    return data if isinstance(data, list) else []
                last_error = (
                    f"Failed to read related objects for {obj_type}:{obj_id}:{ver}: "
                    f"{response.status_code} {response.text}"
                )

        if last_error:
            raise MFilesError(last_error)
        return []

    def download_file_bytes(self, obj_type: int, obj_id: int, version: str, file_id: int) -> bytes:
        requested_version = str(version or "latest").strip() or "latest"
        versions_to_try = [requested_version]
        if requested_version.lower() != "latest":
            versions_to_try.append("latest")

        last_error = None
        for ver in versions_to_try:
            endpoints = [
                f"objects/{obj_type}/{obj_id}/{ver}/files/{file_id}/content",
                f"objects/{obj_type}/{obj_id}/{ver}/files/{file_id}/content.aspx",
                f"objects/{obj_type}/{obj_id}/{ver}/files/{file_id}/content?format=native",
                f"objects/{obj_type}/{obj_id}/{ver}/files/{file_id}/content.aspx?format=native",
            ]
            for endpoint in endpoints:
                response = self.session.get(self._url(endpoint), timeout=self.timeout)
                if response.status_code < 400:
                    return response.content
                last_error = (
                    f"Failed to download file {file_id} from {obj_type}:{obj_id}:{ver}: "
                    f"{response.status_code} {response.text}"
                )

        raise MFilesError(last_error or "File download failed.")


# -------- Parsing and mapping helpers --------

def extract_defaults_from_txt(path: str) -> Dict[str, str]:
    defaults = {
        "server": "",
        "base_url": "",
        "username": "",
        "password": "",
        "vault_guid": "",
    }
    if not os.path.exists(path):
        return defaults

    text = open(path, "r", encoding="utf-8", errors="ignore").read()

    server_match = re.search(r'NetworkAddress\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
    user_match = re.search(r'UserName\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
    pass_match = re.search(r'Password\s*:\s*"([^"]+)"', text, flags=re.IGNORECASE)
    vault_match = re.search(r'LogInToVault\s*\(\s*"(\{[^\}]+\})"\s*\)', text, flags=re.IGNORECASE)

    if server_match:
        defaults["server"] = server_match.group(1)
        defaults["base_url"] = f"http://{defaults['server']}/REST"
    if user_match:
        defaults["username"] = user_match.group(1)
    if pass_match:
        defaults["password"] = pass_match.group(1)
    if vault_match:
        defaults["vault_guid"] = vault_match.group(1)

    return defaults


def folder_token_from_item(item: Dict) -> Optional[str]:
    view = item.get("View")
    if isinstance(view, dict) and view.get("ID") is not None:
        return f"v{view['ID']}"

    traditional = item.get("TraditionalFolder")
    if isinstance(traditional, dict) and traditional.get("Item") is not None:
        return f"y{traditional['Item']}"

    property_folder = item.get("PropertyFolder")
    if isinstance(property_folder, dict):
        data_type = property_folder.get("DataType")
        serialized = property_folder.get("SerializedValue")
        if serialized:
            prefix_map = {
                1: "T",  # Text
                2: "I",  # Integer
                5: "D",  # Date
                8: "L",  # Lookup
                9: "S",  # Multi-select lookup
                "Text": "T",
                "Lookup": "L",
                "MultiLineText": "M",
                "MultiSelectLookup": "S",
            }
            prefix = prefix_map.get(data_type, "T")
            return f"{prefix}{quote(str(serialized), safe='')}"

    external_view = item.get("ExternalView")
    if isinstance(external_view, dict):
        name = external_view.get("ExternalRepositoryName")
        view_id = external_view.get("ID")
        if name and view_id is not None:
            raw = f"{name}:{view_id}"
            return f"u{quote(raw, safe='')}"

    return None


def get_item_name(item: Dict) -> str:
    for key in ("DisplayName", "Name", "Title"):
        value = item.get(key)
        if value:
            return str(value)

    if isinstance(item.get("View"), dict):
        return item["View"].get("Name") or f"View {item['View'].get('ID', '')}".strip()

    if isinstance(item.get("TraditionalFolder"), dict):
        return item["TraditionalFolder"].get("DisplayValue") or "Folder"

    if isinstance(item.get("PropertyFolder"), dict):
        return item["PropertyFolder"].get("DisplayValue") or "Property Folder"

    ov = item.get("ObjectVersion") or {}
    return ov.get("Title") or "(Unnamed)"


def parse_view_items(data: Dict, current_path: str) -> Tuple[List[Dict], List[Dict]]:
    items = data.get("Items") if isinstance(data, dict) else []
    if not isinstance(items, list):
        items = []

    folders: List[Dict] = []
    objects: List[Dict] = []

    for item in items:
        if not isinstance(item, dict):
            continue

        obj_version = item.get("ObjectVersion")
        if isinstance(obj_version, dict) and obj_version.get("ObjVer"):
            obj_ver = obj_version.get("ObjVer", {})
            obj_type = obj_ver.get("Type")
            obj_id = obj_ver.get("ID")
            version = obj_ver.get("Version") or obj_version.get("Version") or "latest"
            if obj_type is None or obj_id is None:
                continue
            title = obj_version.get("Title") or get_item_name(item)
            objects.append(
                {
                    "type": int(obj_type),
                    "id": int(obj_id),
                    "version": str(version),
                    "title": title,
                    "key": f"{int(obj_type)}:{int(obj_id)}:{version}:{title}",
                }
            )
            continue

        token = folder_token_from_item(item)
        if token:
            next_path = f"{current_path.strip('/')}/{token}".strip("/")
            folders.append(
                {
                    "name": get_item_name(item),
                    "token": token,
                    "path": next_path,
                }
            )

    folders.sort(key=lambda x: x["name"].lower())
    objects.sort(key=lambda x: x["title"].lower())
    return folders, objects


def create_client_from_session() -> Optional[MFilesClient]:
    base_url = session.get("base_url")
    token = session.get("token")
    if not base_url or not token:
        return None
    return MFilesClient(base_url=base_url, token=token)


def safe_zip_filename(name: str) -> str:
    cleaned = re.sub(r"[\\/:*?\"<>|]", "_", str(name)).strip().rstrip(". ")
    if not cleaned:
        cleaned = "download"
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
    if cleaned.upper() in reserved:
        cleaned = f"{cleaned}_"
    return cleaned[:120]


def get_original_mfiles_filename(file_row: Dict) -> str:
    # Prefer the original MFWS name fields before reconstructed fallbacks.
    name = file_row.get("Name") or file_row.get("EscapedName") or file_row.get("Title")
    if not name:
        file_id = file_row.get("ID")
        name = f"file_{file_id}" if file_id is not None else "file"
    ext = file_row.get("Extension")
    if ext and not str(name).lower().endswith(f".{ext}".lower()):
        name = f"{name}.{ext}"
    return str(name)


def safe_zip_file_component(name: str) -> str:
    # Keep original filename as much as possible; only strip unsafe path separators/control chars.
    cleaned = (
        str(name)
        .replace("\\", "_")
        .replace("/", "_")
        .replace("\x00", "")
        .strip()
        .rstrip(". ")
    )
    if not cleaned:
        cleaned = "file"
    base, ext = os.path.splitext(cleaned)
    reserved = {
        "CON",
        "PRN",
        "AUX",
        "NUL",
        "COM1",
        "COM2",
        "COM3",
        "COM4",
        "COM5",
        "COM6",
        "COM7",
        "COM8",
        "COM9",
        "LPT1",
        "LPT2",
        "LPT3",
        "LPT4",
        "LPT5",
        "LPT6",
        "LPT7",
        "LPT8",
        "LPT9",
    }
    if base.upper() in reserved:
        base = f"{base}_"
    max_total = 180
    if len(base + ext) > max_total:
        keep = max(20, max_total - len(ext))
        base = base[:keep]
    return (base + ext).strip() or "file"


def append_object_files_to_zip(
    zf: zipfile.ZipFile,
    client: MFilesClient,
    obj_type: int,
    obj_id: int,
    version: str,
    title: str,
    include_related: bool = True,
    target_folder_name: Optional[str] = None,
) -> int:
    try:
        files = client.get_object_files(obj_type, obj_id, version)
    except Exception:
        return 0

    written = 0
    for f in files:
        file_id = f.get("ID")
        if file_id is None:
            continue

        file_name = get_original_mfiles_filename(f)

        try:
            content = client.download_file_bytes(obj_type, obj_id, version, int(file_id))
        except Exception:
            continue

        obj_prefix = safe_zip_filename(target_folder_name or title or f"{obj_type}_{obj_id}")
        file_part = safe_zip_file_component(file_name)
        zip_path = f"{obj_prefix}/{file_part}"
        zf.writestr(zip_path, content)
        written += 1

    # Many custom object types have no direct files, but related document objects do.
    if written == 0 and include_related:
        try:
            related_objects = client.get_related_objects(obj_type, obj_id, version)
        except Exception:
            related_objects = []

        for related in related_objects:
            if not isinstance(related, dict):
                continue
            obj_ver = related.get("ObjVer", {})
            rel_type = obj_ver.get("Type")
            rel_id = obj_ver.get("ID")
            rel_version = obj_ver.get("Version") or "latest"
            if rel_type is None or rel_id is None:
                continue
            rel_title = related.get("Title") or f"{rel_type}_{rel_id}"
            written += append_object_files_to_zip(
                zf=zf,
                client=client,
                obj_type=int(rel_type),
                obj_id=int(rel_id),
                version=str(rel_version),
                title=str(rel_title),
                include_related=False,
                target_folder_name=title,
            )

    return written


def add_zip_info_file(zf: zipfile.ZipFile, message: str) -> None:
    zf.writestr("README_NO_FILES.txt", message.strip() + "\n")


def collect_objects_recursive(
    client: MFilesClient,
    path: str,
    visited: Optional[set] = None,
    max_depth: int = 12,
    depth: int = 0,
) -> List[Dict]:
    if visited is None:
        visited = set()

    norm = path.strip("/")
    if norm in visited or depth > max_depth:
        return []
    visited.add(norm)

    data = client.get_view_contents(norm)
    folders, objects = parse_view_items(data, norm)
    all_objects = list(objects)

    for folder in folders:
        all_objects.extend(
            collect_objects_recursive(
                client=client,
                path=folder["path"],
                visited=visited,
                max_depth=max_depth,
                depth=depth + 1,
            )
        )

    return all_objects


def get_object_download_entries(client: MFilesClient, obj: Dict) -> List[Dict]:
    title = obj.get("title") or f"{obj['type']}_{obj['id']}"
    obj_type = int(obj["type"])
    obj_id = int(obj["id"])
    version = str(obj.get("version") or "latest")
    entries: List[Dict] = []

    direct_files = client.get_object_files(obj_type, obj_id, version)
    for f in direct_files:
        file_id = f.get("ID")
        if file_id is None:
            continue
        entries.append(
            {
                "target_folder": title,
                "source_type": obj_type,
                "source_id": obj_id,
                "source_version": version,
                "file_id": int(file_id),
                "filename": get_original_mfiles_filename(f),
            }
        )

    if entries:
        return entries

    # If object has no direct files, try related objects and pull their files into the same target folder.
    related = client.get_related_objects(obj_type, obj_id, version)
    for rel in related:
        if not isinstance(rel, dict):
            continue
        obj_ver = rel.get("ObjVer", {})
        rel_type = obj_ver.get("Type")
        rel_id = obj_ver.get("ID")
        rel_version = obj_ver.get("Version") or "latest"
        if rel_type is None or rel_id is None:
            continue
        rel_files = client.get_object_files(int(rel_type), int(rel_id), str(rel_version))
        for f in rel_files:
            file_id = f.get("ID")
            if file_id is None:
                continue
            entries.append(
                {
                    "target_folder": title,
                    "source_type": int(rel_type),
                    "source_id": int(rel_id),
                    "source_version": str(rel_version),
                    "file_id": int(file_id),
                    "filename": get_original_mfiles_filename(f),
                }
            )

    return entries


def get_object_download_entries_with_size(client: MFilesClient, obj: Dict) -> List[Dict]:
    title = obj.get("title") or f"{obj['type']}_{obj['id']}"
    obj_type = int(obj["type"])
    obj_id = int(obj["id"])
    version = str(obj.get("version") or "latest")
    entries: List[Dict] = []

    direct_files = client.get_object_files(obj_type, obj_id, version)
    for f in direct_files:
        file_id = f.get("ID")
        if file_id is None:
            continue
        entries.append(
            {
                "target_folder": title,
                "source_type": obj_type,
                "source_id": obj_id,
                "source_version": version,
                "file_id": int(file_id),
                "filename": get_original_mfiles_filename(f),
                "size": int(f.get("Size") or 0),
            }
        )

    if entries:
        return entries

    related = client.get_related_objects(obj_type, obj_id, version)
    for rel in related:
        if not isinstance(rel, dict):
            continue
        obj_ver = rel.get("ObjVer", {})
        rel_type = obj_ver.get("Type")
        rel_id = obj_ver.get("ID")
        rel_version = obj_ver.get("Version") or "latest"
        if rel_type is None or rel_id is None:
            continue
        rel_files = client.get_object_files(int(rel_type), int(rel_id), str(rel_version))
        for f in rel_files:
            file_id = f.get("ID")
            if file_id is None:
                continue
            entries.append(
                {
                    "target_folder": title,
                    "source_type": int(rel_type),
                    "source_id": int(rel_id),
                    "source_version": str(rel_version),
                    "file_id": int(file_id),
                    "filename": get_original_mfiles_filename(f),
                    "size": int(f.get("Size") or 0),
                }
            )

    return entries


def unique_file_path(directory: str, filename: str) -> str:
    base, ext = os.path.splitext(filename)
    candidate = filename
    idx = 1
    while os.path.exists(os.path.join(directory, candidate)):
        candidate = f"{base} ({idx}){ext}"
        idx += 1
    return os.path.join(directory, candidate)


def append_job_error(job: Dict, message: str) -> None:
    with DOWNLOAD_JOBS_LOCK:
        job["errors"].append(message)
        log_path = job.get("log_path")
        log_lock = job.get("log_lock")
    if log_path:
        try:
            if log_lock:
                with log_lock:
                    with open(log_path, "a", encoding="utf-8") as fp:
                        fp.write(f"{datetime.now().isoformat(timespec='seconds')} | {message}\n")
            else:
                with open(log_path, "a", encoding="utf-8") as fp:
                    fp.write(f"{datetime.now().isoformat(timespec='seconds')} | {message}\n")
        except Exception:
            pass


def open_unique_file_for_write(directory: str, filename: str):
    base, ext = os.path.splitext(filename)
    candidate = filename
    idx = 1
    binary_flag = getattr(os, "O_BINARY", 0)
    while True:
        path = os.path.join(directory, candidate)
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | binary_flag)
            return os.fdopen(fd, "wb"), path
        except FileExistsError:
            candidate = f"{base} ({idx}){ext}"
            idx += 1


def write_job_summary(job: Dict) -> None:
    log_path = job.get("log_path")
    if not log_path:
        return
    try:
        with open(log_path, "a", encoding="utf-8") as fp:
            fp.write("\n===== SUMMARY =====\n")
            fp.write(f"status={job.get('status')}\n")
            fp.write(f"phase={job.get('phase')}\n")
            fp.write(f"path={job.get('path','')}\n")
            fp.write(f"chunk={job.get('chunk_start','')}-{job.get('chunk_end','')}\n")
            fp.write(f"worker_count={job.get('worker_count')}\n")
            fp.write(f"object_rate_limit={job.get('object_rate_limit')}\n")
            fp.write(f"total_objects={job.get('total_objects')}\n")
            fp.write(f"processed_objects={job.get('processed_objects')}\n")
            fp.write(f"total_files_discovered={job.get('total_files')}\n")
            fp.write(f"downloaded_files={job.get('downloaded_files')}\n")
            fp.write(f"failed_files={job.get('failed_files')}\n")
            fp.write(f"message={job.get('message','')}\n")
    except Exception:
        pass


def snapshot_job(job: Dict) -> Dict:
    return {
        "id": job["id"],
        "status": job["status"],
        "phase": job["phase"],
        "created_at": job["created_at"],
        "path": job.get("path", ""),
        "output_dir": job["output_dir"],
        "total_objects": job["total_objects"],
        "processed_objects": job["processed_objects"],
        "total_files": job["total_files"],
        "downloaded_files": job["downloaded_files"],
        "failed_files": job["failed_files"],
        "current_item": job.get("current_item", ""),
        "message": job.get("message", ""),
        "errors": job.get("errors", [])[:20],
        "log_path": job.get("log_path", ""),
        "worker_count": job.get("worker_count", 1),
        "object_rate_limit": job.get("object_rate_limit"),
        "chunk_start": job.get("chunk_start"),
        "chunk_end": job.get("chunk_end"),
        "source_total_objects": job.get("source_total_objects"),
    }


def run_direct_download_job(job_id: str) -> None:
    with DOWNLOAD_JOBS_LOCK:
        job = DOWNLOAD_JOBS.get(job_id)
    if not job:
        return

    os.makedirs(job["output_dir"], exist_ok=True)
    # Initialize log header.
    if job.get("log_path"):
        try:
            with open(job["log_path"], "w", encoding="utf-8") as fp:
                fp.write("M-Files Direct Download Error Log\n")
                fp.write(f"job_id={job['id']}\n")
                fp.write(f"created_at={job['created_at']}\n")
                fp.write(f"path={job.get('path','')}\n")
                fp.write(f"chunk={job.get('chunk_start','')}-{job.get('chunk_end','')}\n\n")
        except Exception:
            pass

    try:
        with DOWNLOAD_JOBS_LOCK:
            job["phase"] = "downloading"
            job["status"] = "running"
            job["message"] = f"Downloading with {job['worker_count']} file workers..."

        tasks: "queue.Queue[Optional[Dict]]" = queue.Queue(maxsize=max(1000, job["worker_count"] * 200))
        workers: List[threading.Thread] = []

        def wait_pause_or_cancel() -> bool:
            while True:
                with DOWNLOAD_JOBS_LOCK:
                    paused = job["paused"]
                    canceled = job["canceled"]
                    if paused:
                        job["status"] = "paused"
                        job["message"] = "Paused"
                    elif job["status"] != "running":
                        job["status"] = "running"
                if canceled:
                    return True
                if not paused:
                    return False
                time.sleep(0.3)

        def worker_loop(worker_idx: int) -> None:
            worker_client = MFilesClient(base_url=job["base_url"], token=job["token"])
            while True:
                task = tasks.get()
                try:
                    if task is None:
                        return

                    if wait_pause_or_cancel():
                        return

                    folder = safe_zip_filename(task["target_folder"])
                    folder_path = os.path.join(job["output_dir"], folder)
                    os.makedirs(folder_path, exist_ok=True)
                    filename = safe_zip_file_component(task["filename"])
                    with DOWNLOAD_JOBS_LOCK:
                        job["current_item"] = f"[W{worker_idx}] {folder}/{filename}"

                    content = worker_client.download_file_bytes(
                        obj_type=task["source_type"],
                        obj_id=task["source_id"],
                        version=task["source_version"],
                        file_id=task["file_id"],
                    )

                    fp, _ = open_unique_file_for_write(folder_path, filename)
                    with fp:
                        fp.write(content)

                    with DOWNLOAD_JOBS_LOCK:
                        job["downloaded_files"] += 1
                except Exception as ex:
                    with DOWNLOAD_JOBS_LOCK:
                        job["failed_files"] += 1
                        err_name = safe_zip_file_component((task or {}).get("filename", "file"))
                        err_folder = safe_zip_filename((task or {}).get("target_folder", "object"))
                    append_job_error(job, f"{err_folder}/{err_name}: {str(ex)}")
                finally:
                    tasks.task_done()

        for idx in range(job["worker_count"]):
            t = threading.Thread(target=worker_loop, args=(idx + 1,), daemon=True)
            workers.append(t)
            t.start()

        object_fetch_workers = max(1, min(8, job["worker_count"]))
        batch_size = max(object_fetch_workers * 3, 12)
        all_objects = list(job["objects"])
        object_rate_limit = job.get("object_rate_limit")
        submit_interval = (1.0 / object_rate_limit) if object_rate_limit and object_rate_limit > 0 else 0.0
        next_submit_at = time.monotonic()
        with DOWNLOAD_JOBS_LOCK:
            job["message"] = (
                f"Running {job['worker_count']} file workers + "
                f"{object_fetch_workers} object workers"
                + (f" (rate: {object_rate_limit}/sec)..." if object_rate_limit else "...")
            )

        def fetch_entries_for_object(obj_item: Dict):
            fetch_client = MFilesClient(base_url=job["base_url"], token=job["token"])
            return get_object_download_entries(fetch_client, obj_item)

        for start in range(0, len(all_objects), batch_size):
            if wait_pause_or_cancel():
                with DOWNLOAD_JOBS_LOCK:
                    job["status"] = "canceled"
                    job["phase"] = "done"
                    job["message"] = "Canceled by user."
                return

            batch = all_objects[start : start + batch_size]
            with ThreadPoolExecutor(max_workers=object_fetch_workers) as pool:
                futures = {}
                for obj in batch:
                    if wait_pause_or_cancel():
                        with DOWNLOAD_JOBS_LOCK:
                            job["status"] = "canceled"
                            job["phase"] = "done"
                            job["message"] = "Canceled by user."
                        return
                    if submit_interval > 0:
                        now = time.monotonic()
                        if now < next_submit_at:
                            time.sleep(next_submit_at - now)
                        next_submit_at = max(next_submit_at, time.monotonic()) + submit_interval
                    futures[pool.submit(fetch_entries_for_object, obj)] = obj
                for fut in as_completed(futures):
                    obj = futures[fut]
                    if wait_pause_or_cancel():
                        with DOWNLOAD_JOBS_LOCK:
                            job["status"] = "canceled"
                            job["phase"] = "done"
                            job["message"] = "Canceled by user."
                        return
                    try:
                        entries = fut.result()
                    except Exception as ex:
                        append_job_error(job, f"Object {obj.get('title', '')}: {str(ex)}")
                        with DOWNLOAD_JOBS_LOCK:
                            job["processed_objects"] += 1
                        continue

                    with DOWNLOAD_JOBS_LOCK:
                        job["total_files"] += len(entries)

                    for entry in entries:
                        if wait_pause_or_cancel():
                            with DOWNLOAD_JOBS_LOCK:
                                job["status"] = "canceled"
                                job["phase"] = "done"
                                job["message"] = "Canceled by user."
                            return
                        tasks.put(entry)

                    with DOWNLOAD_JOBS_LOCK:
                        job["processed_objects"] += 1

        # Drain queue and stop workers.
        tasks.join()
        for _ in workers:
            tasks.put(None)
        for w in workers:
            w.join(timeout=5)

        with DOWNLOAD_JOBS_LOCK:
            if job["canceled"]:
                job["status"] = "canceled"
                job["message"] = "Canceled by user."
            else:
                job["status"] = "completed"
                job["message"] = "Direct download completed."
            job["phase"] = "done"
            job["current_item"] = ""
    except Exception as ex:
        with DOWNLOAD_JOBS_LOCK:
            job["status"] = "failed"
            job["phase"] = "done"
            job["message"] = f"Failed: {str(ex)}"
        append_job_error(job, f"FATAL: {str(ex)}")
    finally:
        with DOWNLOAD_JOBS_LOCK:
            snapshot = dict(job)
        write_job_summary(snapshot)


def create_direct_download_job(
    base_url: str,
    token: str,
    objects: List[Dict],
    path: str = "",
    worker_count: int = 6,
    object_rate_limit: Optional[float] = None,
    chunk_start: Optional[int] = None,
    chunk_end: Optional[int] = None,
    source_total_objects: Optional[int] = None,
) -> str:
    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
    job_id = uuid.uuid4().hex[:12]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(DOWNLOAD_ROOT, f"direct_{timestamp}_{job_id}")
    log_path = os.path.join(output_dir, "error_log.txt")
    job = {
        "id": job_id,
        "status": "running",
        "phase": "starting",
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "base_url": base_url,
        "token": token,
        "objects": objects,
        "path": path,
        "output_dir": output_dir,
        "total_objects": len(objects),
        "processed_objects": 0,
        "total_files": 0,
        "downloaded_files": 0,
        "failed_files": 0,
        "current_item": "",
        "message": "Starting...",
        "errors": [],
        "log_path": log_path,
        "worker_count": max(1, min(16, int(worker_count))),
        "object_rate_limit": float(object_rate_limit) if object_rate_limit else None,
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "source_total_objects": source_total_objects if source_total_objects is not None else len(objects),
        "paused": False,
        "canceled": False,
        "log_lock": threading.Lock(),
    }
    with DOWNLOAD_JOBS_LOCK:
        DOWNLOAD_JOBS[job_id] = job

    thread = threading.Thread(target=run_direct_download_job, args=(job_id,), daemon=True)
    thread.start()
    return job_id


# -------- Flask routes --------

DEFAULTS = extract_defaults_from_txt(os.environ.get("MFWS_DEFAULTS_FILE", ""))


@app.route("/", methods=["GET"])
def index():
    if "token" not in session:
        return render_template("index.html", mode="login", defaults=DEFAULTS)

    path = request.args.get("path", "").strip("/")
    try:
        page = int(request.args.get("page", "1"))
    except ValueError:
        page = 1
    try:
        page_size = int(request.args.get("page_size", "100"))
    except ValueError:
        page_size = 100
    page = max(1, page)
    page_size = max(20, min(1000, page_size))
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("logout"))

    try:
        data = client.get_view_contents(path)
        folders, objects = parse_view_items(data, path)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("logout"))

    total_objects = len(objects)
    total_pages = max(1, (total_objects + page_size - 1) // page_size)
    if page > total_pages:
        page = total_pages
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    objects_page = objects[start_idx:end_idx]

    breadcrumbs = []
    parts = [p for p in path.split("/") if p]
    accum = []
    for p in parts:
        accum.append(p)
        breadcrumbs.append({"label": p, "path": "/".join(accum)})

    active_job = None
    active_job_id = session.get("active_job_id")
    if active_job_id:
        with DOWNLOAD_JOBS_LOCK:
            job = DOWNLOAD_JOBS.get(active_job_id)
            if job:
                active_job = snapshot_job(job)

    return render_template(
        "index.html",
        mode="browser",
        path=path,
        folders=folders,
        objects=objects_page,
        breadcrumbs=breadcrumbs,
        base_url=session.get("base_url"),
        username=session.get("username"),
        active_job=active_job,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        total_objects=total_objects,
    )


@app.route("/login", methods=["POST"])
def login():
    base_url = request.form.get("base_url", "").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    vault_guid = request.form.get("vault_guid", "").strip()

    client = MFilesClient(base_url=base_url)
    try:
        token = client.authenticate(username, password, vault_guid)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("index"))

    session["token"] = token
    session["base_url"] = client.base_url
    session["username"] = username
    return redirect(url_for("index"))


@app.route("/logout", methods=["GET"])
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/download/object", methods=["GET"])
def download_object():
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("index"))

    path = request.args.get("path", "").strip("/")
    obj_type = int(request.args.get("type"))
    obj_id = int(request.args.get("id"))
    version = request.args.get("version", "latest")
    title = request.args.get("title", f"{obj_type}_{obj_id}")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        count = append_object_files_to_zip(zf, client, obj_type, obj_id, version, title)

    if count == 0:
        flash("No files found for selected object. This item is likely metadata-only.", "error")
        return redirect(url_for("index", path=path))

    buf.seek(0)
    filename = f"{safe_zip_filename(title)}.zip"
    return send_file(buf, mimetype="application/zip", as_attachment=True, download_name=filename)


@app.route("/download/file", methods=["GET"])
def download_file():
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("index"))

    path = request.args.get("path", "").strip("/")
    obj_type = int(request.args.get("type"))
    obj_id = int(request.args.get("id"))
    version = request.args.get("version", "latest")
    file_id = int(request.args.get("file_id"))
    filename = request.args.get("filename", f"file_{file_id}")

    try:
        content = client.download_file_bytes(obj_type, obj_id, version, file_id)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("index", path=path))

    return send_file(
        io.BytesIO(content),
        mimetype="application/octet-stream",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/download/selected", methods=["POST"])
def download_selected():
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("index"))

    keys = request.form.getlist("object_keys")
    path = request.form.get("path", "")
    if not keys:
        flash("Select at least one object.", "error")
        return redirect(url_for("index", path=path))

    buf = io.BytesIO()
    total_files = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for key in keys:
            try:
                obj_type, obj_id, version, title = key.split(":", 3)
                total_files += append_object_files_to_zip(
                    zf,
                    client,
                    int(obj_type),
                    int(obj_id),
                    version,
                    title,
                )
            except Exception:
                continue

        if total_files == 0:
            add_zip_info_file(
                zf,
                "No downloadable files were found in the selected objects. "
                "Objects/folders without files were skipped.",
            )

    buf.seek(0)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"selected_objects_{stamp}.zip",
    )


@app.route("/download/folder", methods=["POST"])
def download_folder():
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("index"))

    path = request.form.get("path", "").strip("/")

    try:
        objects = collect_objects_recursive(client, path)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("index", path=path))

    buf = io.BytesIO()
    total_files = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for obj in objects:
            total_files += append_object_files_to_zip(
                zf,
                client,
                obj["type"],
                obj["id"],
                obj["version"],
                obj["title"],
            )

        if total_files == 0:
            add_zip_info_file(
                zf,
                "No downloadable files were found in this folder/subfolders. "
                "Empty folders and metadata-only objects were skipped.",
            )

    buf.seek(0)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder_name = safe_zip_filename(path.replace("/", "_")) if path else "root"
    return send_file(
        buf,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"{folder_name}_all_{stamp}.zip",
    )


@app.route("/direct-download/start-folder", methods=["POST"])
def direct_download_start_folder():
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("index"))

    path = request.form.get("path", "").strip("/")
    workers_raw = (request.form.get("worker_count") or "").strip()
    rate_raw = (request.form.get("object_rate_limit") or "").strip()
    start_raw = (request.form.get("start_index") or "").strip()
    end_raw = (request.form.get("end_index") or "").strip()
    try:
        worker_count = int(workers_raw) if workers_raw else 6
    except ValueError:
        worker_count = 6
    worker_count = max(1, min(16, worker_count))
    object_rate_limit = None
    if rate_raw:
        try:
            object_rate_limit = float(rate_raw)
            if object_rate_limit <= 0:
                object_rate_limit = None
        except ValueError:
            object_rate_limit = None
    try:
        objects = collect_objects_recursive(client, path)
    except Exception as ex:
        flash(str(ex), "error")
        return redirect(url_for("index", path=path))

    if not objects:
        flash("No objects found in this folder/view.", "error")
        return redirect(url_for("index", path=path))

    source_total = len(objects)
    chunk_start = None
    chunk_end = None
    if start_raw or end_raw:
        try:
            chunk_start = int(start_raw) if start_raw else 1
            chunk_end = int(end_raw) if end_raw else source_total
        except ValueError:
            flash("Chunk range must be numeric (example: 1 to 5000).", "error")
            return redirect(url_for("index", path=path))

        if chunk_start < 1:
            chunk_start = 1
        if chunk_end > source_total:
            chunk_end = source_total
        if chunk_start > chunk_end:
            flash("Chunk range is invalid. Start must be <= End.", "error")
            return redirect(url_for("index", path=path))

        objects = objects[chunk_start - 1 : chunk_end]
        if not objects:
            flash("No objects found in the selected chunk range.", "error")
            return redirect(url_for("index", path=path))

    job_id = create_direct_download_job(
        base_url=session["base_url"],
        token=session["token"],
        objects=objects,
        path=path,
        worker_count=worker_count,
        object_rate_limit=object_rate_limit,
        chunk_start=chunk_start,
        chunk_end=chunk_end,
        source_total_objects=source_total,
    )
    session["active_job_id"] = job_id
    if chunk_start and chunk_end:
        flash(
            f"Direct chunk download started for objects {chunk_start}..{chunk_end} (of {source_total}) with {worker_count} workers.",
            "info",
        )
    else:
        flash(f"Direct download started with {worker_count} workers.", "info")
    return redirect(url_for("index", path=path))


@app.route("/direct-download/estimate-folder", methods=["POST"])
def direct_download_estimate_folder():
    client = create_client_from_session()
    if client is None:
        return {"error": "Not authenticated"}, 401

    path = (request.form.get("path") or "").strip("/")
    start_raw = (request.form.get("start_index") or "").strip()
    end_raw = (request.form.get("end_index") or "").strip()
    try:
        objects = collect_objects_recursive(client, path)
    except Exception as ex:
        return {"error": str(ex)}, 500

    if not objects:
        return {"object_count": 0, "file_count": 0, "total_bytes": 0, "range_start": None, "range_end": None}

    source_total = len(objects)
    range_start = 1
    range_end = source_total
    if start_raw or end_raw:
        try:
            range_start = int(start_raw) if start_raw else 1
            range_end = int(end_raw) if end_raw else source_total
        except ValueError:
            return {"error": "Chunk range must be numeric."}, 400
        range_start = max(1, range_start)
        range_end = min(source_total, range_end)
        if range_start > range_end:
            return {"error": "Chunk range is invalid. Start must be <= End."}, 400

    selected = objects[range_start - 1 : range_end]
    total_bytes = 0
    total_files = 0
    scanned_objects = 0
    for obj in selected:
        try:
            entries = get_object_download_entries_with_size(client, obj)
            total_files += len(entries)
            total_bytes += sum(int(e.get("size") or 0) for e in entries)
        except Exception:
            pass
        scanned_objects += 1

    return {
        "source_total_objects": source_total,
        "object_count": len(selected),
        "file_count": total_files,
        "total_bytes": total_bytes,
        "range_start": range_start,
        "range_end": range_end,
        "scanned_objects": scanned_objects,
    }


@app.route("/direct-download/start-selected", methods=["POST"])
def direct_download_start_selected():
    client = create_client_from_session()
    if client is None:
        return redirect(url_for("index"))

    path = request.form.get("path", "").strip("/")
    workers_raw = (request.form.get("worker_count") or "").strip()
    rate_raw = (request.form.get("object_rate_limit") or "").strip()
    try:
        worker_count = int(workers_raw) if workers_raw else 6
    except ValueError:
        worker_count = 6
    worker_count = max(1, min(16, worker_count))
    object_rate_limit = None
    if rate_raw:
        try:
            object_rate_limit = float(rate_raw)
            if object_rate_limit <= 0:
                object_rate_limit = None
        except ValueError:
            object_rate_limit = None
    keys = request.form.getlist("object_keys")
    if not keys:
        flash("Select at least one object for direct download.", "error")
        return redirect(url_for("index", path=path))

    objects: List[Dict] = []
    for key in keys:
        try:
            obj_type, obj_id, version, title = key.split(":", 3)
            objects.append(
                {
                    "type": int(obj_type),
                    "id": int(obj_id),
                    "version": str(version),
                    "title": title,
                }
            )
        except Exception:
            continue

    if not objects:
        flash("Selected object keys were invalid.", "error")
        return redirect(url_for("index", path=path))

    job_id = create_direct_download_job(
        base_url=session["base_url"],
        token=session["token"],
        objects=objects,
        path=path,
        worker_count=worker_count,
        object_rate_limit=object_rate_limit,
    )
    session["active_job_id"] = job_id
    flash(f"Direct selected-download started with {worker_count} workers.", "info")
    return redirect(url_for("index", path=path))


@app.route("/direct-download/job/<job_id>", methods=["GET"])
def direct_download_job_status(job_id: str):
    with DOWNLOAD_JOBS_LOCK:
        job = DOWNLOAD_JOBS.get(job_id)
        if not job:
            return {"error": "Job not found"}, 404
        return snapshot_job(job)


@app.route("/direct-download/job/<job_id>/pause", methods=["POST"])
def direct_download_pause(job_id: str):
    with DOWNLOAD_JOBS_LOCK:
        job = DOWNLOAD_JOBS.get(job_id)
        if not job:
            return {"error": "Job not found"}, 404
        if job["status"] in ("completed", "failed", "canceled"):
            return {"ok": False, "status": job["status"]}, 400
        job["paused"] = True
        job["status"] = "paused"
        job["message"] = "Paused"
        return {"ok": True, "status": job["status"]}


@app.route("/direct-download/job/<job_id>/resume", methods=["POST"])
def direct_download_resume(job_id: str):
    with DOWNLOAD_JOBS_LOCK:
        job = DOWNLOAD_JOBS.get(job_id)
        if not job:
            return {"error": "Job not found"}, 404
        if job["status"] in ("completed", "failed", "canceled"):
            return {"ok": False, "status": job["status"]}, 400
        job["paused"] = False
        job["status"] = "running"
        job["message"] = "Running"
        return {"ok": True, "status": job["status"]}


@app.route("/object/files", methods=["GET"])
def object_files_json():
    client = create_client_from_session()
    if client is None:
        return {"error": "Not authenticated"}, 401

    obj_type = int(request.args.get("type"))
    obj_id = int(request.args.get("id"))
    version = request.args.get("version", "latest")

    try:
        files = client.get_object_files(obj_type, obj_id, version)
    except Exception as ex:
        return {"error": str(ex)}, 500

    results = []

    def normalize_file_records(file_rows: List[Dict], src_type: int, src_id: int, src_version: str, source_title: str = ""):
        for f in file_rows:
            file_id = f.get("ID")
            if file_id is None:
                continue
            filename = get_original_mfiles_filename(f)
            results.append(
                {
                    "id": file_id,
                    "filename": filename,
                    "type": int(src_type),
                    "obj_id": int(src_id),
                    "version": str(src_version),
                    "source_title": source_title,
                }
            )

    normalize_file_records(files, obj_type, obj_id, version, "")

    if not results:
        try:
            related = client.get_related_objects(obj_type, obj_id, version)
        except Exception:
            related = []

        for rel in related:
            if not isinstance(rel, dict):
                continue
            rel_obj = rel.get("ObjVer", {})
            rel_type = rel_obj.get("Type")
            rel_id = rel_obj.get("ID")
            rel_version = rel_obj.get("Version") or "latest"
            rel_title = rel.get("Title") or ""
            if rel_type is None or rel_id is None:
                continue
            try:
                rel_files = client.get_object_files(int(rel_type), int(rel_id), str(rel_version))
            except Exception:
                continue
            normalize_file_records(rel_files, int(rel_type), int(rel_id), str(rel_version), rel_title)

    return {"files": results}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
