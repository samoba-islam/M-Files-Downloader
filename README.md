# M-Files Downloader

A web-based browser and bulk downloader for [M-Files](https://www.m-files.com/) vaults, powered by the M-Files Web Service (MFWS) REST API.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-3.x-green?logo=flask&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-yellow)

---

## Features

- **Authenticate** against any M-Files Web Service using username, password, and vault GUID.
- **Browse** the full view/folder hierarchy of your vault in a clean web UI.
- **Download single files** directly from objects.
- **Download all files** in an object as a ZIP archive.
- **Bulk download** — select multiple objects and download as a single ZIP.
- **Recursive folder download** — grab every file in a view and its subfolders.
- **Direct (no-zip) download** — save thousands of files one-by-one to disk with multi-threaded workers (1–16 concurrent threads).
- **Chunked downloads** — specify an object range (e.g. 1–5000) for very large vaults.
- **Rate limiting** — control object-fetch throughput (objects/sec) to avoid overloading the server.
- **Pause / Resume / Cancel** running download jobs.
- **Pre-built size estimation** for a selected range before starting.

---

## Requirements

- Python **3.9+**
- pip

---

## Installation

```bash
# Clone the repository
git clone https://github.com/<your-username>/M-Files_downloader.git
cd M-Files_downloader

# Create and activate a virtual environment
python -m venv .venv

# Windows
.\.venv\Scripts\Activate.ps1

# macOS / Linux
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## Configuration

Copy the example environment file and edit it:

```bash
cp .env.example .env
```

| Variable | Default | Description |
|---|---|---|
| `FLASK_SECRET_KEY` | `change-me-in-production` | Flask session secret — **must** be changed in production. |
| `MFWS_VIEW_LIMIT` | `50000` | Max items returned by a single view request. |
| `MFWS_DEFAULTS_FILE` | *(empty)* | Optional path to a text file with default credentials (auto-fills the login form). |
| `DOWNLOAD_DIR` | `./downloads` | Directory where direct-download jobs save files. |

### Credentials file format (optional)

If you point `MFWS_DEFAULTS_FILE` to a text file, the app will parse these fields to pre-fill the login form:

```text
NetworkAddress: "your-server-hostname"
UserName: "your-username"
Password: "your-password"
LogInToVault("{vault-guid}")
```

---

## Usage

```bash
python app.py
```

Open **http://127.0.0.1:5000** in your browser.

1. Enter your M-Files REST Base URL, username, password, and vault GUID.
2. Browse views and folders.
3. Download files individually, per-object, in bulk, or recursively.

---

## API Endpoints

| Method | Route | Description |
|---|---|---|
| `GET` | `/` | Main page — login form or vault browser |
| `POST` | `/login` | Authenticate with M-Files |
| `GET` | `/logout` | Clear session and return to login |
| `GET` | `/download/object` | Download all files from one object as ZIP |
| `GET` | `/download/file` | Download a single file |
| `POST` | `/download/selected` | Download selected objects as ZIP |
| `POST` | `/download/folder` | Download all objects in a folder (recursive) as ZIP |
| `POST` | `/direct-download/start-folder` | Start a direct (no-zip) folder download job |
| `POST` | `/direct-download/start-selected` | Start a direct (no-zip) download for selected objects |
| `POST` | `/direct-download/estimate-folder` | Estimate file count & size for a range |
| `GET` | `/direct-download/job/<job_id>` | Get status of a download job (JSON) |
| `POST` | `/direct-download/job/<job_id>/pause` | Pause a running job |
| `POST` | `/direct-download/job/<job_id>/resume` | Resume a paused job |
| `GET` | `/object/files` | List files for an object (JSON) |

---

## Project Structure

```
M-Files_downloader/
├── app.py               # Flask application (routes + M-Files client)
├── templates/
│   └── index.html       # Jinja2 template (login + browser UI)
├── downloads/           # Default output directory for direct downloads
├── requirements.txt     # Python dependencies
├── .env.example         # Environment variable template
├── .gitignore
├── LICENSE              # MIT
├── CONTRIBUTING.md
├── SECURITY.md
└── README.md
```

---
## 👤 Author

**Shawon Hossain**

- GitHub: [@samoba-islam](https://github.com/samoba-islam)
- Website: [samoba.pages.dev](https://samoba.pages.dev)

---
## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Security

See [SECURITY.md](SECURITY.md) for responsible disclosure instructions.

## License

This project is licensed under the [MIT License](LICENSE).
