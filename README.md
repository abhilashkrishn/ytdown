# StreamVault

StreamVault is a web-based video downloader built with **FastAPI** and
**yt-dlp**. It provides an authenticated browser interface for
retrieving media metadata, selecting an available format, monitoring
download progress, cancelling downloads, and retrieving the completed
file.

## Features

-   Username/password authentication.
-   Signed, HTTP-only session cookie with a 30-day session lifetime.
-   CSRF checks using the request `Origin` or `Referer`.
-   Video metadata and available-format discovery through yt-dlp.
-   Download progress streamed to the browser using Server-Sent Events
    (SSE).
-   Download cancellation.
-   Temporary download-file cleanup.
-   Limits on concurrent metadata extractions and downloads.
-   Initial URL validation that rejects hostnames resolving to
    non-public IP addresses.
-   Optional `cookies.txt` support for sources that require an
    authenticated browser session.

## Project structure

``` text
streamvault/
├── main.py
├── .env
├── cookies.txt              # Optional; do not commit
├── frontend/
│   ├── index.html
│   ├── login.html
│   ├── app.js
│   └── style.css
└── temp_downloads/          # Created automatically; runtime data
```

## Requirements

-   Python 3.10 or later
-   FastAPI
-   Uvicorn
-   yt-dlp

Install the Python packages:

``` bash
python -m pip install fastapi uvicorn yt-dlp
```

Keep dependencies pinned in `requirements.txt` for reproducible
deployments.

## Configuration

Create a `.env` file beside `main.py`:

``` env
AUTH_USERNAME=change_this_username
AUTH_PASSWORD=use_a_strong_unique_password
SESSION_SECRET=replace_with_at_least_32_random_characters
CANONICAL_ORIGIN=http://127.0.0.1:8300
ALLOWED_ORIGINS=http://127.0.0.1:8300
TRUST_REVERSE_PROXY=false
COOKIE_SECURE=false
```

  --------------------------------------------------------------------------
  Variable                                    Required Description
  ----------------------- ---------------------------- ---------------------
  `AUTH_USERNAME`                                  Yes Login username.

  `AUTH_PASSWORD`                                  Yes Login password.

  `SESSION_SECRET`                                 Yes Session-signing
                                                       secret; minimum 32
                                                       characters.

  `CANONICAL_ORIGIN`                               Yes Canonical scheme,
                                                       hostname and optional
                                                       port; no path.

  `ALLOWED_ORIGINS`                                 No Comma-separated
                                                       CSRF-allowed origins.
                                                       Defaults to
                                                       `CANONICAL_ORIGIN`.

  `TRUST_REVERSE_PROXY`                             No Enable only behind a
                                                       trusted reverse
                                                       proxy. Defaults to
                                                       `false`.

  `TRUSTED_PROXIES`                                 No Comma-separated
                                                       trusted proxy IPs.
                                                       Defaults to
                                                       `127.0.0.1,::1`.

  `COOKIE_SECURE`                                   No Set to `true` for
                                                       HTTPS. The
                                                       application default
                                                       is `true`.
  --------------------------------------------------------------------------

Generate a random session secret:

``` bash
python -c "import secrets; print(secrets.token_urlsafe(48))"
```

For local HTTP development, set `COOKIE_SECURE=false`. For HTTPS
deployment, set it to `true` and configure the canonical and allowed
origins to the public HTTPS origin.

**Never commit `.env`, `cookies.txt`, credentials, session secrets, or
other private configuration.**

## Run locally

From the project directory:

``` bash
python -m uvicorn main:app --host 127.0.0.1 --port 8300
```

Open `http://127.0.0.1:8300/`. The login page is served until a valid
session is established.

## Optional source cookies

If a supported source requires an authenticated browser session, place a
yt-dlp-compatible Netscape-format `cookies.txt` beside `main.py`. The
application uses it when present.

Treat this file as a credential. Keep it private and never commit it.

## Runtime limits

-   Maximum concurrent downloads: **3**.
-   Maximum concurrent metadata extractions: **5**.
-   Maximum individual download size configured in yt-dlp: **50 GB**.
-   Session lifetime: **30 days**.
-   Temporary files are stored in `temp_downloads/`.
-   Completed files are removed after retrieval; abandoned files are
    cleaned up by application maintenance.

These settings are defined in `main.py`.

## Reverse-proxy deployment

The application exposes routes from `/`. When hosted under a URL prefix
such as `https://example.com/ytdown/`, configure the reverse proxy to
strip `/ytdown/` before forwarding requests to the application root.

For HTTPS deployment:

-   Bind Uvicorn to `127.0.0.1` on the selected application port.
-   Set `CANONICAL_ORIGIN` and `ALLOWED_ORIGINS` to
    `https://example.com` (origin only; no path).
-   Set `COOKIE_SECURE=true`.
-   Enable `TRUST_REVERSE_PROXY` only when proxy addresses and forwarded
    headers are configured securely.
-   Do not expose the Uvicorn port directly to the public internet.

## Security notes

-   URL validation checks the initial DNS resolution and rejects
    non-public IP addresses.
-   This is **not complete SSRF protection**. DNS rebinding, redirects,
    and secondary requests made by yt-dlp may bypass the initial check.
    Production deployments should enforce network-level egress
    restrictions against private, loopback, link-local, and other
    internal network ranges.
-   Keep Python packages and yt-dlp updated.
-   Use a strong unique password and a randomly generated session
    secret.
-   Serve the application over HTTPS.
-   Never publish cookies, environment files, downloaded media, or
    runtime data.

## Git

Ensure `.gitignore` excludes at least:

``` gitignore
.env
.env.*
!.env.example
cookies.txt
__pycache__/
*.py[cod]
.venv/
venv/
temp_downloads/
downloads/
*.log
```

Review `git status` before every push to ensure no secrets or private
runtime files are staged.

## Disclaimer

StreamVault is a general-purpose interface to yt-dlp. Users are
responsible for complying with the terms of service of websites they
access and all applicable copyright laws and regulations. Download only
content you are authorized to access and save.
