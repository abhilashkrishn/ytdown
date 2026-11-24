let currentUrl = "";
let extractedTitle = "";
let eventSource = null;
let currentFileId = null;
let pendingRetryToken = null;

async function analyzeVideo() {
    const urlInput = document.getElementById('videoUrl');
    const analyzeBtn = document.getElementById('analyzeBtn');
    const formatDiv = document.getElementById('formatSelection');
    const displayTitleDiv = document.getElementById('displayTitle');
    const selectDropdown = document.getElementById('resolutionSelect');
    const statusDiv = document.getElementById('statusText');
    const progressContainer = document.getElementById('progressContainer');
    const retryBtn = document.getElementById('retryBtn');

    if (!urlInput.value.trim()) return alert('Please enter a URL');

    currentUrl = urlInput.value.trim();
    pendingRetryToken = null;
    if (retryBtn) retryBtn.style.display = 'none';

    analyzeBtn.disabled = true;
    formatDiv.style.display = 'none';
    progressContainer.style.display = 'none';
    statusDiv.style.display = 'block';
    statusDiv.style.color = 'var(--text-muted)';
    statusDiv.innerText = 'Extracting video container schemas...';

    try {
        const response = await fetch(`api/options?url=${encodeURIComponent(currentUrl)}`);

        // Handle session expiration
        if (response.status === 401) {
            window.location.reload();
            return;
        }

        const data = await response.json();

        if (response.ok) {
            extractedTitle = data.title;
            displayTitleDiv.innerText = data.title;

            selectDropdown.innerHTML = '';
            data.formats.forEach(f => {
                // Skip this option if it's missing data or returned 'undefined'
                if (!f.resolution || !f.ext || String(f.resolution) === 'undefined' || String(f.ext) === 'undefined') {
                    return;
                }

                const opt = document.createElement('option');
                opt.value = f.format_id;
                opt.innerText = `${f.resolution} (${f.ext}) — ${f.note || ''}`;
                selectDropdown.appendChild(opt);
            });

            statusDiv.style.display = 'none';
            formatDiv.style.display = 'block';
        } else {
            statusDiv.style.color = 'var(--error)';
            if (data.error_code === 'cookie_required') {
                statusDiv.innerText = 'YouTube authentication required. Cookies need refreshing: ' + (data.error || data.detail);
            } else {
                statusDiv.innerText = 'Error: ' + data.detail;
            }
        }
    } catch (err) {
        statusDiv.style.color = 'var(--error)';
        statusDiv.innerText = 'Failed to interface local backend daemon.';
    } finally {
        analyzeBtn.disabled = false;
    }
}

function retryDownload() {
    if (!pendingRetryToken) return;
    const token = pendingRetryToken;
    pendingRetryToken = null;
    const retryBtn = document.getElementById('retryBtn');
    if (retryBtn) retryBtn.style.display = 'none';
    startDownload(token);
}

function startDownload(retryToken = null) {
    const selectDropdown = document.getElementById('resolutionSelect');
    const progressContainer = document.getElementById('progressContainer');
    const statusDiv = document.getElementById('statusText');
    const downloadBtn = document.getElementById('downloadBtn');
    const analyzeBtn = document.getElementById('analyzeBtn');
    const cancelBtn = document.getElementById('cancelBtn');
    const retryBtn = document.getElementById('retryBtn');

    const selectedFormat = selectDropdown.value;

    currentFileId = null;
    if (!retryToken) {
        pendingRetryToken = null;
    }
    if (retryBtn) retryBtn.style.display = 'none';

    downloadBtn.disabled = true;
    analyzeBtn.disabled = true;
    if (cancelBtn) cancelBtn.style.display = 'inline-block';

    progressContainer.style.display = 'block';
    statusDiv.style.display = 'block';
    statusDiv.style.color = 'var(--text-muted)';

    if (retryToken) {
        statusDiv.innerText = 'Refreshing YouTube session & cookies...';
    } else {
        statusDiv.innerText = 'Initializing parallel download worker engine...';
    }

    let sseUrl = `api/download-progress?url=${encodeURIComponent(currentUrl)}&format_id=${encodeURIComponent(selectedFormat)}`;
    if (retryToken) {
        sseUrl += `&retry_token=${encodeURIComponent(retryToken)}`;
    }

    if (eventSource) eventSource.close();
    eventSource = new EventSource(sseUrl);

    eventSource.onmessage = function(event) {
        if (!event.data || event.data.trim() === "") return;

        try {
            const data = JSON.parse(event.data);

            if (data.status === 'processing') {
                if (data.file_id) currentFileId = data.file_id;

                if (data.speed && data.speed.toLowerCase().includes('refreshing')) {
                    statusDiv.innerText = data.speed;
                } else {
                    statusDiv.innerText = 'Fetching payload file chunks from server...';
                }
                document.getElementById('metricPercent').innerText = data.percent + '%';
                document.getElementById('metricSize').innerText = data.total_size;
                document.getElementById('metricSpeed').innerText = data.speed;
                document.getElementById('metricEta').innerText = data.eta;

                const progressBar = document.getElementById('progressBar');
                progressBar.style.width = data.percent + '%';
                progressBar.setAttribute('aria-valuenow', data.percent);
            }
            else if (data.status === 'completed') {
                eventSource.close();
                pendingRetryToken = null;
                statusDiv.style.color = 'var(--success)';
                statusDiv.innerText = 'Download finished on server! Pushing file pipeline to browser...';

                window.location.href = `api/get-file?file_id=${data.file_id}&title=${encodeURIComponent(extractedTitle)}`;

                downloadBtn.disabled = false;
                analyzeBtn.disabled = false;
                if (cancelBtn) cancelBtn.style.display = 'none';
                if (retryBtn) retryBtn.style.display = 'none';
            }
            else if (data.status === 'failed') {
                eventSource.close();
                statusDiv.style.color = 'var(--error)';

                if (data.error_code === 'cookie_required' && data.retry_token) {
                    pendingRetryToken = data.retry_token;
                    statusDiv.innerText = 'YouTube authentication required. Cookies need refreshing to download this video: ' + data.error;
                    if (retryBtn) retryBtn.style.display = 'inline-block';
                } else {
                    pendingRetryToken = null;
                    if (retryBtn) retryBtn.style.display = 'none';
                    if (data.error_code === 'cookie_refresh_failed') {
                        statusDiv.innerText = data.error;
                    } else {
                        statusDiv.innerText = 'Error: ' + data.error;
                    }
                }

                downloadBtn.disabled = false;
                analyzeBtn.disabled = false;
                if (cancelBtn) cancelBtn.style.display = 'none';
            }
        } catch (e) {
            console.log("Skipping non-JSON progress line:", event.data);
        }
    };

    eventSource.onerror = function(e) {
        eventSource.close();

        if (retryToken) {
            pendingRetryToken = null;
            statusDiv.style.color = 'var(--error)';
            statusDiv.innerText = 'The cookie retry token is invalid or has expired. Please re-analyze the link and try again.';
            downloadBtn.disabled = false;
            analyzeBtn.disabled = false;
            if (cancelBtn) cancelBtn.style.display = 'none';
            if (retryBtn) retryBtn.style.display = 'none';
            return;
        }

        // If it fails before initializing anything, reload to verify session.
        if (!currentFileId) {
            window.location.reload();
            return;
        }

        statusDiv.style.color = 'var(--error)';
        statusDiv.innerText = 'Server processing connection loop terminated unexpectedly.';
        downloadBtn.disabled = false;
        analyzeBtn.disabled = false;
        if (cancelBtn) cancelBtn.style.display = 'none';
        if (retryBtn) retryBtn.style.display = 'none';
    };
}

async function cancelDownload() {
    const cancelBtn = document.getElementById('cancelBtn');
    const retryBtn = document.getElementById('retryBtn');
    if (cancelBtn) cancelBtn.disabled = true;

    if (currentFileId) {
        try {
            const response = await fetch(`api/cancel?file_id=${currentFileId}`, { method: 'POST' });
            if (response.status === 401) {
                window.location.reload();
                return;
            }
        } catch (err) {
            console.error("Failed to send cancel request:", err);
        }
    }

    if (eventSource) {
        eventSource.close();
    }

    pendingRetryToken = null;

    const statusDiv = document.getElementById('statusText');
    const downloadBtn = document.getElementById('downloadBtn');
    const analyzeBtn = document.getElementById('analyzeBtn');

    statusDiv.style.color = 'var(--error)';
    statusDiv.innerText = 'Download cancelled by user.';

    downloadBtn.disabled = false;
    analyzeBtn.disabled = false;

    if (cancelBtn) {
        cancelBtn.style.display = 'none';
        cancelBtn.disabled = false;
    }
    if (retryBtn) {
        retryBtn.style.display = 'none';
    }

    currentFileId = null;
}

async function logout(e) {
    e.preventDefault();
    try {
        await fetch('api/logout', { method: 'POST' });
        // Force a reload back to login page regardless of outcome,
        // since the token is cleared client-side too if the backend succeeded.
        window.location.reload();
    } catch (err) {
        console.error("Logout failed", err);
        // Fallback reload
        window.location.reload();
    }
}
