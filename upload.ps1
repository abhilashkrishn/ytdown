# ==========================================
# StreamVault - GitHub Upload
# ==========================================

$ErrorActionPreference = "Stop"

function Clean-FileWhitespace {
    param([string]$FilePath)

    if (-not (Test-Path -LiteralPath $FilePath -PathType Leaf)) {
        return
    }

    $fullPath = (Resolve-Path -LiteralPath $FilePath).Path
    $bytes = [System.IO.File]::ReadAllBytes($fullPath)

    # Detect UTF-8 BOM
    $hasBom = (
        $bytes.Length -ge 3 -and
        $bytes[0] -eq 0xEF -and
        $bytes[1] -eq 0xBB -and
        $bytes[2] -eq 0xBF
    )

    $encoding = [System.Text.UTF8Encoding]::new($hasBom)
    $content = [System.IO.File]::ReadAllText($fullPath, $encoding)

    # Preserve existing line-ending style
    if ($content.Contains("`r`n")) {
        $newline = "`r`n"
    }
    else {
        $newline = "`n"
    }

    # Split into lines
    $lines = [regex]::Split($content, "\r\n|\n|\r")

    # Remove unnecessary blank lines at EOF
    $last = $lines.Length - 1

    while ($last -ge 0 -and [string]::IsNullOrWhiteSpace($lines[$last])) {
        $last--
    }

    if ($last -lt 0) {
        $cleaned = ""
    }
    else {
        # Exactly one newline at EOF
        $cleaned = ($lines[0..$last] -join $newline) + $newline
    }

    if ($cleaned -cne $content) {
        [System.IO.File]::WriteAllText($fullPath, $cleaned, $encoding)
        Write-Host "Cleaned EOF: $FilePath" -ForegroundColor Green
    }
}

function Get-TextFiles {
    # Include tracked and non-ignored untracked repository files
    $files = @(git ls-files --cached --others --exclude-standard)

    if ($LASTEXITCODE -ne 0) {
        throw "Unable to retrieve repository file list."
    }

    # Text-file extensions
    $extensions = @(
        ".py", ".ps1", ".md", ".txt",
        ".json", ".yml", ".yaml", ".toml",
        ".ini", ".cfg", ".sh", ".html",
        ".css", ".js", ".ts", ".tsx",
        ".jsx", ".kt", ".kts", ".xml",
        ".gradle", ".properties", ".sql",
        ".conf", ".service", ".csv"
    )

    # Extensionless and special text files
    $specialNames = @(
        "Dockerfile",
        "Makefile",
        "Jenkinsfile",
        ".gitignore",
        ".gitattributes",
        ".dockerignore",
        ".editorconfig",
        ".env.example"
    )

    return @(
        $files | Where-Object {
            $name = [System.IO.Path]::GetFileName($_)
            $extension = [System.IO.Path]::GetExtension($_)

            ($extensions -contains $extension) -or
            ($specialNames -contains $name)
        }
    )
}

Write-Host "`n======================================" -ForegroundColor Cyan
Write-Host "       StreamVault GitHub Upload"
Write-Host "======================================`n" -ForegroundColor Cyan

try {
    # 1. Verify repository
    git rev-parse --is-inside-work-tree *> $null

    if ($LASTEXITCODE -ne 0) {
        throw "Current directory is not a Git repository."
    }

    # 2. Verify branch
    $branch = git branch --show-current

    if ($LASTEXITCODE -ne 0 -or $branch -ne "main") {
        throw "Expected main branch. Current branch: $branch"
    }

    # 3. Verify remote
    $remote = git remote get-url origin

    if ($LASTEXITCODE -ne 0) {
        throw "GitHub remote 'origin' not configured."
    }

    Write-Host "Repository: $remote" -ForegroundColor Gray
    Write-Host "Branch: $branch" -ForegroundColor Gray

    # 4. Automatically clean repository text files
    Write-Host "`n[1/6] Cleaning whitespace and EOF..." -ForegroundColor Yellow

    $textFiles = @(Get-TextFiles)

    foreach ($file in $textFiles) {
        Clean-FileWhitespace -FilePath $file
    }

    Write-Host "Checked $($textFiles.Count) text files." -ForegroundColor Green

    # 5. Stage changes
    Write-Host "`n[2/6] Staging changes..." -ForegroundColor Yellow

    git add -A

    if ($LASTEXITCODE -ne 0) {
        throw "Git staging failed."
    }

    # 6. Check staged files
    Write-Host "`n[3/6] Checking sensitive and temporary files..." -ForegroundColor Yellow

    $staged = @(git diff --cached --name-only)

    $blocked = $staged | Where-Object {
        (
            $_ -match '(^|/)\.env($|\.)' -and
            $_ -notmatch '(^|/)\.env\.example$'
        ) -or
        $_ -match '(^|/)cookies\.txt$' -or
        $_ -match '(^|/)(temp_downloads|__pycache__|\.venv|venv)(/|$)'
    }

    if ($blocked) {
        throw "Sensitive or temporary files detected: $($blocked -join ', ')"
    }

    # Check sensitive files already tracked by Git
    $tracked = @(git ls-files)

    $trackedBlocked = $tracked | Where-Object {
        (
            $_ -match '(^|/)\.env($|\.)' -and
            $_ -notmatch '(^|/)\.env\.example$'
        ) -or
        $_ -match '(^|/)cookies\.txt$'
    }

    if ($trackedBlocked) {
        throw "Sensitive files already tracked by Git: $($trackedBlocked -join ', ')"
    }

    # 7. Validate whitespace
    Write-Host "`n[4/6] Validating changes..." -ForegroundColor Yellow

    $checkOutput = @(git diff --cached --check 2>&1)
    $checkExitCode = $LASTEXITCODE

    if ($checkExitCode -ne 0) {
        $checkOutput | ForEach-Object {
            Write-Host $_ -ForegroundColor Red
        }

        throw "Whitespace validation failed. Upload stopped."
    }

    Write-Host "Whitespace validation passed." -ForegroundColor Green

    if ($staged.Count -eq 0) {
        Write-Host "`nNo changes to upload." -ForegroundColor Yellow
        exit 0
    }

    # 8. Display changes
    Write-Host "`nFiles ready for upload:" -ForegroundColor Cyan

    git diff --cached --stat

    Write-Host "`nStaged files:" -ForegroundColor Cyan

    $staged | ForEach-Object {
        Write-Host "  $_"
    }

    # 9. Custom commit message
    Write-Host ""
    $message = Read-Host "Enter commit message"

    if ([string]::IsNullOrWhiteSpace($message)) {
        throw "Commit message cannot be empty."
    }

    Write-Host "`nCommit message: $message" -ForegroundColor Green

    # 10. Confirmation
    Write-Host "`nTarget: origin/main" -ForegroundColor Yellow

    $confirm = Read-Host "Proceed with commit and push? (Y/N)"

    if ($confirm -notmatch '^(Y|YES)$') {
        Write-Host "`nUpload cancelled." -ForegroundColor Yellow
        exit 0
    }

    # 11. Commit
    Write-Host "`n[5/6] Committing..." -ForegroundColor Yellow

    git commit -m $message

    if ($LASTEXITCODE -ne 0) {
        throw "Git commit failed."
    }

    # 12. Push
    Write-Host "`n[6/6] Pushing to GitHub..." -ForegroundColor Yellow

    git push origin main

    if ($LASTEXITCODE -ne 0) {
        throw "Git push failed. Local commit is preserved."
    }

    Write-Host "`n======================================" -ForegroundColor Green
    Write-Host "        UPLOAD SUCCESSFUL"
    Write-Host "======================================`n" -ForegroundColor Green
}
catch {
    Write-Host "`nERROR: $_" -ForegroundColor Red
    Write-Host "Upload stopped." -ForegroundColor Red
    exit 1
}
