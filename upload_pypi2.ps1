#!/usr/bin/env pwsh
Set-Location "C:\Users\sida\Desktop\zsdtdx"
Write-Host "Uploading to PyPI (no proxy)..."
set HTTP_PROXY=""
set HTTPS_PROXY=""
set http_proxy=""
set https_proxy=""
python -m twine upload --no-proxy dist/*
Write-Host "Done!"
