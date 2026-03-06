#!/usr/bin/env pwsh
Set-Location "C:\Users\sida\Desktop\zsdtdx"
Write-Host "Uploading to PyPI..."
python -m twine upload dist/*
Write-Host "Done!"
