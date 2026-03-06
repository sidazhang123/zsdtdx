#!/usr/bin/env pwsh
Set-Location "C:\Users\sida\Desktop\zsdtdx"
Write-Host "Checking git status..."
git status --porcelain | Select-Object -First 10
Write-Host "Adding all files..."
git add -A
Write-Host "Committing..."
git commit -m "v1.1.1: prewarm_parallel_fetcher uses config"
Write-Host "Pushing to remote..."
git push
Write-Host "Done!"
