$env:HTTP_PROXY=""
$env:HTTPS_PROXY=""
$env:http_proxy=""
$env:https_proxy=""
Set-Location "C:\Users\sida\Desktop\zsdtdx"
Write-Host "Uploading to PyPI..."
python -m twine upload dist/*
Write-Host "Done!"
