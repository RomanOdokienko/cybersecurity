param(
  [string[]]$Domains,
  [string]$FilePath
)

$ErrorActionPreference = "Stop"

function Normalize-Domain([string]$raw) {
  if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
  $d = $raw.Trim().ToLower()
  $d = $d -replace "^\s*https?://", ""
  $d = $d -replace "/.*$", ""
  $d = $d.Trim()
  if ([string]::IsNullOrWhiteSpace($d)) { return $null }
  return $d
}

function Get-DomainsFromFile([string]$path) {
  if ([string]::IsNullOrWhiteSpace($path)) { return @() }
  if (-not (Test-Path -LiteralPath $path)) {
    throw "File not found: $path"
  }

  $text = Get-Content -LiteralPath $path -Raw
  if ([string]::IsNullOrWhiteSpace($text)) { return @() }
  $parts = $text -split "[`r`n,;]"
  return @($parts | ForEach-Object { Normalize-Domain $_ } | Where-Object { $_ } | Sort-Object -Unique)
}

$fromArgsRaw = @()
foreach ($entry in @($Domains)) {
  if ([string]::IsNullOrWhiteSpace("$entry")) { continue }
  $fromArgsRaw += ($entry -split "[,;`r`n]")
}
$fromArgs = @($fromArgsRaw | ForEach-Object { Normalize-Domain $_ } | Where-Object { $_ } | Sort-Object -Unique)
$fromFile = Get-DomainsFromFile -path $FilePath
$targetDomains = @((@($fromArgs) + @($fromFile)) | Sort-Object -Unique)

if ($targetDomains.Count -eq 0) {
  throw "No domains provided. Use -Domains and/or -FilePath."
}

$results = @()
foreach ($domain in $targetDomains) {
  Write-Host "==> $domain"
  try {
    powershell -ExecutionPolicy Bypass -File "scripts/run-site-quick.ps1" -Domain $domain
    $results += [pscustomobject]@{
      domain = $domain
      status = "ok"
      error = $null
    }
  } catch {
    $results += [pscustomobject]@{
      domain = $domain
      status = "error"
      error = "$($_.Exception.Message)"
    }
  }
}

powershell -ExecutionPolicy Bypass -File "scripts/export-dashboard-data.ps1"

$okCount = @($results | Where-Object { $_.status -eq "ok" }).Count
$errCount = @($results | Where-Object { $_.status -eq "error" }).Count
Write-Host ""
Write-Host "Batch complete. ok=$okCount; error=$errCount; total=$($results.Count)"
if ($errCount -gt 0) {
  Write-Host "Failed domains:"
  $results | Where-Object { $_.status -eq "error" } | ForEach-Object {
    Write-Host " - $($_.domain): $($_.error)"
  }
}
