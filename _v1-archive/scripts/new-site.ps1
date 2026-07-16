param(
  [Parameter(Mandatory = $true)]
  [string]$Domain
)

$siteId = $Domain.ToLower().Replace("https://", "").Replace("http://", "").Replace("/", "_").Replace(".", "_")
$timestamp = (Get-Date).ToUniversalTime().ToString("o")

$paths = @(
  "data/raw/$siteId",
  "data/raw/$siteId/http",
  "data/raw/$siteId/dns",
  "data/raw/$siteId/osint",
  "data/snapshots/$siteId"
)

foreach ($p in $paths) {
  New-Item -ItemType Directory -Path $p -Force | Out-Null
}

$queuePath = "data/input/site_queue.csv"
if (-not (Test-Path $queuePath)) {
  "site_id,domain,status,owner,notes,created_utc" | Set-Content -Path $queuePath -Encoding UTF8
}

$line = "$siteId,$Domain,pending,,,${timestamp}"
Add-Content -Path $queuePath -Value $line -Encoding UTF8

$seedJson = @"
{
  "site_id": "$siteId",
  "domain": "$Domain",
  "audit_timestamp_utc": "$timestamp",
  "overall_score": 0,
  "risk_band": "low",
  "free_offer_summary": [],
  "blocks": []
}
"@

$normalizedPath = "data/normalized/$siteId.json"
if (-not (Test-Path $normalizedPath)) {
  $seedJson | Set-Content -Path $normalizedPath -Encoding UTF8
}

Write-Output "Initialized site workspace: $siteId"
Write-Output "Normalized file: $normalizedPath"
