$normalizedDir = "data/normalized"
$outputPath = "dashboard/sample_audits.json"
$reportIndexPath = "dashboard/report_index.json"

if (-not (Test-Path $normalizedDir)) {
  Write-Error "Missing directory: $normalizedDir"
  exit 1
}

function Normalize-Domain([string]$domain) {
  if ([string]::IsNullOrWhiteSpace($domain)) { return "" }
  return $domain.ToLower().Replace("https://", "").Replace("http://", "").TrimEnd("/")
}

function Get-DomainTokens([string]$domain, [string]$siteId) {
  $domainHost = Normalize-Domain $domain
  if ($domainHost.StartsWith("www.")) { $domainHost = $domainHost.Substring(4) }

  $tokens = New-Object System.Collections.Generic.HashSet[string]
  if ($domainHost) { [void]$tokens.Add($domainHost) }
  if ($domainHost) { [void]$tokens.Add($domainHost.Replace(".", "-")) }

  $parts = $domainHost.Split(".")
  if ($parts.Count -ge 2) {
    $base = ($parts[0..($parts.Count - 2)] -join "-")
    if ($base) { [void]$tokens.Add($base) }
  }

  if ($siteId) {
    $sid = $siteId.ToLower()
    [void]$tokens.Add($sid)
    [void]$tokens.Add($sid.Replace("_", "-"))
  }

  return @($tokens | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Get-Label([string]$baseName, [string]$ext) {
  $extUp = $ext.TrimStart(".").ToUpper()
  if ($baseName -match "152fz_detailed") { return "152-ФЗ подробный отчет ($extUp)" }
  if ($baseName -match "sales_offer") { return "Продажный one-pager ($extUp)" }
  if ($baseName -match "criteria_matrix") { return "Матрица критериев ($extUp)" }
  return "Отчет ($extUp)"
}

$rawItems = @()
Get-ChildItem -Path $normalizedDir -Filter *.json -File | Sort-Object LastWriteTime -Descending | ForEach-Object {
  try {
    $content = Get-Content -Path $_.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
    $rawItems += [pscustomobject]@{
      file_name = $_.Name
      data = $content
    }
  } catch {
    Write-Warning "Skipped invalid JSON: $($_.Name)"
  }
}

$items = $rawItems | ForEach-Object { $_.data }

$latestByDomain = $items |
  Group-Object -Property domain |
  ForEach-Object {
    $_.Group | Sort-Object { [datetime]$_.audit_timestamp_utc } -Descending | Select-Object -First 1
  } |
  Sort-Object { [datetime]$_.audit_timestamp_utc } -Descending

$latestByDomain | ConvertTo-Json -Depth 12 | Set-Content -Path $outputPath -Encoding UTF8

$domains = @{}
$latestByDomain | ForEach-Object {
  $domain = Normalize-Domain $_.domain
  if (-not $domain) { return }

  $domains[$domain] = [ordered]@{
    domain = $domain
    site_id = $_.site_id
    links = (New-Object System.Collections.ArrayList)
    full_markers = $null
    full_audit_timestamp = $null
  }
}

$tokenMap = @{}
foreach ($d in $domains.Keys) {
  $tokenMap[$d] = Get-DomainTokens -domain $d -siteId $domains[$d]["site_id"]
}

$fullCandidates = $rawItems | Where-Object {
  $_.file_name -like "*.full.json" -or ($_.data.mode -eq "full")
}

$latestFullByDomain = @{}
foreach ($row in $fullCandidates) {
  $d = Normalize-Domain $row.data.domain
  if (-not $d) { continue }
  $ts = [datetime]$row.data.audit_timestamp_utc
  if (-not $latestFullByDomain.ContainsKey($d) -or $ts -gt [datetime]$latestFullByDomain[$d].audit_timestamp_utc) {
    $latestFullByDomain[$d] = $row.data
  }
}

foreach ($d in $latestFullByDomain.Keys) {
  if (-not $domains.ContainsKey($d)) {
    $domains[$d] = [ordered]@{
      domain = $d
      site_id = $latestFullByDomain[$d].site_id
      links = (New-Object System.Collections.ArrayList)
      full_markers = $null
      full_audit_timestamp = $null
    }
    $tokenMap[$d] = Get-DomainTokens -domain $d -siteId $domains[$d]["site_id"]
  }

  $domains[$d]["full_markers"] = $latestFullByDomain[$d].extra_full_markers
  $domains[$d]["full_audit_timestamp"] = $latestFullByDomain[$d].audit_timestamp_utc
}

$outputFiles = Get-ChildItem -Path outputs -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object { $_.Extension -in @('.html', '.pdf', '.md') }

foreach ($f in $outputFiles) {
  $base = [System.IO.Path]::GetFileNameWithoutExtension($f.Name).ToLower()
  $bestDomain = $null
  $bestLen = -1

  foreach ($d in $tokenMap.Keys) {
    foreach ($token in $tokenMap[$d]) {
      if ($base.StartsWith($token) -and $token.Length -gt $bestLen) {
        $bestDomain = $d
        $bestLen = $token.Length
      }
    }
  }

  if (-not $bestDomain) { continue }

  $rel = Resolve-Path -LiteralPath $f.FullName -Relative
  $rel = ($rel -replace '^[.\\]+', '') -replace '\\', '/'
  $pathForDashboard = "../$rel"

  [void]$domains[$bestDomain]["links"].Add([pscustomobject]@{
    label = Get-Label -baseName $base -ext $f.Extension
    path = $pathForDashboard
    kind = $f.Extension.TrimStart('.').ToLower()
  })
}

$indexOut = [ordered]@{}
foreach ($d in ($domains.Keys | Sort-Object)) {
  $entry = $domains[$d]
  $entry["links"] = @($entry["links"] | Sort-Object path)
  $indexOut[$d] = $entry
}

$indexOut | ConvertTo-Json -Depth 12 | Set-Content -Path $reportIndexPath -Encoding UTF8

Write-Output "Exported $($latestByDomain.Count) audit record(s) to $outputPath"
Write-Output "Exported report index for $($indexOut.Keys.Count) domain(s) to $reportIndexPath"
