param(
  [Parameter(Mandatory = $true)]
  [string]$Domain
)

$ErrorActionPreference = 'Stop'

function Get-SiteId([string]$d) {
  return $d.ToLower().Replace('https://','').Replace('http://','').Replace('/','_').Replace('.','_')
}

function Get-Header($map, [string]$name) {
  foreach ($k in $map.Keys) {
    if ($k.ToLower() -eq $name.ToLower()) { return "$($map[$k])" }
  }
  return $null
}

function Parse-CurlHeaders([string[]]$lines) {
  $status = 0
  $headers = @{}
  foreach ($line in $lines) {
    $t = "$line".TrimEnd()
    if ($t -match '^HTTP/\S+\s+(\d{3})') {
      $status = [int]$Matches[1]
      $headers = @{}
      continue
    }
    if ($t -match '^\s*([^:]+):\s*(.*)$') {
      $headers[$Matches[1].Trim()] = $Matches[2].Trim()
    }
  }
  return [pscustomobject]@{ status = $status; headers = $headers }
}

function Invoke-Req {
  param(
    [string]$Url,
    [string]$Method = 'GET',
    [int]$TimeoutSec = 10,
    [int]$MaxRedirect = 3
  )

  $m = $Method.ToUpper()
  if ($m -eq 'HEAD') {
    $args = @('-sS','-I','--max-time',"$TimeoutSec",'-A','Mozilla/5.0')
    if ($MaxRedirect -gt 0) { $args += '-L' }
    $args += $Url
    $raw = & curl.exe @args 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $raw) {
      return [pscustomobject]@{ url = $Url; status = 0; headers = @{}; final_url = $Url; body = ''; error = 'curl_head_failed' }
    }
    $p = Parse-CurlHeaders @($raw)
    $loc = Get-Header $p.headers 'Location'
    return [pscustomobject]@{ url = $Url; status = $p.status; headers = $p.headers; final_url = $(if($loc){$loc}else{$Url}); body = ''; error = $null }
  }

  $h = Invoke-Req -Url $Url -Method 'HEAD' -TimeoutSec $TimeoutSec -MaxRedirect $MaxRedirect
  $argsBody = @('-sS','--max-time',"$TimeoutSec",'-A','Mozilla/5.0')
  if ($MaxRedirect -gt 0) { $argsBody += '-L' }
  $argsBody += $Url
  $rawBody = & curl.exe @argsBody 2>$null
  $body = if ($rawBody) { ($rawBody -join "`n") } else { '' }
  return [pscustomobject]@{ url = $Url; status = $h.status; headers = $h.headers; final_url = $h.final_url; body = $body; error = $(if($LASTEXITCODE -eq 0){$null}else{'curl_get_failed'}) }
}

function Resolve-Txt([string]$Name) {
  try {
    $records = Resolve-DnsName -Name $Name -Type TXT -QuickTimeout -ErrorAction Stop
    $out = @()
    foreach ($r in $records) {
      if ($r.Strings) { $out += ($r.Strings -join '') }
    }
    return $out
  } catch { return @() }
}

function Get-TlsInfo([string]$TargetHost) {
  $tcp = $null
  $ssl = $null
  try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $ar = $tcp.BeginConnect($TargetHost,443,$null,$null)
    if (-not $ar.AsyncWaitHandle.WaitOne(7000,$false)) { throw 'TCP timeout' }
    $tcp.EndConnect($ar)
    $ssl = New-Object System.Net.Security.SslStream($tcp.GetStream(),$false,({$true}))
    $ssl.AuthenticateAsClient($TargetHost)
    $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 $ssl.RemoteCertificate
    $now = (Get-Date).ToUniversalTime()
    return [pscustomobject]@{
      valid_now = ($now -lt $cert.NotAfter.ToUniversalTime() -and $now -gt $cert.NotBefore.ToUniversalTime())
      days_to_expiry = [math]::Floor(($cert.NotAfter.ToUniversalTime() - $now).TotalDays)
      signature_algorithm = $cert.SignatureAlgorithm.FriendlyName
      issuer = $cert.Issuer
      not_after = $cert.NotAfter.ToUniversalTime().ToString('o')
    }
  } catch {
    return [pscustomobject]@{ valid_now = $false; error = "$($_.Exception.Message)" }
  } finally {
    if ($ssl) { $ssl.Dispose() }
    if ($tcp) { $tcp.Close() }
  }
}

function Normalize-InternalUrl([string]$baseUrl, [string]$link, [string]$targetHost) {
  if (-not $link) { return $null }
  $trim = $link.Trim()
  if ($trim -match '^(?i)(mailto:|tel:|javascript:|#)') { return $null }
  if ($trim.StartsWith('//')) { $trim = "https:$trim" }
  try {
    if ($trim.StartsWith('http://') -or $trim.StartsWith('https://')) { $u = [Uri]$trim }
    else { $u = [Uri]::new([Uri]$baseUrl, $trim) }
  } catch { return $null }
  if ($u.Host.ToLower() -ne $targetHost.ToLower()) { return $null }
  return ("$($u.Scheme)://$($u.Host)$($u.AbsolutePath)").TrimEnd('/')
}

function Extract-InternalLinks([string]$html, [string]$baseUrl, [string]$targetHost) {
  $out = @()
  if (-not $html) { return $out }
  $m = [regex]::Matches($html, '(?is)href\s*=\s*["'']([^"''#>]+)')
  foreach ($x in $m) {
    $n = Normalize-InternalUrl -baseUrl $baseUrl -link $x.Groups[1].Value -targetHost $targetHost
    if ($n) { $out += $n }
  }
  return ($out | Sort-Object -Unique)
}

function Get-Title([string]$html) {
  if (-not $html) { return '' }
  if ($html -match '(?is)<title[^>]*>(.*?)</title>') { return (($Matches[1] -replace '\s+',' ').Trim()) }
  return ''
}

function RiskBand([double]$s) {
  if ($s -ge 80) { return 'critical' }
  if ($s -ge 60) { return 'high' }
  if ($s -ge 35) { return 'medium' }
  return 'low'
}

function StatusByScore([double]$s,[bool]$notTested) {
  if ($notTested) { return 'not_tested' }
  if ($s -ge 70) { return 'critical' }
  if ($s -ge 35) { return 'warning' }
  return 'ok'
}
$siteId = Get-SiteId $Domain
$timestamp = (Get-Date).ToUniversalTime().ToString('o')
$rawDir = "data/raw/$siteId"
$fullDir = "data/full/$siteId"
$fullOutDir = 'outputs/full'
$normalizedPath = "data/normalized/$siteId.full.json"
$evidencePath = "$rawDir/evidence_full.json"
$fullPath = "$fullDir/full_audit.json"
$comparisonPath = "$fullOutDir/${siteId}_quick_vs_full.md"

New-Item -ItemType Directory -Path $rawDir -Force | Out-Null
New-Item -ItemType Directory -Path $fullDir -Force | Out-Null
New-Item -ItemType Directory -Path $fullOutDir -Force | Out-Null

$homeUrl = "https://$Domain/"
$homeResp = Invoke-Req -Url $homeUrl -Method 'GET' -TimeoutSec 6 -MaxRedirect 4
$homeHeaders = $homeResp.headers

$httpHome = Invoke-Req -Url "http://$Domain/" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 0
$tls = Get-TlsInfo -TargetHost $Domain
$wwwTls = Get-TlsInfo -TargetHost "www.$Domain"
$wwwHttps = Invoke-Req -Url "https://www.$Domain/" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 2
$wwwHttp = Invoke-Req -Url "http://www.$Domain/" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 0

$hsts = Get-Header $homeHeaders 'Strict-Transport-Security'
$location = Get-Header $httpHome.headers 'Location'
$redirectOk = (@(301,302,307,308) -contains $httpHome.status) -and ($location -and $location.ToLower().StartsWith('https://'))
$mixedContent = 0
if ($homeResp.body) { $mixedContent = ([regex]::Matches($homeResp.body, '(?i)(src|href)\s*=\s*["'']http://')).Count }

$seedLinks = Extract-InternalLinks -html $homeResp.body -baseUrl $homeUrl -targetHost $Domain
$pageUrls = @($homeUrl.TrimEnd('/'))
$pageUrls += ($seedLinks | Select-Object -First 7)
$pageUrls = $pageUrls | Sort-Object -Unique | Select-Object -First 8

$pages = @()
foreach ($u in $pageUrls) {
  $p = Invoke-Req -Url $u -Method 'GET' -TimeoutSec 6 -MaxRedirect 3
  $pages += [pscustomobject]@{ url = $u; status = $p.status; title = Get-Title $p.body; body = $p.body; headers = $p.headers }
}

$dict = Get-Content 'config/path_dictionary.txt' | Where-Object { $_ -and -not $_.StartsWith('#') }
$fileChecks = @()
$sensitiveExposed = @()
$dirListingHits = @()
foreach ($path in $dict) {
  $resp = Invoke-Req -Url "https://$Domain$path" -Method 'HEAD' -TimeoutSec 3 -MaxRedirect 1
  $fileChecks += [pscustomobject]@{ path = $path; status = $resp.status; final_url = $resp.final_url }
  if (-not $path.EndsWith('/') -and @((200),(206)) -contains $resp.status) { $sensitiveExposed += $path }
  if ($path.EndsWith('/') -and $resp.status -eq 200) {
    $dir = Invoke-Req -Url "https://$Domain$path" -Method 'GET' -TimeoutSec 3 -MaxRedirect 1
    if ($dir.body -match '(?i)index of|directory listing|parent directory') { $dirListingHits += $path }
  }
}

$wpAdmin = Invoke-Req -Url "https://$Domain/wp-admin/" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 0
$wpLogin = Invoke-Req -Url "https://$Domain/wp-login.php" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 0
$bitrixAdmin = Invoke-Req -Url "https://$Domain/bitrix/admin/" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 0
$adminGeneric = Invoke-Req -Url "https://$Domain/admin/" -Method 'HEAD' -TimeoutSec 6 -MaxRedirect 0
$readme = Invoke-Req -Url "https://$Domain/readme.html" -Method 'GET' -TimeoutSec 6 -MaxRedirect 1
$feed = Invoke-Req -Url "https://$Domain/feed" -Method 'GET' -TimeoutSec 6 -MaxRedirect 2

$cms = 'unknown'
$generator = $null
if ($homeResp.body -match '(?is)<meta[^>]+name=["'']generator["''][^>]+content=["'']([^"'']+)["'']') { $generator = $Matches[1] }
if ($homeResp.body -match '(?i)tilda') { $cms = 'tilda' }
if ($homeResp.body -match '(?i)wp-content|wp-includes|wordpress') { $cms = 'wordpress' }
if ($homeResp.body -match '(?i)bitrix') { $cms = 'bitrix' }
if ($generator -and $cms -eq 'unknown') { $cms = $generator }

$wpVersion = $null
if ($readme.body -match '(?i)version\s+([0-9]+\.[0-9]+(\.[0-9]+)?)') { $wpVersion = $Matches[1] }
if (-not $wpVersion -and $feed.body -match '(?i)wordpress\.org\/\?v=([0-9]+\.[0-9]+(\.[0-9]+)?)') { $wpVersion = $Matches[1] }

$requiredHeaders = @('Strict-Transport-Security','Content-Security-Policy','X-Frame-Options','X-Content-Type-Options','Referrer-Policy','Permissions-Policy')
$missingHeaders = @()
foreach ($h in $requiredHeaders) { if (-not (Get-Header $homeHeaders $h)) { $missingHeaders += $h } }
$serverHeader = Get-Header $homeHeaders 'Server'
$xPoweredBy = Get-Header $homeHeaders 'X-Powered-By'

$formsTotal = 0
$formPages = @()
$httpFormActions = @()
$formPagesWithoutPrivacy = @()
$formPagesWithoutConsent = @()
$captchaPages = @()
$honeypotPages = @()
$cookieNoticePages = @()
$privacyPages = @()
$policy152Pages = @()
$policyUrls = @()

foreach ($page in $pages) {
  if (-not $page.body) { continue }
  $hasForm = $page.body -match '(?is)<form\b'
  $hasPrivacy = $page.body -match '(?i)политик|конфиденц|privacy\s*policy|персональн'
  $has152 = $page.body -match '(?i)152[\-\s]?фз|федеральн(ый|ого)\s+закон|персональн(ых|ые)\s+данн'
  $hasConsent = $page.body -match '(?i)соглас(ие|ен)|consent'
  $hasCaptcha = $page.body -match '(?i)recaptcha|hcaptcha|captcha'
  $hasHoneypot = $page.body -match '(?i)honeypot|_gotcha|name=["'']website["'']'
  $hasCookie = $page.body -match '(?i)cookie|куки'

  if ($hasPrivacy) { $privacyPages += $page.url }
  if ($has152) { $policy152Pages += $page.url }
  if ($hasCaptcha) { $captchaPages += $page.url }
  if ($hasHoneypot) { $honeypotPages += $page.url }
  if ($hasCookie) { $cookieNoticePages += $page.url }

  $pl = [regex]::Matches($page.body, '(?is)href\s*=\s*["'']([^"'']*(policy|privacy|politika|confiden)[^"'']*)["'']')
  foreach ($m in $pl) {
    $purl = Normalize-InternalUrl -baseUrl $page.url -link $m.Groups[1].Value -targetHost $Domain
    if ($purl) { $policyUrls += $purl }
  }

  if ($hasForm) {
    $formPages += $page.url
    $formsTotal += ([regex]::Matches($page.body, '(?is)<form\b')).Count
    if (-not $hasPrivacy) { $formPagesWithoutPrivacy += $page.url }
    if (-not $hasConsent) { $formPagesWithoutConsent += $page.url }

    $fa = [regex]::Matches($page.body, '(?is)<form\b[^>]*action\s*=\s*["'']([^"'']+)["'']')
    foreach ($x in $fa) {
      $action = $x.Groups[1].Value
      if ($action.ToLower().StartsWith('http://')) { $httpFormActions += "$($page.url) -> $action" }
    }
  }
}

$formPages = $formPages | Sort-Object -Unique
$formPagesWithoutPrivacy = $formPagesWithoutPrivacy | Sort-Object -Unique
$formPagesWithoutConsent = $formPagesWithoutConsent | Sort-Object -Unique
$privacyPages = $privacyPages | Sort-Object -Unique
$policy152Pages = $policy152Pages | Sort-Object -Unique
$policyUrls = $policyUrls | Sort-Object -Unique
$captchaPages = $captchaPages | Sort-Object -Unique
$honeypotPages = $honeypotPages | Sort-Object -Unique
$cookieNoticePages = $cookieNoticePages | Sort-Object -Unique

$spf = Resolve-Txt $Domain | Where-Object { $_ -match '(?i)^v=spf1' }
$dmarc = Resolve-Txt "_dmarc.$Domain"
$dkimSelectors = @('default','google','mail')
$dkimFound = @()
foreach ($sel in $dkimSelectors) {
  $dk = Resolve-Txt "$sel._domainkey.$Domain"
  if ($dk.Count -gt 0) { $dkimFound += $sel }
}
$mtaSts = Resolve-Txt "_mta-sts.$Domain"
$tlsRpt = Resolve-Txt "_smtp._tls.$Domain"
$externalScriptDomains = @()
$externalScriptsTotal = 0
$externalScriptsNoSRI = 0
$outdatedJquery = @()
foreach ($page in $pages) {
  if (-not $page.body) { continue }
  $sm = [regex]::Matches($page.body, '(?is)<script\b[^>]*src\s*=\s*["'']([^"'']+)["''][^>]*>')
  foreach ($s in $sm) {
    $tag = $s.Value
    $src = $s.Groups[1].Value
    $resolved = $src
    if ($resolved.StartsWith('//')) { $resolved = "https:$resolved" }
    $scriptHost = $null
    try {
      if ($resolved.StartsWith('http://') -or $resolved.StartsWith('https://')) { $scriptHost = ([Uri]$resolved).Host.ToLower() }
      else { $scriptHost = $Domain.ToLower() }
    } catch { $scriptHost = $null }

    if ($scriptHost -and ($scriptHost -ne $Domain.ToLower()) -and (-not $scriptHost.EndsWith(".$($Domain.ToLower())"))) {
      $externalScriptsTotal += 1
      $externalScriptDomains += $scriptHost
      if ($tag -notmatch '(?i)\sintegrity\s*=') { $externalScriptsNoSRI += 1 }
    }

    if ($resolved -match '(?i)jquery[^0-9]*([0-9]+\.[0-9]+(\.[0-9]+)?)') {
      $v = $Matches[1]
      $parts = $v.Split('.')
      $major = [int]$parts[0]
      $minor = if ($parts.Count -gt 1) { [int]$parts[1] } else { 0 }
      if (($major -eq 1 -and $minor -lt 12) -or ($major -eq 2)) { $outdatedJquery += $v }
    }
  }
}
$externalScriptDomains = $externalScriptDomains | Sort-Object -Unique
$outdatedJquery = $outdatedJquery | Sort-Object -Unique

$robots = Invoke-Req -Url "https://$Domain/robots.txt" -Method 'GET' -TimeoutSec 6 -MaxRedirect 2
$sitemap = Invoke-Req -Url "https://$Domain/sitemap.xml" -Method 'GET' -TimeoutSec 6 -MaxRedirect 2
$randomPath = '/this-page-should-not-exist-' + ([guid]::NewGuid().ToString('N'))
$soft404 = Invoke-Req -Url "https://$Domain$randomPath" -Method 'GET' -TimeoutSec 6 -MaxRedirect 2
$homeTitle = Get-Title $homeResp.body
$softTitle = Get-Title $soft404.body
$soft404Likely = ($soft404.status -eq 200) -and ($softTitle -eq $homeTitle -or [string]::IsNullOrWhiteSpace($softTitle))

$archiveSampleCount = 0
$archiveError = $null
try {
  $archiveUrl = "https://web.archive.org/cdx/search/cdx?url=$Domain/*&output=json&fl=timestamp,original,statuscode&filter=statuscode:200&limit=5"
  $archiveRaw = & curl.exe -sS --max-time 12 -A 'Mozilla/5.0' $archiveUrl 2>$null
  if ($LASTEXITCODE -eq 0 -and $archiveRaw) {
    $archiveJson = ($archiveRaw -join "`n") | ConvertFrom-Json
    if ($archiveJson.Count -gt 1) { $archiveSampleCount = $archiveJson.Count - 1 }
  }
} catch { $archiveError = "$($_.Exception.Message)" }

$sslRisk = 0; $sslFindings = @()
if (-not $tls.valid_now) { $sslRisk += 45; $sslFindings += 'TLS certificate is invalid/unavailable.' }
if ($tls.days_to_expiry -is [int] -and $tls.days_to_expiry -lt 30) { $sslRisk += 25; $sslFindings += 'Certificate expires in less than 30 days.' }
if (-not $redirectOk) { $sslRisk += 25; $sslFindings += 'HTTP->HTTPS redirect is not strict.' }
if (-not $hsts) { $sslRisk += 15; $sslFindings += 'HSTS header is missing.' }
if ($mixedContent -gt 0) { $sslRisk += 15; $sslFindings += "Mixed-content links detected ($mixedContent)." }
if (-not $wwwTls.valid_now) { $sslRisk += 10; $sslFindings += 'www-host TLS endpoint is unavailable or invalid.' }
if ($sslRisk -gt 100) { $sslRisk = 100 }

$fileRisk = 0; $fileFindings = @()
if ($sensitiveExposed.Count -gt 0) { $fileRisk += [math]::Min(100,40 + ($sensitiveExposed.Count * 20)); $fileFindings += "Sensitive files exposed: $($sensitiveExposed -join ', ')." }
if ($dirListingHits.Count -gt 0) { $fileRisk += [math]::Min(40,$dirListingHits.Count * 20); $fileFindings += "Directory listing indicators: $($dirListingHits -join ', ')." }
if ($fileRisk -gt 100) { $fileRisk = 100 }

$openAdminRisk = 0; $openAdminFindings = @(); $adminOpen = @()
if ($wpAdmin.status -eq 200) { $adminOpen += '/wp-admin/' }
if ($bitrixAdmin.status -eq 200) { $adminOpen += '/bitrix/admin/' }
if ($adminGeneric.status -eq 200) { $adminOpen += '/admin/' }
if ($adminOpen.Count -gt 0) { $openAdminRisk += 35; $openAdminFindings += "Admin endpoints reachable without redirect: $($adminOpen -join ', ')." }
if ($wpVersion) { $openAdminRisk += 20; $openAdminFindings += "Public WordPress version leakage: $wpVersion." }
if ($cms -ne 'unknown') { $openAdminFindings += "CMS fingerprint: $cms." }
if ($openAdminRisk -gt 100) { $openAdminRisk = 100 }

$headersRisk = 0; $headerFindings = @()
if ($missingHeaders.Count -gt 0) { $headersRisk += [math]::Min(70,$missingHeaders.Count * 12); $headerFindings += "Missing security headers: $($missingHeaders -join ', ')." }
if ($serverHeader) { $headersRisk += 10; $headerFindings += 'Server header discloses infrastructure vendor.' }
if ($xPoweredBy) { $headersRisk += 10; $headerFindings += 'X-Powered-By header discloses technology.' }
if ($headersRisk -gt 100) { $headersRisk = 100 }

$formsRisk = 0; $formsFindings = @(); $formsNotTested = $false
if ($formPages.Count -eq 0) { $formsNotTested = $true }
else {
  if ($httpFormActions.Count -gt 0) { $formsRisk += 45; $formsFindings += 'Form action over HTTP detected.' }
  if ($formPagesWithoutPrivacy.Count -gt 0) { $formsRisk += 20; $formsFindings += 'Forms found on pages without privacy policy markers.' }
  if ($formPagesWithoutConsent.Count -gt 0) { $formsRisk += 20; $formsFindings += 'Forms found without explicit consent markers.' }
  if (($captchaPages.Count + $honeypotPages.Count) -eq 0) { $formsRisk += 15; $formsFindings += 'No CAPTCHA/honeypot markers detected on crawled form pages.' }
  if ($cookieNoticePages.Count -eq 0) { $formsRisk += 10; $formsFindings += 'Cookie notice marker not detected.' }
}
if ($formsRisk -gt 100) { $formsRisk = 100 }

$dnsRisk = 0; $dnsFindings = @()
if ($spf.Count -eq 0) { $dnsRisk += 25; $dnsFindings += 'SPF record missing.' }
else {
  $spfJoined = ($spf -join ' ').ToLower()
  if ($spfJoined -match '~all') { $dnsRisk += 10; $dnsFindings += 'SPF policy softfail (~all).' }
  elseif ($spfJoined -notmatch '-all') { $dnsRisk += 15; $dnsFindings += 'SPF policy not strict.' }
}
if ($dmarc.Count -eq 0) { $dnsRisk += 30; $dnsFindings += 'DMARC record missing.' }
else {
  $dmarcJoined = ($dmarc -join ' ').ToLower()
  if ($dmarcJoined -match 'p=none') { $dnsRisk += 15; $dnsFindings += 'DMARC policy p=none.' }
}
if ($dkimFound.Count -eq 0) { $dnsRisk += 15; $dnsFindings += 'DKIM not found for default selectors.' }
if ($mtaSts.Count -eq 0) { $dnsRisk += 5; $dnsFindings += 'MTA-STS record missing.' }
if ($tlsRpt.Count -eq 0) { $dnsRisk += 5; $dnsFindings += 'TLS-RPT record missing.' }
if ($dnsRisk -gt 100) { $dnsRisk = 100 }

$scriptsRisk = 0; $scriptFindings = @()
if ($externalScriptsTotal -gt 0) { $scriptsRisk += [math]::Min(25,$externalScriptsTotal * 2) }
if ($externalScriptsNoSRI -gt 0) { $scriptsRisk += [math]::Min(35,$externalScriptsNoSRI * 3); $scriptFindings += "External scripts without SRI: $externalScriptsNoSRI/$externalScriptsTotal." }
if ($outdatedJquery.Count -gt 0) { $scriptsRisk += 30; $scriptFindings += "Outdated jQuery versions: $($outdatedJquery -join ', ')." }
if ($scriptsRisk -gt 100) { $scriptsRisk = 100 }

$repRisk = 0; $repNotTested = $true
$repFindings = @()
if ($archiveSampleCount -gt 0) { $repFindings += "Wayback snapshots found: $archiveSampleCount." }
elseif ($archiveError) { $repFindings += "Archive check failed: $archiveError" }
$repFindings += 'Google Safe Browsing / VirusTotal API checks are not configured.'

$qualityRisk = 0; $qualityFindings = @()
if ($robots.status -ne 200) { $qualityRisk += 20; $qualityFindings += 'robots.txt is missing or inaccessible.' }
if ($sitemap.status -ne 200) { $qualityRisk += 20; $qualityFindings += 'sitemap.xml is missing or inaccessible.' }
if ($soft404Likely) { $qualityRisk += 30; $qualityFindings += 'Soft-404 behavior detected on random URL.' }
if ($qualityRisk -gt 100) { $qualityRisk = 100 }

$legal152Risk = 0; $legalFindings = @()
if ($formPages.Count -gt 0 -and $policyUrls.Count -eq 0) { $legal152Risk += 35; $legalFindings += 'No clear privacy-policy URL detected on crawled pages with forms.' }
if ($formPages.Count -gt 0 -and $policy152Pages.Count -eq 0) { $legal152Risk += 25; $legalFindings += 'No explicit 152-FZ/personal-data markers detected.' }
if ($formPages.Count -gt 0 -and $formPagesWithoutConsent.Count -gt 0) { $legal152Risk += 20; $legalFindings += 'Consent markers missing on some form pages.' }
if ($legal152Risk -gt 100) { $legal152Risk = 100 }
$offerMap = @{
  ssl_redirects = 'Visible immediately and easy to explain.'
  file_leaks = 'Strong urgency when exposure is proven.'
  open_admin = 'Easy to demonstrate to non-technical owner.'
  security_headers = 'Can be benchmarked with a clear technical grade.'
  forms_patient_data = 'Direct legal and trust-risk narrative.'
  dns_email_protection = 'Strong infrastructure credibility for demo.'
  third_party_scripts = 'Advanced supply-chain risk layer.'
  reputation_osint = 'External trust context.'
}

$blocks = @(
  [pscustomobject]@{ id='ssl_redirects'; name='SSL + redirects'; priority=1; score=[math]::Round($sslRisk,1); status=(StatusByScore $sslRisk $false); free_offer_visible=$true; offer_reason=$offerMap.ssl_redirects; findings_count=$sslFindings.Count; key_finding=$(if($sslFindings.Count){$sslFindings[0]}else{'No major SSL/redirect issues on tested hosts.'}) },
  [pscustomobject]@{ id='file_leaks'; name='File leaks'; priority=2; score=[math]::Round($fileRisk,1); status=(StatusByScore $fileRisk $false); free_offer_visible=$true; offer_reason=$offerMap.file_leaks; findings_count=$fileFindings.Count; key_finding=$(if($fileFindings.Count){$fileFindings[0]}else{'No sensitive file exposure in tested dictionary.'}) },
  [pscustomobject]@{ id='open_admin'; name='Open admin and CMS exposure'; priority=3; score=[math]::Round($openAdminRisk,1); status=(StatusByScore $openAdminRisk $false); free_offer_visible=$true; offer_reason=$offerMap.open_admin; findings_count=$openAdminFindings.Count; key_finding=$(if($openAdminFindings.Count){$openAdminFindings[0]}else{'No strong admin/CMS exposure on tested paths.'}) },
  [pscustomobject]@{ id='security_headers'; name='Security headers'; priority=4; score=[math]::Round($headersRisk,1); status=(StatusByScore $headersRisk $false); free_offer_visible=$true; offer_reason=$offerMap.security_headers; findings_count=$headerFindings.Count; key_finding=$(if($headerFindings.Count){$headerFindings[0]}else{'Security headers baseline looks acceptable.'}) },
  [pscustomobject]@{ id='forms_patient_data'; name='Forms and patient data'; priority=5; score=[math]::Round($formsRisk,1); status=(StatusByScore $formsRisk $formsNotTested); free_offer_visible=$true; offer_reason=$offerMap.forms_patient_data; findings_count=$formsFindings.Count; key_finding=$(if($formsNotTested){'No forms found on crawled pages.'}elseif($formsFindings.Count){$formsFindings[0]}else{'No obvious transport/privacy issue on crawled forms.'}) },
  [pscustomobject]@{ id='dns_email_protection'; name='DNS and email protection'; priority=6; score=[math]::Round($dnsRisk,1); status=(StatusByScore $dnsRisk $false); free_offer_visible=$false; offer_reason=$offerMap.dns_email_protection; findings_count=$dnsFindings.Count; key_finding=$(if($dnsFindings.Count){$dnsFindings[0]}else{'DNS/email protection baseline looks acceptable.'}) },
  [pscustomobject]@{ id='third_party_scripts'; name='Third-party scripts'; priority=7; score=[math]::Round($scriptsRisk,1); status=(StatusByScore $scriptsRisk $false); free_offer_visible=$false; offer_reason=$offerMap.third_party_scripts; findings_count=$scriptFindings.Count; key_finding=$(if($scriptFindings.Count){$scriptFindings[0]}else{'No major script-chain issues detected.'}) },
  [pscustomobject]@{ id='reputation_osint'; name='Reputation and OSINT'; priority=8; score=[math]::Round($repRisk,1); status=(StatusByScore $repRisk $repNotTested); free_offer_visible=$false; offer_reason=$offerMap.reputation_osint; findings_count=$repFindings.Count; key_finding=$(if($repFindings.Count){$repFindings[0]}else{'No external reputation flags in tested sources.'}) }
)

$weighted = 0.0; $wTotal = 0.0
foreach ($b in $blocks) {
  $w = 1.0
  if ($b.priority -le 3) { $w = 1.5 } elseif ($b.priority -le 5) { $w = 1.2 }
  $weighted += ($b.score * $w)
  $wTotal += $w
}
$overall = if ($wTotal -gt 0) { [math]::Round($weighted / $wTotal, 1) } else { 0.0 }
$fullPlusScore = [math]::Round(($overall * 0.8) + ($qualityRisk * 0.1) + ($legal152Risk * 0.1), 1)
$riskBand = RiskBand $overall
$riskBandFull = RiskBand $fullPlusScore

$freeSummary = @()
foreach ($b in ($blocks | Where-Object { $_.free_offer_visible } | Sort-Object priority)) {
  if ($b.status -eq 'warning' -or $b.status -eq 'critical') { $freeSummary += "$($b.name): $($b.key_finding)" }
}
$freeSummary = $freeSummary | Select-Object -First 5

$fullAudit = [ordered]@{
  site_id = $siteId
  domain = $Domain
  audit_timestamp_utc = $timestamp
  mode = 'full'
  crawl_pages_tested = $pages.Count
  overall_score = $overall
  overall_risk_band = $riskBand
  full_plus_score = $fullPlusScore
  full_plus_risk_band = $riskBandFull
  free_offer_summary = $freeSummary
  blocks = $blocks
  extra_full_markers = [ordered]@{
    compliance_152fz = [ordered]@{ score = $legal152Risk; status = (StatusByScore $legal152Risk ($formPages.Count -eq 0)); findings = $legalFindings; policy_urls = $policyUrls; form_pages = $formPages }
    site_quality = [ordered]@{ score = $qualityRisk; status = (StatusByScore $qualityRisk $false); findings = $qualityFindings; robots_status = $robots.status; sitemap_status = $sitemap.status; soft404_status = $soft404.status; soft404_likely = $soft404Likely }
  }
}

$evidence = [ordered]@{
  site_id = $siteId
  domain = $Domain
  audit_timestamp_utc = $timestamp
  transport = [ordered]@{ https_home_status = $homeResp.status; http_home_status = $httpHome.status; http_redirect_location = $location; redirect_to_https = $redirectOk; hsts = $hsts; tls = $tls; tls_www = $wwwTls; www_https_status = $wwwHttps.status; www_http_status = $wwwHttp.status; mixed_content_count = $mixedContent }
  crawl = [ordered]@{ pages_tested = $pages | Select-Object url,status,title }
  file_checks = $fileChecks
  cms_checks = [ordered]@{ cms = $cms; generator = $generator; wp_version = $wpVersion; wp_admin_status = $wpAdmin.status; wp_login_status = $wpLogin.status; bitrix_admin_status = $bitrixAdmin.status; admin_status = $adminGeneric.status }
  header_checks = [ordered]@{ missing_headers = $missingHeaders; server = $serverHeader; x_powered_by = $xPoweredBy }
  forms_checks = [ordered]@{ forms_total = $formsTotal; form_pages = $formPages; form_pages_without_privacy = $formPagesWithoutPrivacy; form_pages_without_consent = $formPagesWithoutConsent; http_form_actions = $httpFormActions; captcha_pages = $captchaPages; honeypot_pages = $honeypotPages; cookie_notice_pages = $cookieNoticePages; privacy_pages = $privacyPages; policy_152_pages = $policy152Pages; policy_urls = $policyUrls }
  dns_checks = [ordered]@{ spf = $spf; dmarc = $dmarc; dkim_found = $dkimFound; mta_sts = $mtaSts; tls_rpt = $tlsRpt }
  script_checks = [ordered]@{ external_script_domains = $externalScriptDomains; external_scripts_total = $externalScriptsTotal; external_scripts_without_sri = $externalScriptsNoSRI; outdated_jquery_versions = $outdatedJquery }
  quality_checks = [ordered]@{ robots_status = $robots.status; sitemap_status = $sitemap.status; soft404_status = $soft404.status; soft404_title = $softTitle; soft404_likely = $soft404Likely }
  reputation_checks = [ordered]@{ archive_sample_count = $archiveSampleCount; archive_error = $archiveError }
}

$fullAudit | ConvertTo-Json -Depth 30 | Set-Content -Path $normalizedPath -Encoding UTF8
$fullAudit | ConvertTo-Json -Depth 30 | Set-Content -Path $fullPath -Encoding UTF8
$evidence | ConvertTo-Json -Depth 30 | Set-Content -Path $evidencePath -Encoding UTF8

$quickPath = "data/normalized/$siteId.json"
$quick = $null
if (Test-Path $quickPath) { try { $quick = Get-Content -Raw $quickPath | ConvertFrom-Json } catch {} }

$cmp = @()
$cmp += "# Quick vs Full: $Domain"
$cmp += ''
$cmp += "- Full audit timestamp (UTC): $timestamp"
if ($quick) { $cmp += "- Quick score: $($quick.overall_score) ($($quick.risk_band))" }
$cmp += "- Full score (base blocks): $overall ($riskBand)"
$cmp += "- Full+ score (with compliance/quality markers): $fullPlusScore ($riskBandFull)"
$cmp += ''
$cmp += '## New Coverage In Full'
$cmp += "- Multi-page crawl count: $($pages.Count)"
$cmp += '- 152-FZ/compliance marker set enabled'
$cmp += '- robots/sitemap/soft-404 markers enabled'
$cmp += '- www-host TLS and redirect checks enabled'
$cmp += '- Expanded DNS email set includes MTA-STS/TLS-RPT'
$cmp += ''
$cmp += '## Key Full Findings'
foreach ($b in ($blocks | Sort-Object priority)) { $cmp += "- [$($b.id)] score $($b.score), status $($b.status): $($b.key_finding)" }
$cmp += "- [compliance_152fz] score ${legal152Risk}: $(if($legalFindings.Count){$legalFindings[0]}else{'No explicit compliance marker issues on crawled pages.'})"
$cmp += "- [site_quality] score ${qualityRisk}: $(if($qualityFindings.Count){$qualityFindings[0]}else{'No major robots/sitemap/soft-404 issues detected.'})"
$cmp += ''
$cmp += '## Evidence Files'
$cmp += "- Normalized full: $normalizedPath"
$cmp += "- Raw evidence full: $evidencePath"
$cmp += "- Full audit copy: $fullPath"
$cmp -join "`r`n" | Set-Content -Path $comparisonPath -Encoding UTF8

Write-Output "Full audit complete for $Domain"
Write-Output "Full normalized: $normalizedPath"
Write-Output "Full evidence: $evidencePath"
Write-Output "Comparison: $comparisonPath"
Write-Output "Overall base score: $overall ($riskBand)"
Write-Output "Overall full+ score: $fullPlusScore ($riskBandFull)"




