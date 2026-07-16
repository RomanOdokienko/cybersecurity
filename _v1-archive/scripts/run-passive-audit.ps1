param(
  [Parameter(Mandatory = $true)]
  [string]$Domain
)

$ErrorActionPreference = "Stop"

[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.SecurityProtocolType]::Tls13

function Get-SiteId([string]$inputDomain) {
  return $inputDomain.ToLower().Replace("https://", "").Replace("http://", "").Replace("/", "_").Replace(".", "_")
}

function Convert-HeadersToMap($headers) {
  $map = @{}
  if ($null -eq $headers) { return $map }

  if ($headers -is [System.Collections.IDictionary]) {
    foreach ($k in $headers.Keys) { $map["$k"] = "$($headers[$k])" }
    return $map
  }

  try {
    foreach ($k in $headers.AllKeys) { $map["$k"] = "$($headers[$k])" }
  } catch {}
  return $map
}

function Get-HeaderValue($headers, [string]$name) {
  if ($null -eq $headers) { return $null }
  foreach ($k in $headers.Keys) {
    if ("$k".ToLower() -eq $name.ToLower()) { return "$($headers[$k])" }
  }
  return $null
}

function Invoke-SafeRequest {
  param(
    [Parameter(Mandatory = $true)][string]$Url,
    [string]$Method = "GET",
    [int]$MaxRedirect = 5,
    [int]$TimeoutSec = 20
  )

  $requestHeaders = @{ "User-Agent" = "ClinicAuditBot/1.0 (passive-audit)" }

  try {
    $resp = Invoke-WebRequest -Uri $Url -Method $Method -MaximumRedirection $MaxRedirect -TimeoutSec $TimeoutSec -Headers $requestHeaders -ErrorAction Stop
    $headers = Convert-HeadersToMap $resp.Headers
    return [pscustomobject]@{
      url = $Url
      status = [int]$resp.StatusCode
      status_description = "$($resp.StatusDescription)"
      headers = $headers
      final_url = if ($resp.BaseResponse -and $resp.BaseResponse.ResponseUri) { $resp.BaseResponse.ResponseUri.AbsoluteUri } else { $Url }
      body = "$($resp.Content)"
      error = $null
    }
  } catch {
    $ex = $_.Exception
    $responseObj = $null
    try { $responseObj = $ex.Response } catch {}
    if ($responseObj) {
      $r = $responseObj
      $headers = Convert-HeadersToMap $r.Headers
      $status = 0
      $statusDesc = "request_failed"
      $finalUrl = $Url
      $bodyText = ""

      try { $status = [int]$r.StatusCode } catch {}
      try { $statusDesc = "$($r.StatusDescription)" } catch {}
      try { $finalUrl = $r.ResponseUri.AbsoluteUri } catch {}

      return [pscustomobject]@{
        url = $Url
        status = $status
        status_description = $statusDesc
        headers = $headers
        final_url = $finalUrl
        body = $bodyText
        error = "$($ex.Message)"
      }
    }

    return [pscustomobject]@{
      url = $Url
      status = 0
      status_description = "request_failed"
      headers = @{}
      final_url = $Url
      body = ""
      error = "$($ex.Message)"
    }
  }
}

function Resolve-TxtRecords([string]$Name) {
  try {
    $records = Resolve-DnsName -Name $Name -Type TXT -QuickTimeout -ErrorAction Stop
    $out = @()
    foreach ($r in $records) {
      if ($r.Strings) { $out += ($r.Strings -join "") }
    }
    return $out
  } catch {
    return @()
  }
}

function Get-TlsCertInfo([string]$TargetHost, [int]$Port = 443) {
  $tcp = $null
  $ssl = $null

  try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $connectResult = $tcp.BeginConnect($TargetHost, $Port, $null, $null)
    if (-not $connectResult.AsyncWaitHandle.WaitOne(7000, $false)) {
      throw "TCP connect timeout for ${TargetHost}:$Port"
    }
    $tcp.EndConnect($connectResult)
    $ssl = New-Object System.Net.Security.SslStream($tcp.GetStream(), $false, ({ $true }))
    $ssl.AuthenticateAsClient($TargetHost)
    $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 $ssl.RemoteCertificate
    $now = (Get-Date).ToUniversalTime()
    return [pscustomobject]@{
      subject = $cert.Subject
      issuer = $cert.Issuer
      not_before = $cert.NotBefore.ToUniversalTime().ToString("o")
      not_after = $cert.NotAfter.ToUniversalTime().ToString("o")
      days_to_expiry = [math]::Floor(($cert.NotAfter.ToUniversalTime() - $now).TotalDays)
      signature_algorithm = $cert.SignatureAlgorithm.FriendlyName
      valid_now = ($now -gt $cert.NotBefore.ToUniversalTime() -and $now -lt $cert.NotAfter.ToUniversalTime())
      thumbprint = $cert.Thumbprint
    }
  } catch {
    return [pscustomobject]@{
      valid_now = $false
      error = "$($_.Exception.Message)"
    }
  } finally {
    if ($ssl) { $ssl.Dispose() }
    if ($tcp) { $tcp.Close() }
  }
}

function Get-RiskBand([double]$Score) {
  if ($Score -ge 80) { return "critical" }
  if ($Score -ge 60) { return "high" }
  if ($Score -ge 35) { return "medium" }
  return "low"
}

function Get-StatusFromScore([double]$Score, [bool]$NotTested) {
  if ($NotTested) { return "not_tested" }
  if ($Score -ge 70) { return "critical" }
  if ($Score -ge 35) { return "warning" }
  return "ok"
}

$siteId = Get-SiteId $Domain
$rawDir = "data/raw/$siteId"
$normalizedPath = "data/normalized/$siteId.json"
$rawEvidencePath = "$rawDir/evidence.json"
$timestampUtc = (Get-Date).ToUniversalTime().ToString("o")

New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

$httpsHome = Invoke-SafeRequest -Url "https://$Domain/" -Method "GET" -MaxRedirect 5 -TimeoutSec 12
$httpHomeNoRedirect = Invoke-SafeRequest -Url "http://$Domain/" -Method "GET" -MaxRedirect 0 -TimeoutSec 12
$tlsInfo = Get-TlsCertInfo -TargetHost $Domain -Port 443

$httpsHeaders = $httpsHome.headers
$hstsValue = Get-HeaderValue $httpsHeaders "Strict-Transport-Security"

$httpLocation = Get-HeaderValue $httpHomeNoRedirect.headers "Location"
$redirectStatuses = @(301, 302, 307, 308)
$redirectToHttps = ($redirectStatuses -contains $httpHomeNoRedirect.status) -and ($httpLocation -and $httpLocation.ToLower().StartsWith("https://"))

$mixedContentCount = 0
if ($httpsHome.body) {
  $mixedContentCount = ([regex]::Matches($httpsHome.body, "(?i)(src|href)\s*=\s*[''`"]http://[^''`"\s>]+" )).Count
}

$sslRisk = 0
$sslFindings = @()
if (-not $tlsInfo.valid_now) {
  $sslRisk += 45
  $sslFindings += "TLS certificate is not currently valid."
}

if ($tlsInfo.days_to_expiry -is [int]) {
  if ($tlsInfo.days_to_expiry -lt 30) {
    $sslRisk += 25
    $sslFindings += "Certificate expires in less than 30 days ($($tlsInfo.days_to_expiry))."
  } elseif ($tlsInfo.days_to_expiry -lt 90) {
    $sslRisk += 10
  }
}

if (-not $redirectToHttps) {
  $sslRisk += 25
  $sslFindings += "HTTP to HTTPS redirect is not strictly enforced."
}

if (-not $hstsValue) {
  $sslRisk += 15
  $sslFindings += "HSTS header is missing."
}

if ($mixedContentCount -gt 0) {
  $sslRisk += 15
  $sslFindings += "Mixed-content references detected: $mixedContentCount."
}

if ($sslRisk -gt 100) { $sslRisk = 100 }

$pathDict = Get-Content "config/path_dictionary.txt" | Where-Object { $_ -and -not $_.StartsWith("#") }
$pathChecks = @()
$exposedSensitive = @()
$dirListingHits = @()

foreach ($path in $pathDict) {
  $url = "https://$Domain$path"
  $resp = Invoke-SafeRequest -Url $url -Method "HEAD" -MaxRedirect 2 -TimeoutSec 5
  $pathChecks += [pscustomobject]@{
    path = $path
    status = $resp.status
    final_url = $resp.final_url
  }

  $isDirectory = $path.EndsWith("/")
  if (-not $isDirectory -and @((200), (206)) -contains $resp.status) {
    $exposedSensitive += $path
  }

  if ($isDirectory -and $resp.status -eq 200) {
    $dirResp = Invoke-SafeRequest -Url $url -Method "GET" -MaxRedirect 2 -TimeoutSec 7
    if ($dirResp.body -match "(?i)index of\s*/|directory listing|parent directory") {
      $dirListingHits += $path
    }
  }
}

$fileLeakRisk = 0
$fileLeakFindings = @()
if ($exposedSensitive.Count -gt 0) {
  $fileLeakRisk += [math]::Min(100, 35 + ($exposedSensitive.Count * 20))
  $fileLeakFindings += "Sensitive paths exposed: $($exposedSensitive -join ', ')."
}
if ($dirListingHits.Count -gt 0) {
  $fileLeakRisk += [math]::Min(40, $dirListingHits.Count * 20)
  $fileLeakFindings += "Directory listing indicators: $($dirListingHits -join ', ')."
}
if ($fileLeakRisk -gt 100) { $fileLeakRisk = 100 }

$wpAdmin = Invoke-SafeRequest -Url "https://$Domain/wp-admin/" -Method "HEAD" -MaxRedirect 0 -TimeoutSec 8
$wpLogin = Invoke-SafeRequest -Url "https://$Domain/wp-login.php" -Method "HEAD" -MaxRedirect 0 -TimeoutSec 8
$bitrixAdmin = Invoke-SafeRequest -Url "https://$Domain/bitrix/admin/" -Method "HEAD" -MaxRedirect 0 -TimeoutSec 8
$readmeResp = Invoke-SafeRequest -Url "https://$Domain/readme.html" -Method "GET" -MaxRedirect 2 -TimeoutSec 8
$feedResp = Invoke-SafeRequest -Url "https://$Domain/feed" -Method "GET" -MaxRedirect 2 -TimeoutSec 8

$cms = "unknown"
$generatorMeta = $null
if ($httpsHome.body -match "(?i)<meta[^>]+name=[''`"]generator[''`"][^>]+content=[''`"]([^''`"]+)[''`"]") {
  $generatorMeta = $Matches[1]
}
if ($httpsHome.body -match "(?i)wp-content|wp-includes|wordpress") { $cms = "wordpress" }
if ($httpsHome.body -match "(?i)bitrix") { $cms = "bitrix" }
if ($generatorMeta -and $cms -eq "unknown") { $cms = $generatorMeta }

$wpVersion = $null
if ($readmeResp.body -match '(?i)version\s+([0-9]+\.[0-9]+(\.[0-9]+)?)') {
  $wpVersion = $Matches[1]
}
if (-not $wpVersion -and $feedResp.body -match '(?i)wordpress\.org\/\?v=([0-9]+\.[0-9]+(\.[0-9]+)?)') {
  $wpVersion = $Matches[1]
}

$openAdminRisk = 0
$openAdminFindings = @()

$adminNoRedirect = @()
if ($wpAdmin.status -eq 200) { $adminNoRedirect += "/wp-admin/" }
if ($bitrixAdmin.status -eq 200) { $adminNoRedirect += "/bitrix/admin/" }
if ($adminNoRedirect.Count -gt 0) {
  $openAdminRisk += 30
  $openAdminFindings += "Admin endpoints reachable without redirect: $($adminNoRedirect -join ', ')."
}

if ($wpVersion) {
  $openAdminRisk += 20
  $openAdminFindings += "Public WordPress version leakage: $wpVersion."
}

if ($openAdminRisk -gt 100) { $openAdminRisk = 100 }

$requiredHeaders = @(
  "Strict-Transport-Security",
  "Content-Security-Policy",
  "X-Frame-Options",
  "X-Content-Type-Options",
  "Referrer-Policy",
  "Permissions-Policy"
)

$missingHeaders = @()
foreach ($h in $requiredHeaders) {
  if (-not (Get-HeaderValue $httpsHeaders $h)) { $missingHeaders += $h }
}

$serverHeader = Get-HeaderValue $httpsHeaders "Server"
$xPoweredBy = Get-HeaderValue $httpsHeaders "X-Powered-By"

$headersRisk = 0
$headerFindings = @()
if ($missingHeaders.Count -gt 0) {
  $headersRisk += [math]::Min(70, $missingHeaders.Count * 12)
  $headerFindings += "Missing security headers: $($missingHeaders -join ', ')."
}
if ($serverHeader) {
  $headersRisk += 10
  $headerFindings += "Server header discloses stack details."
}
if ($xPoweredBy) {
  $headersRisk += 10
  $headerFindings += "X-Powered-By header discloses technology."
}
if ($headersRisk -gt 100) { $headersRisk = 100 }

$formsRisk = 0
$formsFindings = @()
$formsCount = 0
$formActions = @()
  $hasCaptcha = $httpsHome.body -match '(?i)recaptcha|hcaptcha|captcha'
  $hasHoneypot = $httpsHome.body -match '(?i)honeypot|_gotcha|name=[''\"]website[''\"]'
  $hasPrivacy = $httpsHome.body -match '(?i)privacy|policy|consent'
  $hasCookieBanner = $httpsHome.body -match '(?i)cookie'
$formsNotTested = $false

if ($httpsHome.body) {
  $formsCount = ([regex]::Matches($httpsHome.body, "(?is)<form\b[^>]*>")).Count
  $actionMatches = [regex]::Matches($httpsHome.body, "(?is)<form\b[^>]*action=[''`"]([^''`"]+)[''`"]")
  foreach ($m in $actionMatches) { $formActions += $m.Groups[1].Value }

  $hasCaptcha = $httpsHome.body -match '(?i)recaptcha|hcaptcha|captcha'
  $hasHoneypot = $httpsHome.body -match "(?i)honeypot|_gotcha|name=[''`"]website[''`"]"
  $hasPrivacy = $httpsHome.body -match '(?i)privacy|policy|consent'
  $hasCookieBanner = $httpsHome.body -match '(?i)cookie'
} else {
  $formsNotTested = $true
}

if (-not $formsNotTested -and $formsCount -gt 0) {
  $badActions = @($formActions | Where-Object { $_.ToLower().StartsWith("http://") })
  if ($badActions.Count -gt 0) {
    $formsRisk += 45
    $formsFindings += "Form action over HTTP detected."
  }
  if (-not $hasPrivacy) {
    $formsRisk += 20
    $formsFindings += "Privacy policy link not detected on main page."
  }
  if (-not ($hasCaptcha -or $hasHoneypot)) {
    $formsRisk += 15
    $formsFindings += "CAPTCHA/honeypot control not detected."
  }
  if (-not $hasCookieBanner) {
    $formsRisk += 10
    $formsFindings += "Cookie notice not detected."
  }
}

if (-not $formsNotTested -and $formsCount -eq 0) {
  $formsNotTested = $true
}

if ($formsRisk -gt 100) { $formsRisk = 100 }

$spfRecords = Resolve-TxtRecords $Domain | Where-Object { $_ -match "(?i)^v=spf1" }
$dmarcRecords = Resolve-TxtRecords "_dmarc.$Domain"
$dkimSelectors = @("default")
$dkimFound = @()
foreach ($sel in $dkimSelectors) {
  $r = Resolve-TxtRecords "$sel._domainkey.$Domain"
  if ($r.Count -gt 0) { $dkimFound += $sel }
}
$mtaStsRecords = @()
$tlsRptRecords = @()

$dnsRisk = 0
$dnsFindings = @()
if ($spfRecords.Count -eq 0) {
  $dnsRisk += 25
  $dnsFindings += "SPF record missing."
} else {
  $spfJoined = ($spfRecords -join " ").ToLower()
  if ($spfJoined -match "~all") {
    $dnsRisk += 10
    $dnsFindings += "SPF policy is softfail (~all)."
  } elseif ($spfJoined -notmatch "-all") {
    $dnsRisk += 15
    $dnsFindings += "SPF policy is not strict."
  }
}

if ($dmarcRecords.Count -eq 0) {
  $dnsRisk += 30
  $dnsFindings += "DMARC record missing."
} else {
  $dmarcJoined = ($dmarcRecords -join " ").ToLower()
  if ($dmarcJoined -match "p=none") {
    $dnsRisk += 15
    $dnsFindings += "DMARC policy is p=none."
  }
}

if ($dkimFound.Count -eq 0) {
  $dnsRisk += 15
  $dnsFindings += "No DKIM records on standard selectors."
}
if ($dnsRisk -gt 100) { $dnsRisk = 100 }

$scriptRisk = 0
$scriptFindings = @()
$externalScriptDomains = @()
$externalScriptsTotal = 0
$externalScriptsNoSRI = 0
$outdatedJquery = @()

if ($httpsHome.body) {
  $scriptMatches = [regex]::Matches($httpsHome.body, "(?is)<script\b[^>]*src=[''`"]([^''`"]+)[''`"][^>]*>")
  foreach ($m in $scriptMatches) {
    $tag = $m.Value
    $src = $m.Groups[1].Value
    $resolvedSrc = $src
    if ($resolvedSrc.StartsWith("//")) { $resolvedSrc = "https:$resolvedSrc" }
    $hasIntegrity = $tag -match '(?i)\sintegrity\s*='

    $host = $null
    try {
      if ($resolvedSrc.StartsWith("http://") -or $resolvedSrc.StartsWith("https://")) {
        $uri = [Uri]$resolvedSrc
        $host = $uri.Host.ToLower()
      }
    } catch {}

    if ($host -and ($host -ne $Domain.ToLower()) -and (-not $host.EndsWith(".$($Domain.ToLower())"))) {
      $externalScriptsTotal += 1
      $externalScriptDomains += $host
      if (-not $hasIntegrity) { $externalScriptsNoSRI += 1 }
    }

    if ($resolvedSrc -match '(?i)jquery[^0-9]*([0-9]+\.[0-9]+(\.[0-9]+)?)') {
      $version = $Matches[1]
      $parts = $version.Split(".")
      $major = [int]$parts[0]
      $minor = if ($parts.Count -ge 2) { [int]$parts[1] } else { 0 }
      if ($major -eq 1 -and $minor -lt 12) { $outdatedJquery += $version }
      if ($major -eq 2) { $outdatedJquery += $version }
    }
  }
}

$externalScriptDomains = $externalScriptDomains | Sort-Object -Unique
if ($externalScriptsTotal -gt 0) {
  $scriptRisk += [math]::Min(20, $externalScriptsTotal * 2)
  if ($externalScriptsNoSRI -gt 0) {
    $scriptRisk += [math]::Min(30, $externalScriptsNoSRI * 5)
    $scriptFindings += "External scripts without SRI: $externalScriptsNoSRI of $externalScriptsTotal."
  }
}
if ($outdatedJquery.Count -gt 0) {
  $scriptRisk += 30
  $scriptFindings += "Outdated jQuery indicators: $($outdatedJquery -join ', ')."
}
if ($scriptRisk -gt 100) { $scriptRisk = 100 }

$reputationRisk = 0
$reputationNotTested = $true
$reputationFindings = @(
  "Reputation sources requiring API keys are not configured in this run."
)

$offerReasonMap = @{
  "ssl_redirects" = "Visible immediately and easy to explain to non-technical owners."
  "file_leaks" = "High-impact exposure that creates clear urgency."
  "open_admin" = "Admin/CMS exposure is easy to demonstrate."
  "security_headers" = "Can be benchmarked quickly with a clear score."
  "forms_patient_data" = "Maps directly to legal and trust risks."
  "dns_email_protection" = "Less visible, but strong technical credibility in demo."
  "third_party_scripts" = "Adds advanced depth and supply-chain narrative."
  "reputation_osint" = "External trust signals strengthen the risk narrative."
}

$blocks = @(
  [pscustomobject]@{
    id = "ssl_redirects"
    name = "SSL + redirects"
    priority = 1
    score = [math]::Round($sslRisk, 1)
    status = Get-StatusFromScore -Score $sslRisk -NotTested $false
    free_offer_visible = $true
    offer_reason = $offerReasonMap["ssl_redirects"]
    findings_count = $sslFindings.Count
    key_finding = if ($sslFindings.Count -gt 0) { $sslFindings[0] } else { "No major SSL/redirect issues detected." }
  },
  [pscustomobject]@{
    id = "file_leaks"
    name = "File leaks"
    priority = 2
    score = [math]::Round($fileLeakRisk, 1)
    status = Get-StatusFromScore -Score $fileLeakRisk -NotTested $false
    free_offer_visible = $true
    offer_reason = $offerReasonMap["file_leaks"]
    findings_count = $fileLeakFindings.Count
    key_finding = if ($fileLeakFindings.Count -gt 0) { $fileLeakFindings[0] } else { "No public sensitive path exposure in the dictionary." }
  },
  [pscustomobject]@{
    id = "open_admin"
    name = "Open admin and CMS exposure"
    priority = 3
    score = [math]::Round($openAdminRisk, 1)
    status = Get-StatusFromScore -Score $openAdminRisk -NotTested $false
    free_offer_visible = $true
    offer_reason = $offerReasonMap["open_admin"]
    findings_count = $openAdminFindings.Count
    key_finding = if ($openAdminFindings.Count -gt 0) { $openAdminFindings[0] } else { "No strong admin/CMS exposure signals on tested paths." }
  },
  [pscustomobject]@{
    id = "security_headers"
    name = "Security headers"
    priority = 4
    score = [math]::Round($headersRisk, 1)
    status = Get-StatusFromScore -Score $headersRisk -NotTested $false
    free_offer_visible = $true
    offer_reason = $offerReasonMap["security_headers"]
    findings_count = $headerFindings.Count
    key_finding = if ($headerFindings.Count -gt 0) { $headerFindings[0] } else { "Required security headers baseline looks acceptable." }
  },
  [pscustomobject]@{
    id = "forms_patient_data"
    name = "Forms and patient data"
    priority = 5
    score = [math]::Round($formsRisk, 1)
    status = Get-StatusFromScore -Score $formsRisk -NotTested $formsNotTested
    free_offer_visible = $true
    offer_reason = $offerReasonMap["forms_patient_data"]
    findings_count = $formsFindings.Count
    key_finding = if ($formsNotTested) { "No forms found on tested page." } elseif ($formsFindings.Count -gt 0) { $formsFindings[0] } else { "No obvious form transport/privacy issues on tested page." }
  },
  [pscustomobject]@{
    id = "dns_email_protection"
    name = "DNS and email protection"
    priority = 6
    score = [math]::Round($dnsRisk, 1)
    status = Get-StatusFromScore -Score $dnsRisk -NotTested $false
    free_offer_visible = $false
    offer_reason = $offerReasonMap["dns_email_protection"]
    findings_count = $dnsFindings.Count
    key_finding = if ($dnsFindings.Count -gt 0) { $dnsFindings[0] } else { "Core DNS/email protection records detected." }
  },
  [pscustomobject]@{
    id = "third_party_scripts"
    name = "Third-party scripts"
    priority = 7
    score = [math]::Round($scriptRisk, 1)
    status = Get-StatusFromScore -Score $scriptRisk -NotTested $false
    free_offer_visible = $false
    offer_reason = $offerReasonMap["third_party_scripts"]
    findings_count = $scriptFindings.Count
    key_finding = if ($scriptFindings.Count -gt 0) { $scriptFindings[0] } else { "No major third-party script risk indicators on tested page." }
  },
  [pscustomobject]@{
    id = "reputation_osint"
    name = "Reputation and OSINT"
    priority = 8
    score = [math]::Round($reputationRisk, 1)
    status = Get-StatusFromScore -Score $reputationRisk -NotTested $reputationNotTested
    free_offer_visible = $false
    offer_reason = $offerReasonMap["reputation_osint"]
    findings_count = $reputationFindings.Count
    key_finding = $reputationFindings[0]
  }
)

$weightedSum = 0.0
$weightTotal = 0.0
foreach ($b in $blocks) {
  $w = 1.0
  if ($b.priority -le 3) { $w = 1.5 } elseif ($b.priority -le 5) { $w = 1.2 }
  $weightedSum += ($b.score * $w)
  $weightTotal += $w
}
$overallScore = if ($weightTotal -gt 0) { [math]::Round($weightedSum / $weightTotal, 1) } else { 0.0 }
$riskBand = Get-RiskBand $overallScore

$freeOfferSummary = @()
foreach ($b in ($blocks | Where-Object { $_.free_offer_visible -eq $true } | Sort-Object priority)) {
  if ($b.status -ne "ok" -and $b.status -ne "not_tested") {
    $freeOfferSummary += "$($b.name): $($b.key_finding)"
  }
}
$freeOfferSummary = $freeOfferSummary | Select-Object -First 5

$normalized = [ordered]@{
  site_id = $siteId
  domain = $Domain
  audit_timestamp_utc = $timestampUtc
  overall_score = $overallScore
  risk_band = $riskBand
  free_offer_summary = $freeOfferSummary
  blocks = $blocks
}

$rawEvidence = [ordered]@{
  site_id = $siteId
  domain = $Domain
  audit_timestamp_utc = $timestampUtc
  transport = @{
    https_home = @{
      status = $httpsHome.status
      final_url = $httpsHome.final_url
      headers = $httpsHome.headers
    }
    http_home_no_redirect = @{
      status = $httpHomeNoRedirect.status
      final_url = $httpHomeNoRedirect.final_url
      headers = $httpHomeNoRedirect.headers
    }
    tls_certificate = $tlsInfo
    mixed_content_count = $mixedContentCount
    redirect_to_https = $redirectToHttps
  }
  file_leak_checks = $pathChecks
  cms_checks = @{
    cms_detected = $cms
    generator_meta = $generatorMeta
    wordpress_version = $wpVersion
    wp_admin_status = $wpAdmin.status
    wp_login_status = $wpLogin.status
    bitrix_admin_status = $bitrixAdmin.status
  }
  header_checks = @{
    missing_headers = $missingHeaders
    server_header = $serverHeader
    x_powered_by = $xPoweredBy
  }
  form_checks = @{
    forms_count = $formsCount
    form_actions = $formActions
    has_privacy_link = $hasPrivacy
    has_captcha = $hasCaptcha
    has_honeypot = $hasHoneypot
    has_cookie_banner = $hasCookieBanner
  }
  dns_checks = @{
    spf = $spfRecords
    dmarc = $dmarcRecords
    dkim_selectors_found = $dkimFound
    mta_sts = $mtaStsRecords
    tls_rpt = $tlsRptRecords
  }
  script_checks = @{
    external_script_domains = $externalScriptDomains
    external_scripts_total = $externalScriptsTotal
    external_scripts_without_sri = $externalScriptsNoSRI
    outdated_jquery_versions = $outdatedJquery
  }
}

$normalized | ConvertTo-Json -Depth 20 | Set-Content -Path $normalizedPath -Encoding UTF8
$rawEvidence | ConvertTo-Json -Depth 20 | Set-Content -Path $rawEvidencePath -Encoding UTF8

Write-Output "Audit complete for $Domain"
Write-Output "Normalized: $normalizedPath"
Write-Output "Evidence: $rawEvidencePath"
Write-Output "Overall score: $overallScore ($riskBand)"
