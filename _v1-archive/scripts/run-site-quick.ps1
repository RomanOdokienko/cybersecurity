param(
  [Parameter(Mandatory = $true)]
  [string]$Domain
)

$ErrorActionPreference = "Stop"
if ($PSVersionTable.PSVersion.Major -ge 7) {
  $PSNativeCommandUseErrorActionPreference = $false
}

function Get-SiteId([string]$inputDomain) {
  return $inputDomain.ToLower().Replace("https://", "").Replace("http://", "").Replace("/", "_").Replace(".", "_")
}

function To-HeaderMap($headers) {
  $map = @{}
  if ($null -eq $headers) { return $map }
  foreach ($k in $headers.Keys) { $map["$k"] = "$($headers[$k])" }
  return $map
}

function Get-Header($map, [string]$name) {
  foreach ($k in $map.Keys) {
    if ($k.ToLower() -eq $name.ToLower()) { return "$($map[$k])" }
  }
  return $null
}

function Invoke-Req {
  param(
    [string]$Url,
    [string]$Method = "GET",
    [int]$TimeoutSec = 8,
    [int]$MaxRedirect = 2,
    [string]$UserAgent = "Mozilla/5.0"
  )

  function Parse-CurlHeaders([string[]]$Lines) {
    $status = 0
    $headers = @{}
    foreach ($line in $Lines) {
      $text = "$line".TrimEnd()
      if ($text -match "^HTTP/\S+\s+(\d{3})") {
        $status = [int]$Matches[1]
        $headers = @{}
        continue
      }
      if ($text -match "^\s*([^:]+):\s*(.*)$") {
        $headers[$Matches[1].Trim()] = $Matches[2].Trim()
      }
    }
    return [pscustomobject]@{
      status = $status
      headers = $headers
    }
  }

  $methodUp = $Method.ToUpper()
  if ($methodUp -eq "HEAD") {
    $args = @("-sS", "-I", "--max-time", "$TimeoutSec", "-A", "$UserAgent")
    if ($MaxRedirect -gt 0) { $args += "-L" }
    $args += $Url
    $raw = $null
    try {
      $raw = & curl.exe @args 2>$null
    } catch {
      $raw = $null
    }
    if ($LASTEXITCODE -ne 0 -or -not $raw) {
      return [pscustomobject]@{
        url = $Url
        status = 0
        headers = @{}
        final_url = $Url
        body = ""
        error = "curl_head_failed"
      }
    }

    $parsed = Parse-CurlHeaders @($raw)
    $location = Get-Header $parsed.headers "Location"
    return [pscustomobject]@{
      url = $Url
      status = $parsed.status
      headers = $parsed.headers
      final_url = if ($location) { $location } else { $Url }
      body = ""
      error = $null
    }
  }

  $head = Invoke-Req -Url $Url -Method "HEAD" -TimeoutSec $TimeoutSec -MaxRedirect $MaxRedirect
  $argsBody = @("-sS", "--max-time", "$TimeoutSec", "-A", "$UserAgent")
  if ($MaxRedirect -gt 0) { $argsBody += "-L" }
  $argsBody += $Url
  $rawBody = $null
  try {
    $rawBody = & curl.exe @argsBody 2>$null
  } catch {
    $rawBody = $null
  }
  $bodyText = if ($rawBody) { ($rawBody -join "`n") } else { "" }

  return [pscustomobject]@{
    url = $Url
    status = $head.status
    headers = $head.headers
    final_url = $head.final_url
    body = $bodyText
    error = if ($LASTEXITCODE -eq 0) { $null } else { "curl_get_failed" }
  }
}

function Invoke-ReqWithRetry {
  param(
    [string]$Url,
    [string]$Method = "GET",
    [int]$TimeoutSec = 8,
    [int]$MaxRedirect = 2
  )

  $userAgents = @(
    "Mozilla/5.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
  )

  $attempts = @()
  $last = $null
  foreach ($ua in $userAgents) {
    $resp = Invoke-Req -Url $Url -Method $Method -TimeoutSec $TimeoutSec -MaxRedirect $MaxRedirect -UserAgent $ua
    $attempts += [pscustomobject]@{
      user_agent = $ua
      status = $resp.status
      error = $resp.error
    }
    $last = $resp
    if ((@(200, 301, 302, 307, 308, 404) -contains $resp.status) -and -not $resp.error) {
      break
    }
  }

  if ($last) {
    $last | Add-Member -MemberType NoteProperty -Name attempts -Value $attempts -Force
  }
  return $last
}

function Resolve-Txt([string]$Name) {
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

function Get-TlsInfo([string]$TargetHost) {
  $tcp = $null
  $ssl = $null
  try {
    $tcp = New-Object System.Net.Sockets.TcpClient
    $ar = $tcp.BeginConnect($TargetHost, 443, $null, $null)
    if (-not $ar.AsyncWaitHandle.WaitOne(6000, $false)) {
      throw "TCP timeout"
    }
    $tcp.EndConnect($ar)
    $ssl = New-Object System.Net.Security.SslStream($tcp.GetStream(), $false, ({ $true }))
    $ssl.AuthenticateAsClient($TargetHost)
    $cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2 $ssl.RemoteCertificate
    $now = (Get-Date).ToUniversalTime()
    return [pscustomobject]@{
      valid_now = ($now -lt $cert.NotAfter.ToUniversalTime() -and $now -gt $cert.NotBefore.ToUniversalTime())
      days_to_expiry = [math]::Floor(($cert.NotAfter.ToUniversalTime() - $now).TotalDays)
      signature_algorithm = $cert.SignatureAlgorithm.FriendlyName
      issuer = $cert.Issuer
      not_after = $cert.NotAfter.ToUniversalTime().ToString("o")
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

function RiskBand([double]$score) {
  if ($score -ge 80) { return "critical" }
  if ($score -ge 60) { return "high" }
  if ($score -ge 35) { return "medium" }
  return "low"
}

function StatusByScore([double]$score, [bool]$notTested) {
  if ($notTested) { return "not_tested" }
  if ($score -ge 70) { return "critical" }
  if ($score -ge 35) { return "warning" }
  return "ok"
}

function Resolve-BlockStatus([double]$score, [string]$coverage) {
  if ($coverage -eq "error") { return "error" }
  if ($coverage -eq "not_tested") { return "not_tested" }
  if ($coverage -eq "review_required") { return "review_required" }
  if ($score -ge 70) { return "critical" }
  if ($score -ge 35) { return "warning" }
  return "ok"
}

function Resolve-HeaderBlockStatus(
  [double]$score,
  [string]$coverage,
  [int]$missingPrimaryCount,
  [bool]$missingHsts,
  [bool]$redirectStrict,
  [bool]$xPoweredByPresent
) {
  if ($coverage -eq "error") { return "error" }
  if ($coverage -eq "not_tested") { return "not_tested" }
  if ($coverage -eq "review_required") { return "review_required" }

  # Keep headers as a benchmark-oriented signal in quick mode.
  # Escalate to critical only on a strong compound transport+hardening gap.
  $criticalCompound = ($missingPrimaryCount -ge 4) -and $missingHsts -and (-not $redirectStrict) -and $xPoweredByPresent
  if ($criticalCompound -or $score -ge 80) { return "critical" }
  if ($score -ge 20) { return "warning" }
  return "ok"
}

$siteId = Get-SiteId $Domain
$timestamp = (Get-Date).ToUniversalTime().ToString("o")
$rawDir = "data/raw/$siteId"
$normalizedPath = "data/normalized/$siteId.json"
$evidencePath = "$rawDir/evidence_quick.json"
New-Item -ItemType Directory -Path $rawDir -Force | Out-Null

$httpsHome = Invoke-ReqWithRetry -Url "https://$Domain/" -Method "GET" -TimeoutSec 10 -MaxRedirect 4
$httpHome = Invoke-ReqWithRetry -Url "http://$Domain/" -Method "GET" -TimeoutSec 10 -MaxRedirect 0
$tlsInfo = Get-TlsInfo -TargetHost $Domain

$httpsHeaders = $httpsHome.headers
$hsts = Get-Header $httpsHeaders "Strict-Transport-Security"
$location = Get-Header $httpHome.headers "Location"
$redirectOk = (@(301, 302, 307, 308) -contains $httpHome.status) -and ($location -and $location.ToLower().StartsWith("https://"))
$mixedContent = 0
if ($httpsHome.body) {
  $mixedContent = ([regex]::Matches($httpsHome.body, "(?i)(src|href)\s*=\s*['`"]http://")).Count
}

$sslRisk = 0
$sslFindings = @()
if (-not $tlsInfo.valid_now) { $sslRisk += 45; $sslFindings += "Certificate is invalid or not retrievable." }
if ($tlsInfo.days_to_expiry -is [int] -and $tlsInfo.days_to_expiry -lt 30) { $sslRisk += 25; $sslFindings += "Certificate expires in less than 30 days." }
if (-not $redirectOk) { $sslRisk += 25; $sslFindings += "HTTP->HTTPS redirect is not strict." }
if (-not $hsts) { $sslRisk += 15; $sslFindings += "HSTS is missing." }
if ($mixedContent -gt 0) { $sslRisk += 15; $sslFindings += "Mixed content detected ($mixedContent)." }
if ($sslRisk -gt 100) { $sslRisk = 100 }

$pathsToCheck = @("/.env", "/.git/config", "/wp-config.php", "/backup.sql", "/phpinfo.php", "/uploads/")
$pathEvidence = @()
$leaked = @()
$dirListing = @()
foreach ($p in $pathsToCheck) {
  $resp = Invoke-ReqWithRetry -Url "https://$Domain$p" -Method "HEAD" -TimeoutSec 4 -MaxRedirect 1
  $pathEvidence += [pscustomobject]@{ path = $p; status = $resp.status; final_url = $resp.final_url }
  if ($p.EndsWith("/") -and $resp.status -eq 200) {
    $dir = Invoke-ReqWithRetry -Url "https://$Domain$p" -Method "GET" -TimeoutSec 4 -MaxRedirect 1
    if ($dir.body -match "(?i)index of|parent directory") { $dirListing += $p }
  }
  if (-not $p.EndsWith("/") -and @((200), (206)) -contains $resp.status) { $leaked += $p }
}
$fileRisk = 0
$fileFindings = @()
if ($leaked.Count -gt 0) { $fileRisk += [math]::Min(100, 40 + ($leaked.Count * 20)); $fileFindings += "Sensitive files exposed: $($leaked -join ', ')." }
if ($dirListing.Count -gt 0) { $fileRisk += [math]::Min(40, $dirListing.Count * 20); $fileFindings += "Directory listing: $($dirListing -join ', ')." }
if ($fileRisk -gt 100) { $fileRisk = 100 }

$wpAdmin = Invoke-ReqWithRetry -Url "https://$Domain/wp-admin/" -Method "HEAD" -TimeoutSec 5 -MaxRedirect 0
$bitrixAdmin = Invoke-ReqWithRetry -Url "https://$Domain/bitrix/admin/" -Method "HEAD" -TimeoutSec 5 -MaxRedirect 0
$readme = Invoke-ReqWithRetry -Url "https://$Domain/readme.html" -Method "GET" -TimeoutSec 5 -MaxRedirect 1
$openAdminRisk = 0
$openAdminFindings = @()
if ($wpAdmin.status -eq 200) { $openAdminRisk += 30; $openAdminFindings += "/wp-admin/ returned 200." }
if ($bitrixAdmin.status -eq 200) { $openAdminRisk += 30; $openAdminFindings += "/bitrix/admin/ returned 200." }
$wpVersion = $null
if ($readme.body -match "(?i)version\s+([0-9]+\.[0-9]+(\.[0-9]+)?)") { $wpVersion = $Matches[1] }
if ($wpVersion) { $openAdminRisk += 20; $openAdminFindings += "WordPress version leakage: $wpVersion." }
if ($openAdminRisk -gt 100) { $openAdminRisk = 100 }

$requiredHeaders = @("Strict-Transport-Security", "Content-Security-Policy", "X-Frame-Options", "X-Content-Type-Options", "Referrer-Policy", "Permissions-Policy")
$primaryHeaders = @("Strict-Transport-Security", "Content-Security-Policy", "X-Frame-Options", "X-Content-Type-Options")
$secondaryHeaders = @("Referrer-Policy", "Permissions-Policy")
$missing = @()
foreach ($h in $requiredHeaders) {
  if (-not (Get-Header $httpsHeaders $h)) { $missing += $h }
}
$missingPrimary = @($missing | Where-Object { $primaryHeaders -contains $_ })
$missingSecondary = @($missing | Where-Object { $secondaryHeaders -contains $_ })
$serverHeader = Get-Header $httpsHeaders "Server"
$xPoweredBy = Get-Header $httpsHeaders "X-Powered-By"
$headerRisk = 0
$headerFindings = @()
if ($missingPrimary.Count -gt 0) { $headerRisk += [math]::Min(48, $missingPrimary.Count * 12) }
if ($missingSecondary.Count -gt 0) { $headerRisk += [math]::Min(8, $missingSecondary.Count * 4) }
if ($missing.Count -gt 0) { $headerFindings += "Missing: $($missing -join ', ')." }
if ($serverHeader) { $headerRisk += 4; $headerFindings += "Server header reveals technology." }
if ($xPoweredBy) { $headerRisk += 8; $headerFindings += "X-Powered-By header reveals technology." }
$xPoweredByPresent = -not [string]::IsNullOrWhiteSpace("$xPoweredBy")
# Transport + hardening coupling signal.
$missingHsts = ($missingPrimary -contains "Strict-Transport-Security")
if ($missingHsts -and (-not $redirectOk)) { $headerRisk += 10; $headerFindings += "HSTS is missing while HTTP->HTTPS redirect is not strict." }
if ($headerRisk -gt 100) { $headerRisk = 100 }

$formsRisk = 0
$formsFindings = @()
$formsCoverage = "normal"
$formsCount = 0
$formsTotalDetected = 0
$formsNonRelevant = 0
$formsWithPolicy = 0
$formsWithRequiredConsent = 0
$formsWithAnyRequiredCheckbox = 0
$formsInsecureAction = 0
$formsNonPost = 0
$formsMethodUnknown = 0
$hasPrivacy = $false
$hasCaptcha = $false
$hasHoneypot = $false
$hasCookie = $false
$formPages = @("/", "/contacts", "/contact", "/zapis", "/appointment", "/feedback", "/callback", "/online")
$formPageEvidence = @()
$policyLinksFound = @()
$pagesAccessible = 0
$pagesBlocked = 0
$formsAnalyzed = 0
$policyKeywordRegex = "(?i)privacy|policy|politik|consent|personal|персонал|политик|соглас"
$formIntentRegex = "(?i)appointment|callback|feedback|contact|consult|request|lead|zapis|obrat|zayav|patient|пациент|прием|запис|обрат|заяв|консульт"

foreach ($path in $formPages) {
  $resp = Invoke-ReqWithRetry -Url "https://$Domain$path" -Method "GET" -TimeoutSec 7 -MaxRedirect 2
  $bodyLen = if ($resp.body) { $resp.body.Length } else { 0 }
  $formPageEvidence += [pscustomobject]@{
    path = $path
    status = $resp.status
    body_length = $bodyLen
    error = $resp.error
    final_url = $resp.final_url
  }

  if ($resp.status -eq 200 -and $resp.body) {
    $pagesAccessible += 1
    $body = $resp.body
    $pageHasPolicyMarker = $body -match $policyKeywordRegex
    $hasPrivacy = $hasPrivacy -or $pageHasPolicyMarker
    $hasCaptcha = $hasCaptcha -or ($body -match "(?i)recaptcha|hcaptcha|captcha")
    $hasHoneypot = $hasHoneypot -or ($body -match "(?i)honeypot|_gotcha")
    $hasCookie = $hasCookie -or ($body -match "(?i)cookie")

    $pagePolicyLinksFound = 0
    $anchors = [regex]::Matches($body, "(?is)<a\b[^>]*href\s*=\s*['`"]([^'`">]+)['`"][^>]*>[\s\S]*?</a>")
    foreach ($a in $anchors) {
      $href = $a.Groups[1].Value
      $anchorText = [regex]::Replace($a.Value, "(?is)<[^>]+>", " ")
      if (-not ("$href $anchorText" -match $policyKeywordRegex)) { continue }
      if ($href.StartsWith("#") -or $href.StartsWith("javascript:")) { continue }
      $candidate = $null
      if ($href.StartsWith("//")) {
        $candidate = "https:$href"
      } elseif ($href.StartsWith("http://") -or $href.StartsWith("https://")) {
        $candidate = $href
      } elseif ($href.StartsWith("/")) {
        $candidate = "https://$Domain$href"
      } else {
        $candidate = "https://$Domain/$href"
      }
      $policyLinksFound += $candidate
      $pagePolicyLinksFound += 1
    }

    $forms = [regex]::Matches($body, "(?is)<form\b[\s\S]*?</form>")
    foreach ($fm in $forms) {
      $form = $fm.Value
      $formsTotalDetected += 1

      $formText = [regex]::Replace($form, "(?is)<[^>]+>", " ")
      $hasPersonalField = $form -match "(?i)type\s*=\s*['`"](?:tel|email|date|datetime-local|number)['`"]|<textarea\b|name\s*=\s*['`"][^'`"]*(?:phone|tel|mobile|email|mail|name|fio|comment|message|question|patient|пациент|имя|телефон|почта)['`"]"
      $hasIntentMarker = ("$formText $form" -match $formIntentRegex)
      $isLikelySearch = ("$formText $form" -match "(?i)\bsearch\b|name\s*=\s*['`"]q['`"]|placeholder\s*=\s*['`"][^'`"]*(?:найти|поиск|search)['`"]")

      if ((-not $hasPersonalField -and -not $hasIntentMarker) -or ($isLikelySearch -and -not $hasIntentMarker -and -not ($form -match $policyKeywordRegex))) {
        $formsNonRelevant += 1
        continue
      }

      $formsAnalyzed += 1
      $formsCount += 1

      if ($form -match "(?is)action\s*=\s*['`"]http://") { $formsInsecureAction += 1 }
      $methodMatch = [regex]::Match($form, "(?is)method\s*=\s*['`"]?([a-z]+)")
      if ($methodMatch.Success) {
        $method = $methodMatch.Groups[1].Value.ToUpper()
        if ($method -ne "POST") { $formsNonPost += 1 }
      } else {
        $formsMethodUnknown += 1
      }

      $formHasPolicy = $form -match $policyKeywordRegex
      if ($formHasPolicy -or $pageHasPolicyMarker -or ($pagePolicyLinksFound -gt 0)) { $formsWithPolicy += 1 }

      $hasConsentCheckbox = $form -match "(?i)type\s*=\s*['`"]checkbox['`"]"
      $hasRequired = $form -match "(?i)\brequired\b|aria-required\s*=\s*['`"]true['`"]|data-tilda-req\s*=\s*['`"]1['`"]"
      $hasConsentContext = ("$formText $form" -match $policyKeywordRegex) -or $pageHasPolicyMarker -or ($pagePolicyLinksFound -gt 0)
      if ($hasConsentCheckbox -and $hasRequired) {
        $formsWithAnyRequiredCheckbox += 1
        if ($hasConsentContext) { $formsWithRequiredConsent += 1 }
      }
    }
  } elseif (@(403, 429, 503) -contains $resp.status) {
    $pagesBlocked += 1
  }
}

$policyLinksFound = $policyLinksFound | Sort-Object -Unique
$fallbackPolicyCandidates = @(
  "https://$Domain/document_politics.php",
  "https://$Domain/politika",
  "https://$Domain/politika-konfidentsialnosti",
  "https://$Domain/privacy",
  "https://$Domain/privacy-policy",
  "https://$Domain/policy",
  "https://$Domain/personal-data",
  "https://$Domain/privacy-policy/"
)
$policyChecks = @()
$policyOkCount = 0
if ($policyLinksFound.Count -eq 0) {
  $policyLinksFound = @($fallbackPolicyCandidates)
} else {
  $policyLinksFound = @((@($policyLinksFound) + @($fallbackPolicyCandidates)) | Sort-Object -Unique)
}
foreach ($u in ($policyLinksFound | Select-Object -First 8)) {
  $pr = Invoke-ReqWithRetry -Url $u -Method "HEAD" -TimeoutSec 6 -MaxRedirect 2
  $policyChecks += [pscustomobject]@{ url = $u; status = $pr.status; final_url = $pr.final_url }
  if ($pr.status -eq 200) { $policyOkCount += 1 }
}

if ($pagesAccessible -eq 0) {
  $formsCoverage = if ($pagesBlocked -gt 0) { "review_required" } else { "not_tested" }
  $formsFindings += "Form pages are not accessible in quick mode."
} elseif ($formsCount -eq 0) {
  $formsCoverage = "review_required"
  if ($formsTotalDetected -gt 0) {
    $formsFindings += "Only utility/non-personal forms were found on sampled pages ($formsTotalDetected total, $formsCount relevant)."
  } else {
    $formsFindings += "No forms found on sampled pages."
  }
}

if ($formsInsecureAction -gt 0) { $formsRisk += 45; $formsFindings += "Forms with insecure HTTP action: $formsInsecureAction." }
if ($formsCount -gt 0 -and $formsWithPolicy -lt $formsCount) {
  $formsRisk += $(if ($policyOkCount -gt 0) { 8 } else { 18 })
  $formsFindings += "Policy marker near form is inconsistent ($formsWithPolicy/$formsCount)."
}
if ($formsCount -gt 0 -and $formsWithRequiredConsent -lt $formsCount) {
  $formsRisk += $(if ($formsWithAnyRequiredCheckbox -gt 0) { 8 } else { 20 })
  $formsFindings += "Required consent checkbox is inconsistent ($formsWithRequiredConsent/$formsCount)."
}
if ($formsCount -gt 0 -and $formsNonPost -gt 0) { $formsRisk += 10; $formsFindings += "Explicit non-POST forms detected: $formsNonPost." }
if ($formsCount -gt 0 -and $formsMethodUnknown -gt 0) { $formsRisk += 5; $formsFindings += "Forms without explicit method attribute: $formsMethodUnknown." }
if ($formsCount -gt 0 -and $policyOkCount -eq 0) { $formsRisk += 12; $formsFindings += "Policy page is not reliably accessible from observed candidates." }
if ($formsCount -gt 0 -and -not ($hasCaptcha -or $hasHoneypot)) { $formsRisk += 5; $formsFindings += "CAPTCHA/honeypot not detected." }
if ($formsCount -gt 0 -and -not $hasCookie) { $formsRisk += 3; $formsFindings += "Cookie notice not detected." }
if ($formsRisk -gt 100) { $formsRisk = 100 }

$compliance152Status = "review_required"
if ($formsCoverage -eq "not_tested") {
  $compliance152Status = "not_observable"
} elseif ($formsCoverage -eq "review_required" -and $formsCount -eq 0) {
  $compliance152Status = "review_required"
} elseif ($formsCount -eq 0) {
  $compliance152Status = "review_required"
} elseif ($formsInsecureAction -gt 0) {
  $compliance152Status = "fail"
} elseif (($formsWithRequiredConsent -lt $formsCount) -and ($formsWithAnyRequiredCheckbox -eq 0) -and ($policyOkCount -eq 0)) {
  $compliance152Status = "fail"
} elseif (($formsWithRequiredConsent -eq $formsCount) -and (($formsWithPolicy -eq $formsCount) -or ($policyOkCount -gt 0)) -and ($formsNonPost -eq 0) -and ($formsMethodUnknown -eq 0)) {
  $compliance152Status = "pass"
} else {
  $compliance152Status = "review_required"
}
if ($compliance152Status -eq "fail" -and $formsRisk -lt 35) { $formsRisk = 35 }

$pageChecksSummary = @()
foreach ($pc in ($formPageEvidence | Select-Object -First 6)) {
  $pageChecksSummary += "$($pc.path) -> $($pc.status)"
}
$policyChecksSummary = @()
foreach ($pk in ($policyChecks | Select-Object -First 4)) {
  $policyChecksSummary += "$($pk.url) -> $($pk.status)"
}

$criterionStatusCoverage = if ($pagesAccessible -ge 2) { "pass" } elseif ($pagesAccessible -eq 1 -or $pagesBlocked -gt 0) { "review_required" } else { "not_observable" }
$criterionStatusFormsDetected = if ($formsCount -gt 0) { "pass" } elseif ($pagesAccessible -gt 0) { "review_required" } else { "not_observable" }
$criterionStatusConsent = if ($formsCount -eq 0) { "not_observable" } elseif ($formsWithRequiredConsent -eq $formsCount) { "pass" } elseif ($formsWithAnyRequiredCheckbox -gt 0) { "review_required" } else { "fail" }
$criterionStatusPolicyNear = if ($formsCount -eq 0) { "not_observable" } elseif ($formsWithPolicy -eq $formsCount) { "pass" } elseif ($policyOkCount -gt 0) { "review_required" } else { "fail" }
$criterionStatusTransport = if ($formsCount -eq 0) { "not_observable" } elseif ($formsInsecureAction -gt 0) { "fail" } else { "pass" }
$criterionStatusMethodPost = if ($formsCount -eq 0) { "not_observable" } elseif ($formsNonPost -gt 0) { "fail" } elseif ($formsMethodUnknown -gt 0) { "review_required" } else { "pass" }
$criterionStatusPolicyReachable = if ($policyChecks.Count -eq 0) { "not_observable" } elseif ($policyOkCount -gt 0) { "pass" } elseif ($pagesBlocked -gt 0) { "review_required" } else { "fail" }

$compliance152Criteria = @(
  [pscustomobject]@{
    id = "pages_accessible"
    title = "Coverage of pages for screening"
    status = $criterionStatusCoverage
    value = "$pagesAccessible/$($formPages.Count)"
    evidence = if ($pageChecksSummary.Count) { $pageChecksSummary -join "; " } else { "No page-check evidence." }
  },
  [pscustomobject]@{
    id = "forms_detected"
    title = "Forms detected"
    status = $criterionStatusFormsDetected
    value = "$formsCount relevant / $formsTotalDetected total"
    evidence = "forms_relevant=$formsCount; forms_total=$formsTotalDetected; forms_non_relevant=$formsNonRelevant"
  },
  [pscustomobject]@{
    id = "required_consent_checkbox"
    title = "Required consent checkbox on forms"
    status = $criterionStatusConsent
    value = "$formsWithRequiredConsent/$formsCount"
    evidence = "forms_with_required_consent=$formsWithRequiredConsent; forms_with_any_required_checkbox=$formsWithAnyRequiredCheckbox; forms_relevant=$formsCount"
  },
  [pscustomobject]@{
    id = "policy_marker_near_form"
    title = "Policy marker near form"
    status = $criterionStatusPolicyNear
    value = "$formsWithPolicy/$formsCount"
    evidence = "forms_with_policy_marker=$formsWithPolicy; forms_total=$formsCount"
  },
  [pscustomobject]@{
    id = "secure_form_action"
    title = "Secure form action (no HTTP)"
    status = $criterionStatusTransport
    value = "$formsInsecureAction insecure"
    evidence = "forms_insecure_action=$formsInsecureAction"
  },
  [pscustomobject]@{
    id = "post_method"
    title = "Form POST method"
    status = $criterionStatusMethodPost
    value = "$formsNonPost non_post; $formsMethodUnknown unknown"
    evidence = "forms_non_post=$formsNonPost; forms_method_unknown=$formsMethodUnknown; forms_relevant=$formsCount"
  },
  [pscustomobject]@{
    id = "policy_page_reachable"
    title = "Policy page reachable"
    status = $criterionStatusPolicyReachable
    value = "$policyOkCount/$($policyChecks.Count)"
    evidence = if ($policyChecksSummary.Count) { $policyChecksSummary -join "; " } else { "No policy-check evidence." }
  }
)

$spf = Resolve-Txt $Domain | Where-Object { $_ -match "(?i)^v=spf1" }
$dmarc = Resolve-Txt "_dmarc.$Domain"
$dkimDefault = Resolve-Txt "default._domainkey.$Domain"
$dnsRisk = 0
$dnsFindings = @()
if ($spf.Count -eq 0) { $dnsRisk += 25; $dnsFindings += "SPF is missing." }
elseif (($spf -join " ").ToLower() -match "~all") { $dnsRisk += 10; $dnsFindings += "SPF is softfail (~all)." }
if ($dmarc.Count -eq 0) { $dnsRisk += 30; $dnsFindings += "DMARC is missing." }
elseif (($dmarc -join " ").ToLower() -match "p=none") { $dnsRisk += 15; $dnsFindings += "DMARC policy is p=none." }
if ($dkimDefault.Count -eq 0) { $dnsRisk += 15; $dnsFindings += "DKIM default selector not found." }
if ($dnsRisk -gt 100) { $dnsRisk = 100 }

$scriptRisk = 0
$scriptFindings = @()
$extDomains = @()
$extNoSRI = 0
$extTotal = 0
$jqOld = @()
if ($httpsHome.body) {
  $sm = [regex]::Matches($httpsHome.body, "(?is)<script[^>]+src\s*=\s*['`"]([^'`">]+)")
  foreach ($m in $sm) {
    $tag = $m.Value
    $src = $m.Groups[1].Value
    if ($src.StartsWith("//")) { $src = "https:$src" }
    $scriptHost = $null
    try {
      if ($src.StartsWith("http://") -or $src.StartsWith("https://")) {
        $scriptHost = ([Uri]$src).Host.ToLower()
      }
    } catch {}
    if ($scriptHost -and ($scriptHost -ne $Domain.ToLower()) -and (-not $scriptHost.EndsWith(".$($Domain.ToLower())"))) {
      $extTotal += 1
      $extDomains += $scriptHost
      if ($tag -notmatch "(?i)\sintegrity\s*=") { $extNoSRI += 1 }
    }
    if ($src -match "(?i)jquery[^0-9]*([0-9]+\.[0-9]+(\.[0-9]+)?)") {
      $v = $Matches[1]
      $parts = $v.Split(".")
      $major = [int]$parts[0]
      $minor = if ($parts.Count -gt 1) { [int]$parts[1] } else { 0 }
      if (($major -eq 1 -and $minor -lt 12) -or ($major -eq 2)) { $jqOld += $v }
    }
  }
}
$extDomains = $extDomains | Sort-Object -Unique
if ($extTotal -gt 0) { $scriptRisk += [math]::Min(20, $extTotal * 2) }
if ($extNoSRI -gt 0) { $scriptRisk += [math]::Min(30, $extNoSRI * 5); $scriptFindings += "External scripts without SRI: $extNoSRI/$extTotal." }
if ($jqOld.Count -gt 0) { $scriptRisk += 30; $scriptFindings += "Outdated jQuery detected: $($jqOld -join ', ')." }
if ($scriptRisk -gt 100) { $scriptRisk = 100 }

$repRisk = 0
$repNotTested = $true
$repFinding = "Reputation APIs are not configured in quick mode."

$sslCoverage = "normal"
if ($httpsHome.status -eq 0) { $sslCoverage = "error" }
elseif ($httpHome.status -eq 0) { $sslCoverage = "review_required" }

$blockedFileChecks = ($pathEvidence | Where-Object { @(0, 429, 503) -contains $_.status }).Count
$fileCoverage = "normal"
if ($leaked.Count -eq 0 -and $blockedFileChecks -gt 0) {
  $fileCoverage = "review_required"
  $fileFindings += "Some sensitive paths are blocked/rate-limited; manual validation recommended."
}

$openCoverage = "normal"
if ($openAdminRisk -eq 0 -and (@(0, 429, 503) -contains $wpAdmin.status -or @(0, 429, 503) -contains $bitrixAdmin.status)) {
  $openCoverage = "review_required"
  $openAdminFindings += "Admin endpoints are partially blocked; exposure needs manual validation."
}

$headerCoverage = "normal"
if ($httpsHome.status -eq 0) { $headerCoverage = "error" }
elseif ($httpsHeaders.Keys.Count -eq 0) { $headerCoverage = "review_required" }

$dnsCoverage = "normal"
$scriptCoverage = "normal"
if (-not $httpsHome.body) {
  $scriptCoverage = if ($httpsHome.status -eq 0) { "error" } else { "review_required" }
}

$repCoverage = "not_tested"

$sslConfidence = if ($sslCoverage -eq "normal") { "high" } elseif ($sslCoverage -eq "review_required") { "medium" } else { "low" }
$fileConfidence = if ($fileCoverage -eq "normal") { "high" } elseif ($fileCoverage -eq "review_required") { "medium" } else { "low" }
$openConfidence = if ($openCoverage -eq "normal") { "high" } elseif ($openCoverage -eq "review_required") { "medium" } else { "low" }
$headerConfidence = if ($headerCoverage -eq "normal") { "high" } elseif ($headerCoverage -eq "review_required") { "medium" } else { "low" }
$formsConfidence = if ($pagesAccessible -ge 2 -and $formsCount -gt 0) { "high" } elseif ($pagesAccessible -ge 1) { "medium" } else { "low" }
$dnsConfidence = "medium"
$scriptConfidence = if ($scriptCoverage -eq "normal") { "high" } elseif ($scriptCoverage -eq "review_required") { "medium" } else { "low" }
$repConfidence = "low"

$offerReasons = @{
  ssl_redirects = "Visible immediately and easy to explain."
  file_leaks = "Strong urgency when exposure is present."
  open_admin = "Clear and understandable for non-technical owner."
  security_headers = "Easy to benchmark with clear grade logic."
  forms_patient_data = "Direct legal/trust risk narrative."
  dns_email_protection = "Strong technical depth for demo."
  third_party_scripts = "Good advanced layer for upsell."
  reputation_osint = "Adds external trust context."
}

$blocks = @(
  [pscustomobject]@{
    id = "ssl_redirects"; name = "SSL + redirects"; priority = 1; score = $sslRisk
    status = (Resolve-BlockStatus $sslRisk $sslCoverage); confidence = $sslConfidence; evidence_count = 5
    free_offer_visible = $true; offer_reason = $offerReasons.ssl_redirects; findings_count = $sslFindings.Count
    key_finding = $(if($sslFindings.Count){$sslFindings[0]}else{"No major SSL issue detected."})
  },
  [pscustomobject]@{
    id = "file_leaks"; name = "File leaks"; priority = 2; score = $fileRisk
    status = (Resolve-BlockStatus $fileRisk $fileCoverage); confidence = $fileConfidence; evidence_count = $pathEvidence.Count
    free_offer_visible = $true; offer_reason = $offerReasons.file_leaks; findings_count = $fileFindings.Count
    key_finding = $(if($fileFindings.Count){$fileFindings[0]}else{"No sensitive file exposure in quick dictionary."})
  },
  [pscustomobject]@{
    id = "open_admin"; name = "Open admin and CMS exposure"; priority = 3; score = $openAdminRisk
    status = (Resolve-BlockStatus $openAdminRisk $openCoverage); confidence = $openConfidence; evidence_count = 3
    free_offer_visible = $true; offer_reason = $offerReasons.open_admin; findings_count = $openAdminFindings.Count
    key_finding = $(if($openAdminFindings.Count){$openAdminFindings[0]}else{"No strong admin/CMS exposure signal."})
  },
  [pscustomobject]@{
    id = "security_headers"; name = "Security headers"; priority = 4; score = $headerRisk
    status = (Resolve-HeaderBlockStatus $headerRisk $headerCoverage $missingPrimary.Count $missingHsts $redirectOk $xPoweredByPresent); confidence = $headerConfidence; evidence_count = ($requiredHeaders.Count + 2)
    free_offer_visible = $true; offer_reason = $offerReasons.security_headers; findings_count = $headerFindings.Count
    key_finding = $(if($headerFindings.Count){$headerFindings[0]}else{"Headers baseline looks acceptable."})
  },
  [pscustomobject]@{
    id = "forms_patient_data"; name = "Forms and patient data"; priority = 5; score = $formsRisk
    status = (Resolve-BlockStatus $formsRisk $formsCoverage); confidence = $formsConfidence; evidence_count = ($formPageEvidence.Count + $formsAnalyzed + $policyChecks.Count)
    free_offer_visible = $true; offer_reason = $offerReasons.forms_patient_data; findings_count = $formsFindings.Count
    key_finding = $(if($formsFindings.Count){$formsFindings[0]}else{"No obvious form transport/privacy issue."})
  },
  [pscustomobject]@{
    id = "dns_email_protection"; name = "DNS and email protection"; priority = 6; score = $dnsRisk
    status = (Resolve-BlockStatus $dnsRisk $dnsCoverage); confidence = $dnsConfidence; evidence_count = 3
    free_offer_visible = $false; offer_reason = $offerReasons.dns_email_protection; findings_count = $dnsFindings.Count
    key_finding = $(if($dnsFindings.Count){$dnsFindings[0]}else{"SPF/DMARC/DKIM baseline detected."})
  },
  [pscustomobject]@{
    id = "third_party_scripts"; name = "Third-party scripts"; priority = 7; score = $scriptRisk
    status = (Resolve-BlockStatus $scriptRisk $scriptCoverage); confidence = $scriptConfidence; evidence_count = [math]::Max(1, $extTotal)
    free_offer_visible = $false; offer_reason = $offerReasons.third_party_scripts; findings_count = $scriptFindings.Count
    key_finding = $(if($scriptFindings.Count){$scriptFindings[0]}else{"No major script-chain issues in quick scan."})
  },
  [pscustomobject]@{
    id = "reputation_osint"; name = "Reputation and OSINT"; priority = 8; score = $repRisk
    status = (Resolve-BlockStatus $repRisk $repCoverage); confidence = $repConfidence; evidence_count = 0
    free_offer_visible = $false; offer_reason = $offerReasons.reputation_osint; findings_count = 1; key_finding = $repFinding
  }
)

$sum = 0.0
$wSum = 0.0
foreach ($b in $blocks) {
  $w = 1.0
  if ($b.priority -le 3) { $w = 1.5 } elseif ($b.priority -le 5) { $w = 1.2 }
  $sum += ($b.score * $w)
  $wSum += $w
}
$overall = if ($wSum -gt 0) { [math]::Round($sum / $wSum, 1) } else { 0.0 }
$riskBand = RiskBand $overall

$freeSummary = @()
foreach ($b in ($blocks | Where-Object { $_.free_offer_visible } | Sort-Object priority)) {
  if ($b.status -eq "warning" -or $b.status -eq "critical") {
    $freeSummary += "$($b.name): $($b.key_finding)"
  }
}
$freeSummary = $freeSummary | Select-Object -First 5

$criticalCount = @($blocks | Where-Object { $_.status -eq "critical" }).Count
$reviewRequiredCount = @($blocks | Where-Object { $_.status -eq "review_required" }).Count
$notTestedCount = @($blocks | Where-Object { $_.status -eq "not_tested" }).Count
$errorCount = @($blocks | Where-Object { $_.status -eq "error" }).Count

$nextAction = "no_offer_now"
$nextActionReason = "No strong confirmed issues in quick run."
if ($criticalCount -gt 0) {
  $nextAction = "offer_now"
  $nextActionReason = "Critical confirmed findings exist."
} elseif ($reviewRequiredCount -gt 0 -or $errorCount -gt 0) {
  $nextAction = "agent_review"
  $nextActionReason = "Some blocks require manual validation."
} elseif ($overall -ge 35) {
  $nextAction = "offer_now"
  $nextActionReason = "Multiple medium signals in quick run."
}

$normalized = [ordered]@{
  site_id = $siteId
  domain = $Domain
  audit_timestamp_utc = $timestamp
  overall_score = $overall
  risk_band = $riskBand
  free_offer_summary = @($freeSummary)
  triage = [ordered]@{
    next_action = $nextAction
    reason = $nextActionReason
    critical_count = $criticalCount
    review_required_count = $reviewRequiredCount
    not_tested_count = $notTestedCount
    error_count = $errorCount
  }
  compliance_152 = [ordered]@{
    status = $compliance152Status
    confidence = $formsConfidence
    forms_total = $formsCount
    forms_total_detected = $formsTotalDetected
    forms_non_relevant = $formsNonRelevant
    forms_with_required_consent = $formsWithRequiredConsent
    forms_with_any_required_checkbox = $formsWithAnyRequiredCheckbox
    forms_with_policy_marker = $formsWithPolicy
    forms_insecure_action = $formsInsecureAction
    forms_non_post = $formsNonPost
    forms_method_unknown = $formsMethodUnknown
    policy_candidates_checked = $policyChecks.Count
    policy_candidates_ok = $policyOkCount
    pages_sampled = $formPages.Count
    pages_accessible = $pagesAccessible
    criteria = $compliance152Criteria
  }
  blocks = $blocks
}

$evidence = [ordered]@{
  site_id = $siteId
  domain = $Domain
  audit_timestamp_utc = $timestamp
  transport = @{
    https_home_status = $httpsHome.status
    http_home_status = $httpHome.status
    redirect_location = $location
    redirect_to_https = $redirectOk
    hsts = $hsts
    tls = $tlsInfo
    mixed_content_count = $mixedContent
  }
  file_checks = $pathEvidence
  admin_checks = @{
    wp_admin_status = $wpAdmin.status
    bitrix_admin_status = $bitrixAdmin.status
    wp_version = $wpVersion
  }
  header_checks = @{
    missing = $missing
    server = $serverHeader
    x_powered_by = $xPoweredBy
  }
  form_checks = @{
    forms_count = $formsCount
    forms_count_total_detected = $formsTotalDetected
    forms_count_non_relevant = $formsNonRelevant
    forms_with_policy_marker = $formsWithPolicy
    forms_with_required_consent = $formsWithRequiredConsent
    forms_with_any_required_checkbox = $formsWithAnyRequiredCheckbox
    forms_insecure_action = $formsInsecureAction
    forms_non_post = $formsNonPost
    forms_method_unknown = $formsMethodUnknown
    pages_sampled = $formPages.Count
    pages_accessible = $pagesAccessible
    pages_blocked = $pagesBlocked
    page_checks = $formPageEvidence
    policy_checks = $policyChecks
    has_privacy = $hasPrivacy
    has_captcha = $hasCaptcha
    has_honeypot = $hasHoneypot
    has_cookie_notice = $hasCookie
  }
  dns_checks = @{
    spf = $spf
    dmarc = $dmarc
    dkim_default = $dkimDefault
  }
  script_checks = @{
    external_domains = $extDomains
    external_total = $extTotal
    external_no_sri = $extNoSRI
    outdated_jquery = $jqOld
  }
}

$normalized | ConvertTo-Json -Depth 10 | Set-Content -Path $normalizedPath -Encoding UTF8
$evidence | ConvertTo-Json -Depth 10 | Set-Content -Path $evidencePath -Encoding UTF8

Write-Output "Quick audit complete for $Domain"
Write-Output "Normalized: $normalizedPath"
Write-Output "Evidence: $evidencePath"
Write-Output "Overall risk score: $overall ($riskBand)"
