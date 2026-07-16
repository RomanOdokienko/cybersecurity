const siteSelect = document.getElementById("siteSelect");
const actionFilter = document.getElementById("actionFilter");
const domainValue = document.getElementById("domainValue");
const siteFocusValue = document.getElementById("siteFocusValue");
const scoreValue = document.getElementById("scoreValue");
const riskValue = document.getElementById("riskValue");
const timeValue = document.getElementById("timeValue");
const actionValue = document.getElementById("actionValue");
const actionHint = document.getElementById("actionHint");
const law152Value = document.getElementById("law152Value");
const law152Hint = document.getElementById("law152Hint");
const law152Attention = document.getElementById("law152Attention");
const law152Explain = document.getElementById("law152Explain");
const law152CriteriaBody = document.getElementById("law152CriteriaBody");
const reportLinks = document.getElementById("reportLinks");
const fullMarkersMeta = document.getElementById("fullMarkersMeta");
const fullMarkersBody = document.getElementById("fullMarkersBody");
const tableBody = document.getElementById("tableBody");
const queueBody = document.getElementById("queueBody");

const state = {
  data: [],
  reportIndex: {},
  selectedIndex: -1,
  filter: "all",
};

const ACTION_LABELS = {
  offer_now: "оффер сейчас",
  agent_review: "ручная проверка",
  no_offer_now: "без оффера сейчас",
};

const STATUS_LABELS = {
  ok: "ок",
  warning: "предупреждение",
  critical: "критично",
  review_required: "нужна проверка",
  not_tested: "не проверено",
  error: "ошибка",
  pass: "пройдено",
  fail: "провалено",
  not_observable: "не наблюдается",
};

const CONFIDENCE_LABELS = {
  high: "высокая",
  medium: "средняя",
  low: "низкая",
};

const RISK_LABELS = {
  critical: "критичный",
  high: "высокий",
  medium: "средний",
  low: "низкий",
};

const OFFER_REASON_LABELS = {
  "Visible immediately and easy to explain.": "Видно сразу и легко объяснить владельцу.",
  "Strong urgency when exposure is present.": "При наличии экспозиции выглядит срочно и убедительно.",
  "Clear and understandable for non-technical owner.": "Понятно даже нетехническому владельцу.",
  "Easy to benchmark with clear grade logic.": "Легко бенчмаркать через понятный грейд.",
  "Direct legal/trust risk narrative.": "Прямая связка с юридическим и репутационным риском.",
  "Strong technical depth for demo.": "Добавляет техническую глубину на демо.",
  "Good advanced layer for upsell.": "Хороший продвинутый слой для апсейла.",
  "Adds external trust context.": "Добавляет внешний контекст доверия.",
};

const LAW152_CRITERION_LABELS = {
  pages_accessible: "Покрытие страниц для проверки",
  forms_detected: "Наличие форм с ПДн",
  required_consent_checkbox: "Обязательный consent checkbox на формах",
  policy_marker_near_form: "Ссылка/маркер политики рядом с формой",
  secure_form_action: "Безопасный action формы (без HTTP)",
  post_method: "Отправка форм методом POST",
  policy_page_reachable: "Доступность страницы политики",
  "Coverage of pages for screening": "Покрытие страниц для проверки",
  "Forms detected": "Наличие форм с ПДн",
  "Required consent checkbox on forms": "Обязательный consent checkbox на формах",
  "Policy marker near form": "Ссылка/маркер политики рядом с формой",
  "Secure form action (no HTTP)": "Безопасный action формы (без HTTP)",
  "Form POST method": "Отправка форм методом POST",
  "Policy page reachable": "Доступность страницы политики",
};

function statusClass(status) {
  return `status-${status || "not_tested"}`;
}

function formatDate(value, withTime = true) {
  if (!value) return "-";
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  const options = {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  };
  if (withTime) {
    options.hour = "2-digit";
    options.minute = "2-digit";
  }
  return new Intl.DateTimeFormat("ru-RU", options).format(dt);
}

function renderPill(status, label) {
  return `<span class="status-pill ${statusClass(status)}">${label}</span>`;
}

function localizeOfferReason(text) {
  if (!text) return "-";
  return OFFER_REASON_LABELS[text] || text;
}

function localize152Criterion(c) {
  const byId = LAW152_CRITERION_LABELS[c?.id];
  if (byId) return byId;
  const byTitle = LAW152_CRITERION_LABELS[c?.title];
  if (byTitle) return byTitle;
  return c?.title || c?.id || "-";
}

function getScore(site) {
  if (typeof site?.overall_score === "number") return site.overall_score;
  if (typeof site?.full_plus_score === "number") return site.full_plus_score;
  return null;
}

function getScoreClass(score) {
  if (typeof score !== "number") return "";
  if (score >= 80) return "score-critical";
  if (score >= 60) return "score-high";
  if (score >= 35) return "score-medium";
  return "score-low";
}

function getRiskBand(site) {
  const key = site?.risk_band || site?.overall_risk_band || site?.full_plus_risk_band;
  if (!key) return "-";
  return RISK_LABELS[key] || key;
}

function getAction(site) {
  if (site?.triage?.next_action) return site.triage.next_action;

  const blocks = Array.isArray(site?.blocks) ? site.blocks : [];
  const critical = blocks.some((b) => b.status === "critical");
  const review = blocks.some((b) => b.status === "review_required" || b.status === "error");
  if (critical) return "offer_now";
  if (review) return "agent_review";
  return "no_offer_now";
}

function getActionReason(site) {
  if (site?.triage?.reason) {
    const reason = site.triage.reason;
    if (reason === "Critical confirmed findings exist.") return "Есть подтвержденные критичные находки.";
    if (reason === "Some blocks require manual validation.") return "Есть блоки, требующие ручной валидации.";
    if (reason === "Multiple medium signals in quick run.") return "В quick-прогоне набрано несколько средних сигналов.";
    if (reason === "No strong confirmed issues in quick run.") return "В quick-прогоне нет сильных подтвержденных проблем.";
    return reason;
  }
  return "Выведено по статусам блоков.";
}

function get152Status(site) {
  return site?.compliance_152?.status || site?.extra_full_markers?.compliance_152fz?.status || "not_observable";
}

function getCriticalBlocks(site) {
  const blocks = Array.isArray(site?.blocks) ? site.blocks : [];
  return blocks.filter((b) => b.status === "critical").map((b) => b.name || b.id || "-");
}

function getReviewBlocks(site) {
  const blocks = Array.isArray(site?.blocks) ? site.blocks : [];
  return blocks
    .filter((b) => b.status === "review_required" || b.status === "error")
    .map((b) => b.name || b.id || "-");
}

function getLaw152Hint(site) {
  const c = site?.compliance_152;
  const status = get152Status(site);
  if (!c) return "Детали проверки смотрите ниже в блоке 152‑ФЗ.";
  if (status === "pass") return "Quick-критерии 152‑ФЗ пройдены, детали ниже.";
  if (status === "fail") return "Есть подтвержденные разрывы quick-критериев, детали ниже.";
  if (status === "review_required") return "Нужна ручная валидация по 152‑ФЗ, детали ниже.";
  return "На доступных данных нет устойчивого сигнала, детали ниже.";
}

function getLaw152AttentionText(site) {
  const status = get152Status(site);
  if (status !== "not_observable") return "";

  const c = site?.compliance_152 || {};
  const sampled = typeof c.pages_sampled === "number" ? c.pages_sampled : null;
  const accessible = typeof c.pages_accessible === "number" ? c.pages_accessible : null;
  const forms = typeof c.forms_total === "number" ? c.forms_total : null;

  const quickFacts = [];
  if (accessible !== null && sampled !== null) {
    quickFacts.push(`страницы ${accessible}/${sampled}`);
  }
  if (forms !== null) {
    quickFacts.push(`релевантные формы ${forms}`);
  }

  const factsText = quickFacts.length ? ` (quick: ${quickFacts.join(", ")})` : "";
  return `Статус «не наблюдается» не означает «всё хорошо». Quick-скрин не получил достаточно наблюдаемых данных${factsText}; для решения по 152‑ФЗ нужна ручная/агентная проверка.`;
}

function setList(el, items) {
  el.innerHTML = "";
  if (!items || items.length === 0) {
    const li = document.createElement("li");
    li.textContent = "-";
    el.appendChild(li);
    return;
  }
  items.forEach((text) => {
    const li = document.createElement("li");
    li.textContent = text;
    el.appendChild(li);
  });
}

function explain152(site) {
  const c = site?.compliance_152;
  if (!c) {
    return ["В этой записи нет структурированных quick-данных по 152‑ФЗ."];
  }

  const items = [];
  const confidenceLabel = CONFIDENCE_LABELS[c.confidence] || c.confidence || "-";
  items.push(
    `Статус: ${STATUS_LABELS[c.status] || c.status}, уверенность: ${confidenceLabel}.`
  );
  if (typeof c.pages_accessible === "number" && typeof c.pages_sampled === "number") {
    items.push(`Проверено страниц: ${c.pages_accessible} из ${c.pages_sampled} доступны.`);
  }

  if (c.status === "fail" || c.status === "review_required") {
    if (typeof c.forms_total === "number") {
      items.push(`Обнаружено форм: ${c.forms_total}.`);
    }
    if (
      typeof c.forms_total === "number" &&
      typeof c.forms_with_required_consent === "number" &&
      c.forms_with_required_consent < c.forms_total
    ) {
      items.push(
        `Обязательный consent checkbox есть не на всех формах (${c.forms_with_required_consent}/${c.forms_total}).`
      );
    }
    if (
      typeof c.forms_total === "number" &&
      typeof c.forms_with_policy_marker === "number" &&
      c.forms_with_policy_marker < c.forms_total
    ) {
      items.push(`Маркер/ссылка на политику есть не рядом со всеми формами (${c.forms_with_policy_marker}/${c.forms_total}).`);
    }
    if (typeof c.forms_insecure_action === "number" && c.forms_insecure_action > 0) {
      items.push(`Формы с небезопасным HTTP action: ${c.forms_insecure_action}.`);
    }
    if (typeof c.forms_non_post === "number" && c.forms_non_post > 0) {
      items.push(`Формы не через POST: ${c.forms_non_post}.`);
    }
    if (
      typeof c.policy_candidates_checked === "number" &&
      c.policy_candidates_checked > 0 &&
      typeof c.policy_candidates_ok === "number" &&
      c.policy_candidates_ok === 0
    ) {
      items.push("Проверены кандидаты policy-страницы, но ни один не вернул 200.");
    }
    if (typeof c.pages_accessible === "number" && c.pages_accessible === 0) {
      items.push("В quick-режиме нет доступных страниц для проверки форм/политики.");
    }
  }

  return items;
}

function buildFallback152Criteria(site) {
  const c = site?.compliance_152;
  if (!c) return [];
  return [
    {
      title: "Покрытие страниц для проверки",
      status: typeof c.pages_accessible === "number" && c.pages_accessible > 0 ? "pass" : "not_observable",
      value: `${c.pages_accessible ?? "-"} / ${c.pages_sampled ?? "-"}`,
      evidence: "quick page sampling",
    },
    {
      title: "Обязательный consent checkbox на формах",
      status:
        typeof c.forms_total === "number" && c.forms_total > 0 && c.forms_with_required_consent === c.forms_total
          ? "pass"
          : "fail",
      value: `${c.forms_with_required_consent ?? "-"} / ${c.forms_total ?? "-"}`,
      evidence: "forms_with_required_consent/forms_total",
    },
    {
      title: "Ссылка/маркер политики рядом с формой",
      status:
        typeof c.forms_total === "number" && c.forms_total > 0 && c.forms_with_policy_marker === c.forms_total
          ? "pass"
          : "fail",
      value: `${c.forms_with_policy_marker ?? "-"} / ${c.forms_total ?? "-"}`,
      evidence: "forms_with_policy_marker/forms_total",
    },
    {
      title: "Доступность страницы политики",
      status: (c.policy_candidates_ok ?? 0) > 0 ? "pass" : "fail",
      value: `${c.policy_candidates_ok ?? "-"} / ${c.policy_candidates_checked ?? "-"}`,
      evidence: "policy_candidates_ok/policy_candidates_checked",
    },
  ];
}

function render152Criteria(site) {
  const criteriaRaw = site?.compliance_152?.criteria;
  const criteria = Array.isArray(criteriaRaw) && criteriaRaw.length > 0
    ? criteriaRaw
    : buildFallback152Criteria(site);

  law152CriteriaBody.innerHTML = "";
  if (!criteria.length) {
    law152CriteriaBody.innerHTML = `
      <tr>
        <td colspan="4">Нет структурированных критериев для верификации.</td>
      </tr>
    `;
    return;
  }

  criteria.forEach((c) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${localize152Criterion(c)}</td>
      <td>${renderPill(c.status || "not_observable", STATUS_LABELS[c.status] || c.status || "not_observable")}</td>
      <td>${c.value ?? "-"}</td>
      <td>${c.evidence ?? "-"}</td>
    `;
    law152CriteriaBody.appendChild(tr);
  });
}

function explainAction(site) {
  const action = getAction(site);
  const triage = site?.triage || {};
  const items = [];
  items.push(`Следующее действие: ${ACTION_LABELS[action] || action}.`);
  items.push(getActionReason(site));

  const criticalCount = triage.critical_count ?? getCriticalBlocks(site).length;
  const reviewCount = triage.review_required_count ?? getReviewBlocks(site).length;
  const notTestedCount = triage.not_tested_count ?? 0;
  const errorCount = triage.error_count ?? 0;
  items.push(
    `Счетчики блоков -> критично: ${criticalCount}, нужна проверка: ${reviewCount}, не проверено: ${notTestedCount}, ошибка: ${errorCount}.`
  );

  const criticalBlocks = getCriticalBlocks(site);
  if (criticalBlocks.length) {
    items.push(`Критичные блоки: ${criticalBlocks.join(", ")}.`);
  }

  const reviewBlocks = getReviewBlocks(site);
  if (reviewBlocks.length) {
    items.push(`Блоки для ручной проверки: ${reviewBlocks.join(", ")}.`);
  }

  return items;
}

function getReportEntry(site) {
  if (!site) return null;
  const domain = (site.domain || "").toLowerCase();
  if (state.reportIndex[domain]) return state.reportIndex[domain];

  const noWww = domain.replace(/^www\./, "");
  if (state.reportIndex[noWww]) return state.reportIndex[noWww];
  return null;
}

function renderMaterials(site) {
  const entry = getReportEntry(site);

  reportLinks.innerHTML = "";
  const links = Array.isArray(entry?.links) ? entry.links : [];
  if (!links.length) {
    const li = document.createElement("li");
    li.textContent = "Для этого сайта пока доступны только quick-данные на дашборде.";
    reportLinks.appendChild(li);
  } else {
    links.forEach((link) => {
      const li = document.createElement("li");
      const a = document.createElement("a");
      a.href = link.path || "#";
      a.target = "_blank";
      a.rel = "noopener noreferrer";
      a.textContent = link.label || link.path || "Документ";
      li.appendChild(a);
      reportLinks.appendChild(li);
    });
  }

  const fullMarkers = entry?.full_markers || null;
  const fullTs = entry?.full_audit_timestamp || "";
  fullMarkersMeta.textContent = fullMarkers
    ? `Найден full-профиль${fullTs ? ` от ${formatDate(fullTs)}` : ""}.`
    : "Full-профиль для этого домена пока не найден.";

  fullMarkersBody.innerHTML = "";
  if (!fullMarkers) {
    fullMarkersBody.innerHTML = `
      <tr>
        <td colspan="4">Нет full-маркеров для отображения.</td>
      </tr>
    `;
    return;
  }

  Object.entries(fullMarkers).forEach(([key, marker]) => {
    const tr = document.createElement("tr");
    const findings = Array.isArray(marker?.findings) && marker.findings.length
      ? marker.findings.join("; ")
      : "-";
    tr.innerHTML = `
      <td>${key}</td>
      <td>${renderPill(marker?.status || "not_tested", STATUS_LABELS[marker?.status] || marker?.status || "-")}</td>
      <td>${typeof marker?.score === "number" ? marker.score : "-"}</td>
      <td>${findings}</td>
    `;
    fullMarkersBody.appendChild(tr);
  });
}

function getFilteredIndexes() {
  return state.data
    .map((_, idx) => idx)
    .filter((idx) => state.filter === "all" || getAction(state.data[idx]) === state.filter);
}

function renderSelect() {
  const indexes = getFilteredIndexes();
  siteSelect.innerHTML = "";
  indexes.forEach((idx) => {
    const site = state.data[idx];
    const opt = document.createElement("option");
    opt.value = String(idx);
    opt.textContent = site.domain || site.site_id || "unknown";
    siteSelect.appendChild(opt);
  });

  if (indexes.length === 0) {
    state.selectedIndex = -1;
    return;
  }

  if (!indexes.includes(state.selectedIndex)) {
    state.selectedIndex = indexes[0];
  }
  siteSelect.value = String(state.selectedIndex);
}

function renderSite(site) {
  domainValue.textContent = site.domain || "-";
  siteFocusValue.textContent = site.domain || "-";
  const score = getScore(site);
  scoreValue.className = `value score-value ${getScoreClass(score)}`.trim();
  scoreValue.textContent = typeof score === "number" ? `${score}/100` : "-";
  riskValue.textContent = getRiskBand(site);
  timeValue.textContent = formatDate(site.audit_timestamp_utc, false);

  const action = getAction(site);
  actionValue.innerHTML = renderPill(action, ACTION_LABELS[action] || action);
  actionHint.textContent = getActionReason(site);

  const lawStatus = get152Status(site);
  const lawConfidence = site?.compliance_152?.confidence || "";
  const lawConfidenceLabel = CONFIDENCE_LABELS[lawConfidence] || lawConfidence;
  const lawLabel = lawConfidence
    ? `${STATUS_LABELS[lawStatus] || lawStatus} (${lawConfidenceLabel})`
    : (STATUS_LABELS[lawStatus] || lawStatus);
  law152Value.innerHTML = renderPill(lawStatus, lawLabel);
  law152Hint.textContent = getLaw152Hint(site);
  const law152AttentionText = getLaw152AttentionText(site);
  law152Attention.hidden = !law152AttentionText;
  law152Attention.textContent = law152AttentionText;
  setList(law152Explain, explain152(site));
  render152Criteria(site);
  renderMaterials(site);

  const blocks = Array.isArray(site.blocks) ? [...site.blocks] : [];
  blocks.sort((a, b) => (a.priority || 999) - (b.priority || 999));

  tableBody.innerHTML = "";
  blocks.forEach((block) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${block.priority ?? "-"}</td>
      <td>${block.name ?? block.id ?? "-"}</td>
      <td>${renderPill(block.status ?? "not_tested", STATUS_LABELS[block.status] || block.status || "not_tested")}</td>
      <td>${typeof block.score === "number" ? block.score : "-"}</td>
      <td>${CONFIDENCE_LABELS[block.confidence] || block.confidence || "-"}</td>
      <td>${block.evidence_count ?? "-"}</td>
      <td>${localizeOfferReason(block.offer_reason)}</td>
    `;
    tableBody.appendChild(tr);
  });
}

function renderQueue() {
  const indexes = getFilteredIndexes();
  queueBody.innerHTML = "";

  indexes.forEach((idx) => {
    const site = state.data[idx];
    const action = getAction(site);
    const lawStatus = get152Status(site);
    const triage = site?.triage || {};
    const criticalCount = triage.critical_count ?? getCriticalBlocks(site).length;
    const reviewCount = triage.review_required_count ?? getReviewBlocks(site).length;
    const score = getScore(site);

    const tr = document.createElement("tr");
    tr.className = `queue-row${idx === state.selectedIndex ? " active" : ""}`;
    tr.innerHTML = `
      <td>${site.domain || "-"}</td>
      <td>${typeof score === "number" ? score : "-"}</td>
      <td>${getRiskBand(site)}</td>
      <td>${renderPill(action, ACTION_LABELS[action] || action)}</td>
      <td>${renderPill(lawStatus, STATUS_LABELS[lawStatus] || lawStatus)}</td>
      <td>${criticalCount} / ${reviewCount}</td>
      <td>${formatDate(site.audit_timestamp_utc, true)}</td>
    `;
    tr.addEventListener("click", () => {
      state.selectedIndex = idx;
      siteSelect.value = String(idx);
      renderSite(site);
      renderQueue();
    });
    queueBody.appendChild(tr);
  });

  if (indexes.length === 0) {
    queueBody.innerHTML = `
      <tr>
        <td colspan="7">Нет записей для выбранного фильтра.</td>
      </tr>
    `;
  }
}

function resetSummary() {
  domainValue.textContent = "-";
  siteFocusValue.textContent = "-";
  scoreValue.textContent = "-";
  scoreValue.className = "value score-value";
  riskValue.textContent = "-";
  timeValue.textContent = "-";
  actionValue.textContent = "-";
  actionHint.textContent = "-";
  law152Value.textContent = "-";
  law152Hint.textContent = "-";
  law152Attention.hidden = true;
  law152Attention.textContent = "";
  setList(law152Explain, ["-"]);
  law152CriteriaBody.innerHTML = "";
  reportLinks.innerHTML = "";
  fullMarkersMeta.textContent = "-";
  fullMarkersBody.innerHTML = "";
  tableBody.innerHTML = "";
}

async function init() {
  try {
    const [auditsRes, reportsRes] = await Promise.all([
      fetch("sample_audits.json"),
      fetch("report_index.json").catch(() => null),
    ]);

    const data = await auditsRes.json();
    if (!Array.isArray(data) || data.length === 0) {
      throw new Error("No records");
    }

    let reportIndex = {};
    if (reportsRes && reportsRes.ok) {
      const raw = await reportsRes.json();
      if (raw && typeof raw === "object" && !Array.isArray(raw)) {
        reportIndex = raw;
      }
    }

    state.data = data;
    state.reportIndex = reportIndex;
    state.selectedIndex = 0;

    renderSelect();
    if (state.selectedIndex >= 0) {
      renderSite(state.data[state.selectedIndex]);
    }
    renderQueue();

    siteSelect.addEventListener("change", (e) => {
      state.selectedIndex = Number(e.target.value);
      renderSite(state.data[state.selectedIndex]);
      renderQueue();
    });

    actionFilter.addEventListener("change", (e) => {
      state.filter = e.target.value;
      renderSelect();
      if (state.selectedIndex >= 0) {
        renderSite(state.data[state.selectedIndex]);
      } else {
        resetSummary();
      }
      renderQueue();
    });
  } catch (err) {
    queueBody.innerHTML = `
      <tr>
        <td colspan="7">Не удалось загрузить данные дашборда. Выполни экспорт и запусти локальный HTTP-сервер.</td>
      </tr>
    `;
    tableBody.innerHTML = `
      <tr>
        <td colspan="7">Не удалось загрузить данные дашборда. Выполни экспорт и запусти локальный HTTP-сервер.</td>
      </tr>
    `;
  }
}

init();
