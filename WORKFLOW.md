# Workflow

## Что лежит в папке

- `screening_rule.md` — правило проверки
- `dashboard.html` — сводный дашборд
- `sites/*.html` — детальные страницы по каждой клинике (PoC)
- `data/audits/*.audit.json` — результаты проверок сайтов
- `data/sites_manifest.json` — список клиник для дашборда
- `scripts/audit_site.py` — запуск проверки сайта в JSON
- `scripts/build_dashboard.py` — сборка дашборда и деталей из JSON

## Добавить новый сайт

1. Запустить аудит:

```powershell
python scripts/audit_site.py https://example.com --out data/audits/example.audit.json
```

2. Добавить запись в `data/sites_manifest.json`:

```json
{
  "id": "example-com",
  "clinic": "Клиника Example",
  "site": "example.com",
  "audit_file": "data/audits/example.audit.json",
  "contact_email": "mail@example.com",
  "result": "проверить"
}
```

3. Пересобрать страницы:

```powershell
python scripts/build_dashboard.py
```

После этого:
- строка появится в `dashboard.html`
- подробная PoC-страница появится в `sites/example-com.html`
