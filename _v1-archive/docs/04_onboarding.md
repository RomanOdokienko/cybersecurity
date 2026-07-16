# Onboarding нового участника

Версия: 2026-04-16

## 1. Предпосылки
1. Windows + PowerShell.
2. Git.
3. Python 3.x (для локального HTTP сервера dashboard).

## 2. Первичный старт
1. Клонировать репозиторий.
2. Открыть корень проекта.
3. Проверить запуск dashboard:
```powershell
python -m http.server 8080
```
Открыть `http://localhost:8080/dashboard/`.
4. Если в репозиторий заходит AI-агент (Claude), начать с файла `CLAUDE.md` в корне.

## 3. Первый рабочий прогон
1. Запустить quick:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-site-quick.ps1 -Domain example.com
```
2. Обновить dashboard-данные:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-dashboard-data.ps1
```
3. Для списка доменов использовать batch-команду (экспорт выполнится автоматически):
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-quick-batch.ps1 -FilePath .\domains.txt
```

## 4. Где искать результат
1. Raw evidence: `data/raw/<site_id>/`.
2. Normalized JSON: `data/normalized/<site_id>.json`.
3. Dashboard dataset: `dashboard/sample_audits.json`.

## 5. Базовые рабочие правила
1. Не использовать активные/интрузивные методы.
2. Не менять схему данных без фиксации в `schemas/` и docs.
3. Любую смену scoring/triage фиксировать в `docs/`.

## 6. Проверка прав доступа в GitHub (для второго участника)
1. `Settings -> Collaborators`: роль пользователя должна быть `Write` или выше.
2. `Settings -> Rules -> Rulesets`: если ruleset нет, дополнительных branch-ограничений нет.
3. Практический тест:
- создать ветку и сделать `git push origin <branch>`;
- открыть PR в `main`;
- проверить, что merge доступен без обязательного approval (если это ожидаемая политика команды).
