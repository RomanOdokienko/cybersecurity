# Командный workflow

Версия: 2026-04-16

## 1. Роли
1. Аналитик quick: массовый прогон и первичная квалификация.
2. Аналитик manual: ручная валидация спорных кейсов.
3. Продажи/аккаунт: коммуникация оффера и демо.

## 2. Цикл по одному домену
1. Добавить домен в очередь.
2. Выполнить quick.
3. Проверить `next_action` и критичные блоки.
4. При `review_required` выполнить ручную валидацию.
5. Обновить dashboard и клиентские артефакты.

## 3. Definition of Done (для домена)
1. Есть `data/raw/<site_id>/evidence_quick.json`.
2. Есть `data/normalized/<site_id>.json`.
3. Есть запись в dashboard (`sample_audits.json`).
4. Есть понятный `next_action`.
5. Критичные тезисы подтверждены evidence.

## 4. Правила качества
1. Не смешивать подтвержденные факты и гипотезы.
2. Не завышать severity в продажном тексте.
3. Любая сильная формулировка должна иметь проверяемый источник.

## 5. Git-процесс для двух участников
1. Основная ветка: `main`.
2. Рабочие ветки: `feature/*`, `fix/*`, `docs/*`.
3. Перед merge: короткий self-review и smoke-check.
4. Коммиты атомарные: одна логическая задача = один коммит.

## 6. Проверка доступов в репозитории
1. Участник должен иметь роль `Write` или выше (`Settings -> Collaborators`).
2. Проверить `Settings -> Rules -> Rulesets` (ограничения merge/push).
3. Технический smoke-тест: push в свою ветку + создание PR в `main`.

## 7. Синхронизация между аналитиками (обязательно)
Единая модель работы: `pull -> локальная работа -> push -> pull`.

Шаги для каждого участника:
1. Перед началом нового домена: `git pull`.
2. Запустить аудит локально (`run-site-quick.ps1` или `run-site-full.ps1`).
Для пачки доменов можно использовать:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/run-quick-batch.ps1 -FilePath .\domains.txt
```
3. Обновить агрегаты dashboard:
```powershell
powershell -ExecutionPolicy Bypass -File scripts/export-dashboard-data.ps1
```
4. Закоммитить и отправить изменения.
5. Второй участник делает `git pull` и сразу видит обновления в своем dashboard.

Минимальный набор файлов, который должен попасть в коммит после quick:
1. `data/normalized/<site_id>.json`
2. `data/raw/<site_id>/evidence_quick.json`
3. `dashboard/sample_audits.json` (если публикуем обновленный снимок dashboard в репозитории)

Важно:
1. Всегда делать `git pull` до старта нового прогона.
2. Если оба участника меняли общие файлы (`dashboard/sample_audits.json`, `dashboard/report_index.json`), сначала завершить merge/rebase, затем повторно запустить `export-dashboard-data.ps1` и только потом коммитить.
