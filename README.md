# Subscription Control Bot

Личный Telegram-бот для контроля подписок, балансовых сервисов и расходов.

## Что есть в этой версии

- ежемесячные, годовые и балансовые сервисы
- прогноз падения баланса
- фиксация оплат и пополнений
- локальное сохранение в JSON
- гибкие напоминания
- история по каждой подписке
- редактирование подписок
- категории и теги
- недельная и месячная сводка
- поиск, фильтры и архив подписок
- учёт пользователей
- команда `/today` для задач на сегодня
- расширенная `/dashboard`
- прогноз расходов на ближайшие дни через `/forecast`
- экспорт в JSON и CSV
- импорт из JSON и CSV

## Хранение данных

Данные сохраняются локально в JSON-файл.
По умолчанию используется:

`subscription_data.json`

Путь можно изменить через `.env` переменную `DATA_FILE`.

## Требования

- Python 3.13.12
- Telegram bot token

## Быстрый запуск

### Linux / macOS

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py
```

### Windows PowerShell

```powershell
py -3.13 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
python main.py
```

## Настройка `.env`

```env
TG_BOT_API_KEY=твой_токен_бота
OWNER_USER_ID=
TIMEZONE=Europe/Berlin
DAILY_SUMMARY_TIME=09:00
ALERTS_TIME=08:30
WEEKLY_SUMMARY_TIME=09:10
WEEKLY_SUMMARY_WEEKDAY=0
MONTHLY_SUMMARY_TIME=09:20
MONTHLY_SUMMARY_DAY=1
SOON_DAYS=14
BALANCE_WARNING_DAYS=3
FORECAST_DAYS=30
DATA_FILE=subscription_data.json
```

## Основные команды

- `/start` — старт и меню
- `/add` — добавить подписку
- `/edit` — изменить подписку
- `/list` — показать активные подписки
- `/today` — что требует внимания сегодня
- `/soon` — ближайшие списания
- `/topup` — балансовые сервисы и низкий баланс
- `/pay` — отметить оплату или пополнение
- `/setbalance` — обновить баланс вручную
- `/dashboard` — общая расширенная сводка
- `/forecast` — прогноз расходов на `FORECAST_DAYS`
- `/report` — отчёт за текущий месяц
- `/weekly` — сводка за 7 дней
- `/monthly` — сводка за текущий месяц
- `/find текст` — поиск по названию, проекту, категории, тегам, заметкам
- `/filter ...` — фильтры
- `/archive` — архив подписок
- `/history` — история последних трат
- `/export [json|csv]` — выгрузить резервную копию
- `/import` — включить режим импорта и прислать JSON/CSV-файл
- `/users` — кто пользуется ботом
- `/demo` — демо-данные
- `/cancel` — отмена текущего диалога

## Экспорт и импорт

### JSON

`/export`

Отправляет полный бэкап текущего пользователя:
- активные подписки
- архив
- историю операций
- активность пользователя

`/import` + JSON-файл восстанавливает всё это обратно.

### CSV

`/export csv`

Отправляет два файла:
- список подписок
- история операций

`/import` + CSV-файл импортирует подписки из CSV в активный список или архив по колонке `source`.
История из CSV не импортируется.

## Примеры фильтров

- `/filter active`
- `/filter paused`
- `/filter balance`
- `/filter currency:RUB`
- `/filter project:Личное`
- `/filter category:AI/API`
- `/filter tag:сервер`
- `/filter archived category:Домены`

## Примечание

JSON-хранилище локальное. Если среда деплоя полностью пересоздаётся, файл может исчезнуть.
Для Railway это лучше, чем хранение только в памяти, но хуже, чем SQLite/Postgres.


## Fix for Railway/PTB 22.7
- Weekly summary scheduling now uses `run_daily(..., days=(...))`, because PTB 22.7 has `run_daily` and `run_monthly`, but no `run_weekly`.
- `WEEKLY_SUMMARY_WEEKDAY` is interpreted as Monday=0 ... Sunday=6 and converted internally for PTB.
- Monthly summary now uses `run_monthly`.
