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
- еженедельная и месячная сводка
- поиск и фильтры
- архив подписок с восстановлением
- учёт пользователей

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
DATA_FILE=subscription_data.json
```

## Основные команды

- `/start` — старт и меню
- `/add` — добавить подписку
- `/edit` — изменить подписку
- `/list` — показать активные подписки
- `/archive` — показать архив
- `/soon` — ближайшие списания
- `/topup` — балансовые сервисы и низкий баланс
- `/pay` — отметить оплату или пополнение
- `/setbalance` — обновить баланс вручную
- `/dashboard` — общая сводка
- `/report` — отчёт за текущий месяц
- `/weekly` — сводка за последние 7 дней
- `/monthly` — сводка за текущий месяц
- `/find текст` — поиск по названию, проекту, категории, тегам, заметкам
- `/filter ...` — фильтры
- `/history` — общая история трат
- `/users` — кто пользуется ботом
- `/demo` — демо-данные
- `/cancel` — отмена текущего диалога

## Примеры фильтров

- `/filter active`
- `/filter paused`
- `/filter balance`
- `/filter currency:RUB`
- `/filter project:Личное`
- `/filter category:AI/API`
- `/filter tag:сервер`
- `/filter archived category:Домены`

## Архив

Кнопка удаления теперь не удаляет подписку сразу.
Она переносит её в архив.

В архиве можно:
- восстановить подписку
- посмотреть её историю
- удалить её навсегда

## Категории и теги

При создании подписки бот теперь спрашивает:
- проект
- категорию
- теги

Это используется в:
- карточке подписки
- поиске
- фильтрах
- отчётах
- недельной и месячной сводке

## Автосводки

Бот отправляет:
- ежедневную сводку
- еженедельную сводку
- месячную сводку

Дни и время можно менять в `.env`.

## Примечание

JSON-хранилище локальное. Если среда деплоя полностью пересоздаётся, файл может исчезнуть.
Для Railway это лучше, чем хранение только в памяти, но хуже, чем SQLite/Postgres.
