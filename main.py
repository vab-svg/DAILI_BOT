import logging
import math
import os
import re
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from html import escape
from typing import Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    Defaults,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
LOGGER = logging.getLogger("subscription_bot")

TOKEN = os.getenv("TG_BOT_API_KEY", "").strip()
OWNER_USER_ID_RAW = os.getenv("OWNER_USER_ID", "").strip()
OWNER_USER_ID = int(OWNER_USER_ID_RAW) if OWNER_USER_ID_RAW else None
TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Berlin").strip() or "Europe/Berlin"
DAILY_SUMMARY_TIME = os.getenv("DAILY_SUMMARY_TIME", "09:00").strip() or "09:00"
ALERTS_TIME = os.getenv("ALERTS_TIME", "08:30").strip() or "08:30"
SOON_DAYS = int(os.getenv("SOON_DAYS", "14"))
BALANCE_WARNING_DAYS = int(os.getenv("BALANCE_WARNING_DAYS", "3"))

TZ = ZoneInfo(TIMEZONE_NAME)

MENU = ReplyKeyboardMarkup(
    [
        ["➕ Добавить", "📋 Подписки"],
        ["⏰ Скоро списания", "🪫 Низкий баланс"],
        ["💸 Отметить оплату", "💼 Сводка"],
        ["📈 Отчёт", "🧾 История"],
        ["⚙️ Помощь"],
    ],
    resize_keyboard=True,
)

KIND_LABELS = {
    "monthly": "Ежемесячная",
    "yearly": "Годовая",
    "balance": "Балансовый сервис",
}

BALANCE_MODE_LABELS = {
    "manual": "Ручной контроль",
    "fixed": "Фиксированное списание",
    "daily_avg": "Средний расход в день",
}

KIND_KEYBOARD = ReplyKeyboardMarkup(
    [["Ежемесячная", "Годовая"], ["Балансовый сервис"], ["/cancel"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

BALANCE_MODE_KEYBOARD = ReplyKeyboardMarkup(
    [["Ручной контроль"], ["Фиксированное списание"], ["Средний расход в день"], ["/cancel"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

YES_SKIP_KEYBOARD = ReplyKeyboardMarkup(
    [["-", "/cancel"]], resize_keyboard=True, one_time_keyboard=True
)

CURRENCY_ALIASES = {
    "KZT": "KZT",
    "₸": "KZT",
    "ТЕНГЕ": "KZT",
    "ТЕНГЕ КАЗАХСТАН": "KZT",
    "₸ ТЕНГЕ": "KZT",
    "RUB": "RUB",
    "₽": "RUB",
    "РУБ": "RUB",
    "РУБЛЬ": "RUB",
    "РУБЛИ": "RUB",
    "₽ РУБЛИ": "RUB",
    "EUR": "EUR",
    "€": "EUR",
    "ЕВРО": "EUR",
    "€ ЕВРО": "EUR",
    "USD": "USD",
    "$": "USD",
    "ДОЛЛАР": "USD",
    "ДОЛЛАРЫ": "USD",
    "$ ДОЛЛАРЫ": "USD",
    "TRY": "TRY",
    "₺": "TRY",
    "ЛИРА": "TRY",
    "ЛИРЫ": "TRY",
    "₺ ЛИРЫ": "TRY",
}

CURRENCY_KEYBOARD = ReplyKeyboardMarkup(
    [["₸ Тенге", "₽ Рубли", "€ Евро"], ["$ Доллары", "₺ Лиры"], ["/cancel"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

(
    ADD_NAME,
    ADD_KIND,
    ADD_AMOUNT,
    ADD_CURRENCY,
    ADD_PROJECT,
    ADD_NEXT_DATE,
    ADD_REMIND_DAYS,
    ADD_CURRENT_BALANCE,
    ADD_MIN_BALANCE,
    ADD_BALANCE_MODE,
    ADD_SPEND_AMOUNT,
    ADD_SPEND_PERIOD,
    ADD_NOTES,
    PAY_SELECT,
    PAY_AMOUNT,
    PAY_BALANCE,
    BALANCE_SELECT,
    BALANCE_VALUE,
) = range(18)


@dataclass
class Subscription:
    id: str
    name: str
    kind: str
    amount: float
    currency: str
    project: str
    notes: str
    created_at: datetime
    active: bool = True
    next_charge_date: Optional[date] = None
    remind_before_days: int = 3
    current_balance: Optional[float] = None
    min_balance: Optional[float] = None
    balance_updated_at: Optional[date] = None
    spending_mode: Optional[str] = None
    spend_amount: Optional[float] = None
    spend_period_days: Optional[int] = None


@dataclass
class ExpenseEvent:
    timestamp: datetime
    subscription_id: str
    subscription_name: str
    amount: float
    currency: str
    project: str
    event_type: str
    note: str = ""


@dataclass
class UserStore:
    user_id: int
    chat_id: Optional[int] = None
    subscriptions: Dict[str, Subscription] = field(default_factory=dict)
    history: List[ExpenseEvent] = field(default_factory=list)
    sent_alerts: Dict[str, date] = field(default_factory=dict)


@dataclass
class UserActivity:
    user_id: int
    username: str = ""
    full_name: str = ""
    chat_id: Optional[int] = None
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None
    message_count: int = 0
    command_count: int = 0
    callback_count: int = 0
    start_count: int = 0
    last_action: str = ""


RUNTIME_USERS: Dict[int, UserStore] = {}
USER_ACTIVITY_LOG: Dict[int, UserActivity] = {}


def now_local() -> datetime:
    return datetime.now(TZ)


def today_local() -> date:
    return now_local().date()


def get_store(user_id: int, chat_id: Optional[int] = None) -> UserStore:
    store = RUNTIME_USERS.get(user_id)
    if store is None:
        store = UserStore(user_id=user_id, chat_id=chat_id)
        RUNTIME_USERS[user_id] = store
    elif chat_id is not None:
        store.chat_id = chat_id
    return store


def get_user_activity(user_id: int) -> UserActivity:
    activity = USER_ACTIVITY_LOG.get(user_id)
    if activity is None:
        activity = UserActivity(user_id=user_id)
        USER_ACTIVITY_LOG[user_id] = activity
    return activity


def track_user_activity(update: Update) -> None:
    user = update.effective_user
    if user is None:
        return

    activity = get_user_activity(user.id)
    now = now_local()
    if activity.first_seen_at is None:
        activity.first_seen_at = now
    activity.last_seen_at = now
    activity.username = user.username or ""
    activity.full_name = user.full_name or str(user.id)

    chat = update.effective_chat
    if chat is not None:
        activity.chat_id = chat.id

    if update.callback_query is not None:
        activity.callback_count += 1
        activity.last_action = update.callback_query.data or "callback"
        return

    message = update.effective_message
    if message is None or message.text is None:
        return

    activity.message_count += 1
    raw_text = message.text.strip()
    activity.last_action = raw_text[:80]
    if raw_text.startswith('/'):
        activity.command_count += 1
        command = raw_text.split()[0].lower()
        if command == '/start':
            activity.start_count += 1


def format_dt(value: Optional[datetime]) -> str:
    if value is None:
        return '—'
    return value.strftime('%d.%m.%Y %H:%M')


def format_money(value: float, currency: str) -> str:
    if abs(value - int(value)) < 1e-9:
        return f"{int(value)} {currency}"
    return f"{value:.2f} {currency}"


def format_optional_money(value: Optional[float], currency: str) -> str:
    if value is None:
        return "—"
    return format_money(value, currency)


def add_months(source: date, months: int) -> date:
    month_index = source.month - 1 + months
    year = source.year + month_index // 12
    month = month_index % 12 + 1
    day = min(source.day, monthrange(year, month)[1])
    return date(year, month, day)


def add_years(source: date, years: int) -> date:
    year = source.year + years
    day = min(source.day, monthrange(year, source.month)[1])
    return date(year, source.month, day)


def advance_next_charge(subscription: Subscription) -> None:
    base = subscription.next_charge_date or today_local()
    if subscription.kind == "monthly":
        subscription.next_charge_date = add_months(base, 1)
    elif subscription.kind == "yearly":
        subscription.next_charge_date = add_years(base, 1)


def parse_float(text: str) -> Optional[float]:
    cleaned = text.strip().replace(",", ".")
    try:
        value = float(cleaned)
    except ValueError:
        return None
    return value if value >= 0 else None


def parse_currency_input(text: str) -> Optional[str]:
    normalized = " ".join(text.strip().upper().split())
    return CURRENCY_ALIASES.get(normalized)


def parse_int(text: str) -> Optional[int]:
    cleaned = text.strip()
    if not cleaned.isdigit():
        return None
    return int(cleaned)


def parse_date_input(text: str) -> Optional[date]:
    raw = text.strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_hhmm(value: str) -> time:
    hour_str, minute_str = value.split(":", maxsplit=1)
    return time(hour=int(hour_str), minute=int(minute_str), tzinfo=TZ)


def get_kind_from_label(label: str) -> Optional[str]:
    mapping = {
        "Ежемесячная": "monthly",
        "Годовая": "yearly",
        "Балансовый сервис": "balance",
    }
    return mapping.get(label.strip())


def get_balance_mode_from_label(label: str) -> Optional[str]:
    mapping = {
        "Ручной контроль": "manual",
        "Фиксированное списание": "fixed",
        "Средний расход в день": "daily_avg",
    }
    return mapping.get(label.strip())


def current_month_bounds() -> tuple[datetime, datetime]:
    now = now_local()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    if start.month == 12:
        end = start.replace(year=start.year + 1, month=1)
    else:
        end = start.replace(month=start.month + 1)
    return start, end


def summarize_amounts(events: List[ExpenseEvent]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for event in events:
        totals[event.currency] = totals.get(event.currency, 0.0) + event.amount
    return totals


def summarize_by_project(events: List[ExpenseEvent]) -> Dict[str, Dict[str, float]]:
    grouped: Dict[str, Dict[str, float]] = {}
    for event in events:
        grouped.setdefault(event.project, {})
        grouped[event.project][event.currency] = (
            grouped[event.project].get(event.currency, 0.0) + event.amount
        )
    return grouped


def format_currency_totals(totals: Dict[str, float]) -> str:
    if not totals:
        return "0"
    parts = [format_money(amount, currency) for currency, amount in sorted(totals.items())]
    return " / ".join(parts)


def effective_balance(subscription: Subscription, on_date: Optional[date] = None) -> Optional[float]:
    if subscription.kind != "balance":
        return None
    if subscription.current_balance is None:
        return None
    if subscription.spending_mode in (None, "manual"):
        return subscription.current_balance

    start_date = subscription.balance_updated_at or today_local()
    target_date = on_date or today_local()
    days_elapsed = max(0, (target_date - start_date).days)

    if subscription.spending_mode == "daily_avg":
        spend = subscription.spend_amount or 0.0
        return subscription.current_balance - spend * days_elapsed

    if subscription.spending_mode == "fixed":
        spend = subscription.spend_amount or 0.0
        period_days = subscription.spend_period_days or 0
        if spend <= 0 or period_days <= 0:
            return subscription.current_balance
        charges_count = days_elapsed // period_days
        return subscription.current_balance - spend * charges_count

    return subscription.current_balance


def next_fixed_charge_date(subscription: Subscription, on_date: Optional[date] = None) -> Optional[date]:
    if subscription.kind != "balance" or subscription.spending_mode != "fixed":
        return None
    if not subscription.spend_period_days or subscription.spend_period_days <= 0:
        return None
    start_date = subscription.balance_updated_at or today_local()
    target_date = on_date or today_local()
    days_elapsed = max(0, (target_date - start_date).days)
    charges_count = days_elapsed // subscription.spend_period_days
    return start_date + timedelta(days=(charges_count + 1) * subscription.spend_period_days)


def days_until_balance_threshold(subscription: Subscription, on_date: Optional[date] = None) -> Optional[int]:
    if subscription.kind != "balance" or subscription.min_balance is None:
        return None
    balance_now = effective_balance(subscription, on_date)
    if balance_now is None:
        return None
    if balance_now <= subscription.min_balance:
        return 0

    if subscription.spending_mode == "daily_avg":
        spend = subscription.spend_amount or 0.0
        if spend <= 0:
            return None
        return max(0, math.ceil((balance_now - subscription.min_balance) / spend))

    if subscription.spending_mode == "fixed":
        spend = subscription.spend_amount or 0.0
        period_days = subscription.spend_period_days or 0
        if spend <= 0 or period_days <= 0:
            return None
        charges_needed = max(
            1,
            math.ceil((balance_now - subscription.min_balance) / spend),
        )
        next_charge = next_fixed_charge_date(subscription, on_date)
        if next_charge is None:
            return None
        days_to_next_charge = max(0, (next_charge - (on_date or today_local())).days)
        return days_to_next_charge + (charges_needed - 1) * period_days

    return None


def projected_threshold_date(subscription: Subscription, on_date: Optional[date] = None) -> Optional[date]:
    days_left = days_until_balance_threshold(subscription, on_date)
    if days_left is None:
        return None
    return (on_date or today_local()) + timedelta(days=days_left)


def spending_summary(subscription: Subscription) -> str:
    if subscription.kind != "balance":
        return "—"
    mode = subscription.spending_mode or "manual"
    if mode == "manual":
        return "Ручной контроль"
    if mode == "daily_avg":
        spend = subscription.spend_amount or 0.0
        return f"~{format_money(spend, subscription.currency)} в день"
    if mode == "fixed":
        spend = subscription.spend_amount or 0.0
        period_days = subscription.spend_period_days or 0
        return f"{format_money(spend, subscription.currency)} каждые {period_days} дн."
    return "—"


def balance_projection_lines(subscription: Subscription) -> List[str]:
    if subscription.kind != "balance":
        return []

    lines = [f"Модель расхода: {BALANCE_MODE_LABELS.get(subscription.spending_mode or 'manual', 'Ручной контроль')}"]

    if subscription.spending_mode in {"fixed", "daily_avg"}:
        lines.append(f"Прогнозный расход: {spending_summary(subscription)}")

    if subscription.balance_updated_at is not None:
        lines.append(f"Точка отсчёта баланса: {subscription.balance_updated_at.strftime('%d.%m.%Y')}")

    days_left = days_until_balance_threshold(subscription)
    threshold_date = projected_threshold_date(subscription)

    if subscription.spending_mode == "fixed":
        next_charge = next_fixed_charge_date(subscription)
        if next_charge is not None:
            lines.append(f"Следующее прогнозное списание: {next_charge.strftime('%d.%m.%Y')}")

    if threshold_date is not None:
        if days_left == 0:
            lines.append("Порог уже достигнут или ниже")
        else:
            lines.append(
                f"До порога примерно: {days_left} дн. ({threshold_date.strftime('%d.%m.%Y')})"
            )

    return lines


def subscription_status(subscription: Subscription) -> str:
    if not subscription.active:
        return "⏸ Пауза"
    if subscription.kind == "balance":
        current_balance = effective_balance(subscription)
        if (
            current_balance is not None
            and subscription.min_balance is not None
            and current_balance <= subscription.min_balance
        ):
            return "🔴 Нужно пополнить"
        days_left = days_until_balance_threshold(subscription)
        if days_left is not None and 0 < days_left <= BALANCE_WARNING_DAYS:
            return f"🟡 До порога ~{days_left} дн."
        return "🟢 Активна"
    if subscription.next_charge_date is None:
        return "🟢 Активна"
    delta = (subscription.next_charge_date - today_local()).days
    if delta < 0:
        return f"🔴 Просрочено на {abs(delta)} дн."
    if delta == 0:
        return "🟠 Списание сегодня"
    if delta <= subscription.remind_before_days:
        return f"🟡 Скоро списание ({delta} дн.)"
    return "🟢 Активна"


def render_subscription(subscription: Subscription) -> str:
    lines = [
        f"<b>{escape(subscription.name)}</b> [{subscription.id}]",
        f"Тип: {KIND_LABELS[subscription.kind]}",
        f"Проект: {escape(subscription.project)}",
        f"Статус: {subscription_status(subscription)}",
    ]
    if subscription.kind == "balance":
        lines.append(
            f"Обычное пополнение: {format_money(subscription.amount, subscription.currency)}"
        )
        lines.append(
            "Баланс: "
            f"{format_optional_money(effective_balance(subscription), subscription.currency)}"
            f" | Мин. порог: {format_optional_money(subscription.min_balance, subscription.currency)}"
        )
        lines.extend(balance_projection_lines(subscription))
    else:
        lines.append(
            f"Стоимость: {format_money(subscription.amount, subscription.currency)}"
        )
        if subscription.next_charge_date is not None:
            lines.append(
                f"Следующее списание: {subscription.next_charge_date.strftime('%d.%m.%Y')}"
            )
        lines.append(f"Напомнить за: {subscription.remind_before_days} дн.")
    if subscription.notes:
        lines.append(f"Заметка: {escape(subscription.notes)}")
    return "\n".join(lines)


def history_for_month(store: UserStore) -> List[ExpenseEvent]:
    start, end = current_month_bounds()
    return [event for event in store.history if start <= event.timestamp < end]


def active_subscriptions(store: UserStore) -> List[Subscription]:
    return [subscription for subscription in store.subscriptions.values() if subscription.active]


def balance_subscriptions(store: UserStore) -> List[Subscription]:
    return [
        subscription
        for subscription in store.subscriptions.values()
        if subscription.kind == "balance"
    ]


def low_balance_subscriptions(store: UserStore) -> List[Subscription]:
    result = []
    for subscription in balance_subscriptions(store):
        if not subscription.active:
            continue
        current_balance = effective_balance(subscription)
        if (
            current_balance is not None
            and subscription.min_balance is not None
            and current_balance <= subscription.min_balance
        ):
            result.append(subscription)
    return result


def balance_warning_subscriptions(store: UserStore, days: int = BALANCE_WARNING_DAYS) -> List[Subscription]:
    result = []
    for subscription in balance_subscriptions(store):
        if not subscription.active:
            continue
        current_balance = effective_balance(subscription)
        if current_balance is None or subscription.min_balance is None:
            continue
        if current_balance <= subscription.min_balance:
            continue
        days_left = days_until_balance_threshold(subscription)
        if days_left is not None and 0 < days_left <= days:
            result.append(subscription)
    result.sort(key=lambda item: days_until_balance_threshold(item) or 10**9)
    return result


def upcoming_subscriptions(store: UserStore, days: int = SOON_DAYS) -> List[Subscription]:
    result = []
    today = today_local()
    for subscription in store.subscriptions.values():
        if not subscription.active or subscription.kind == "balance":
            continue
        if subscription.next_charge_date is None:
            continue
        delta = (subscription.next_charge_date - today).days
        if delta <= days:
            result.append(subscription)
    result.sort(key=lambda item: item.next_charge_date or today)
    return result


def record_history(
    store: UserStore,
    subscription: Subscription,
    amount: float,
    event_type: str,
    note: str = "",
) -> None:
    store.history.append(
        ExpenseEvent(
            timestamp=now_local(),
            subscription_id=subscription.id,
            subscription_name=subscription.name,
            amount=amount,
            currency=subscription.currency,
            project=subscription.project,
            event_type=event_type,
            note=note,
        )
    )


def build_inline_actions(subscription: Subscription) -> InlineKeyboardMarkup:
    pause_text = "Возобновить" if not subscription.active else "Пауза"
    pause_action = "resume" if not subscription.active else "pause"
    rows = [
        [InlineKeyboardButton("💸 Оплатить", callback_data=f"pay:{subscription.id}")],
        [
            InlineKeyboardButton(f"⏯ {pause_text}", callback_data=f"{pause_action}:{subscription.id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"delete:{subscription.id}"),
        ],
    ]
    if subscription.kind == "balance":
        rows.insert(
            1,
            [
                InlineKeyboardButton(
                    "🪫 Обновить баланс", callback_data=f"setbalance:{subscription.id}"
                )
            ],
        )
    return InlineKeyboardMarkup(rows)


async def ensure_authorized(update: Update) -> bool:
    user = update.effective_user
    if user is None:
        return False
    if OWNER_USER_ID is None or user.id == OWNER_USER_ID:
        return True
    text = "Этот бот закрыт для чужих аккаунтов. Укажи OWNER_USER_ID в .env для приватного режима."
    if update.message:
        await update.message.reply_text(text)
    elif update.callback_query:
        await update.callback_query.answer(text, show_alert=True)
    return False


async def ensure_owner_access(update: Update) -> bool:
    user = update.effective_user
    if user is None:
        return False
    if OWNER_USER_ID is not None and user.id != OWNER_USER_ID:
        text = 'Эта команда доступна только владельцу бота.'
        if update.message:
            await update.message.reply_text(text, reply_markup=MENU)
        elif update.callback_query:
            await update.callback_query.answer(text, show_alert=True)
        return False
    return True


async def usage_message_tracker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    track_user_activity(update)


async def usage_callback_tracker(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    track_user_activity(update)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    message = (
        "Привет. Я помогу держать под контролем все платные сервисы и подписки.\n\n"
        "Что умею:\n"
        "• хранить подписки и балансовые сервисы\n"
        "• напоминать о ближайших списаниях\n"
        "• прогнозировать падение баланса по модели расхода\n"
        "• сигналить, когда баланс ниже порога или скоро упрётся в него\n"
        "• фиксировать оплаты и пополнения\n"
        "• показывать сводку, отчёт и историю трат\n\n"
        "Важно: данные хранятся только в памяти процесса. После перезапуска всё сбросится."
    )
    if not store.subscriptions:
        message += "\n\nДля быстрого теста можешь запустить /demo."
    await update.message.reply_text(message, reply_markup=MENU)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    text = (
        "Команды:\n"
        "/start — главное меню\n"
        "/add — добавить подписку\n"
        "/list — показать подписки\n"
        "/soon — ближайшие списания\n"
        "/topup — сервисы с низким балансом и прогнозом\n"
        "/pay — отметить оплату или пополнение\n"
        "/setbalance — обновить текущий баланс\n"
        "/dashboard — общая сводка\n"
        "/report — отчёт за текущий месяц\n"
        "/history — история последних трат\n"
        "/users — кто пользуется ботом\n"
        "/demo — добавить демо-набор подписок\n"
        "/cancel — отменить текущий диалог"
    )
    await update.message.reply_text(text, reply_markup=MENU)


async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    if not await ensure_owner_access(update):
        return

    if not USER_ACTIVITY_LOG:
        await update.message.reply_text("Пока нет данных по пользователям.", reply_markup=MENU)
        return

    activities = sorted(
        USER_ACTIVITY_LOG.values(),
        key=lambda item: item.last_seen_at or datetime.min.replace(tzinfo=TZ),
        reverse=True,
    )
    today = today_local()
    active_today = sum(1 for item in activities if item.last_seen_at and item.last_seen_at.date() == today)
    total_messages = sum(item.message_count for item in activities)
    total_commands = sum(item.command_count for item in activities)
    lines = [
        "<b>Пользователи бота</b>",
        f"Всего пользователей: {len(activities)}",
        f"Активны сегодня: {active_today}",
        f"Всего сообщений: {total_messages}",
        f"Всего команд: {total_commands}",
        "",
    ]

    for index, item in enumerate(activities, start=1):
        store = RUNTIME_USERS.get(item.user_id)
        subscriptions_count = len(store.subscriptions) if store else 0
        username_display = "@" + item.username if item.username else "—"
        lines.extend([
            f"<b>{index}. {escape(item.full_name or str(item.user_id))}</b>",
            f"ID: <code>{item.user_id}</code> | username: {escape(username_display)}",
            f"Первый вход: {format_dt(item.first_seen_at)}",
            f"Последняя активность: {format_dt(item.last_seen_at)}",
            f"Сообщений: {item.message_count} | Команд: {item.command_count} | Callback: {item.callback_count} | /start: {item.start_count}",
            f"Подписок в боте: {subscriptions_count}",
            f"Последнее действие: {escape(item.last_action or '—')}",
            "",
        ])

    await update.message.reply_text(
        "\n".join(lines).rstrip(),
        parse_mode=ParseMode.HTML,
        reply_markup=MENU,
    )


async def demo_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)

    demo_suffix = uuid4().hex[:4]
    today = today_local()
    samples = [
        Subscription(
            id=uuid4().hex[:8],
            name=f"Hetzner VPS #{demo_suffix}",
            kind="monthly",
            amount=10,
            currency="USD",
            project="Bot A",
            notes="Демо ежемесячной подписки",
            created_at=now_local(),
            next_charge_date=today + timedelta(days=3),
            remind_before_days=3,
        ),
        Subscription(
            id=uuid4().hex[:8],
            name=f"OpenAI #{demo_suffix}",
            kind="balance",
            amount=30,
            currency="USD",
            project="Все проекты",
            notes="Демо балансового сервиса с ежедневным расходом",
            created_at=now_local(),
            current_balance=25,
            min_balance=10,
            balance_updated_at=today,
            spending_mode="daily_avg",
            spend_amount=3.5,
        ),
        Subscription(
            id=uuid4().hex[:8],
            name=f"Интернет #{demo_suffix}",
            kind="balance",
            amount=1000,
            currency="RUB",
            project="Личное",
            notes="Демо фиксированного списания",
            created_at=now_local(),
            current_balance=850,
            min_balance=100,
            balance_updated_at=today,
            spending_mode="fixed",
            spend_amount=250,
            spend_period_days=7,
        ),
        Subscription(
            id=uuid4().hex[:8],
            name=f"Домен example{demo_suffix}.site",
            kind="yearly",
            amount=12,
            currency="USD",
            project="Bot A",
            notes="Демо годовой подписки",
            created_at=now_local(),
            next_charge_date=today + timedelta(days=25),
            remind_before_days=14,
        ),
    ]
    for subscription in samples:
        store.subscriptions[subscription.id] = subscription

    await update.message.reply_text(
        "Добавил демо-набор с месячной подпиской, годовым сервисом и двумя балансовыми моделями.",
        reply_markup=MENU,
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("pending_subscription", None)
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    context.user_data.pop("pending_balance_id", None)
    await update.message.reply_text("Диалог отменён.", reply_markup=MENU)
    return ConversationHandler.END


async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    context.user_data["pending_subscription"] = {}
    await update.message.reply_text(
        "Введи название сервиса.\nНапример: OpenAI или Hetzner VPS",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_NAME


async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip()
    if len(name) < 2:
        await update.message.reply_text("Название слишком короткое. Попробуй ещё раз.")
        return ADD_NAME
    context.user_data["pending_subscription"]["name"] = name
    await update.message.reply_text("Выбери тип сервиса:", reply_markup=KIND_KEYBOARD)
    return ADD_KIND


async def add_kind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    kind = get_kind_from_label(update.message.text)
    if kind is None:
        await update.message.reply_text("Нажми одну из кнопок с типом.", reply_markup=KIND_KEYBOARD)
        return ADD_KIND
    context.user_data["pending_subscription"]["kind"] = kind
    prompt = "Введи стоимость подписки." if kind != "balance" else "Введи обычную сумму пополнения."
    await update.message.reply_text(
        f"{prompt}\nНапример: 10 или 29.99",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_AMOUNT


async def add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount = parse_float(update.message.text)
    if amount is None:
        await update.message.reply_text("Нужна положительная сумма. Например: 15.5")
        return ADD_AMOUNT
    context.user_data["pending_subscription"]["amount"] = amount
    await update.message.reply_text(
        "Выбери одну из доступных валют: KZT, RUB, EUR, USD или TRY.",
        reply_markup=CURRENCY_KEYBOARD,
    )
    return ADD_CURRENCY


async def add_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    currency = parse_currency_input(update.message.text)
    if currency is None:
        await update.message.reply_text(
            "Выбери одну из доступных валют: KZT, RUB, EUR, USD или TRY.",
            reply_markup=CURRENCY_KEYBOARD,
        )
        return ADD_CURRENCY
    context.user_data["pending_subscription"]["currency"] = currency
    await update.message.reply_text(
        "Введи проект или группу, к которой относится сервис.\nНапример: Bot A, Все проекты, Личное",
        reply_markup=ReplyKeyboardMarkup(
            [["Все проекты", "Личное"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True
        ),
    )
    return ADD_PROJECT


async def add_project(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    project = update.message.text.strip()
    if len(project) < 1:
        await update.message.reply_text("Проект не должен быть пустым.")
        return ADD_PROJECT
    pending = context.user_data["pending_subscription"]
    pending["project"] = project
    if pending["kind"] == "balance":
        await update.message.reply_text(
            "Введи текущий баланс сервиса.",
            reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
        )
        return ADD_CURRENT_BALANCE
    await update.message.reply_text(
        "Введи дату следующего списания.\nФорматы: DD.MM.YYYY или YYYY-MM-DD",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_NEXT_DATE


async def add_next_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = parse_date_input(update.message.text)
    if value is None:
        await update.message.reply_text("Не смог распознать дату. Пример: 28.03.2026")
        return ADD_NEXT_DATE
    context.user_data["pending_subscription"]["next_charge_date"] = value
    await update.message.reply_text(
        "За сколько дней до списания напоминать?\nНапример: 3",
        reply_markup=ReplyKeyboardMarkup([["3", "7", "14"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_REMIND_DAYS


async def add_remind_days(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    days = parse_int(update.message.text)
    if days is None:
        await update.message.reply_text("Введи целое число. Например: 3")
        return ADD_REMIND_DAYS
    context.user_data["pending_subscription"]["remind_before_days"] = days
    await update.message.reply_text(
        "Добавь заметку или отправь '-' чтобы пропустить.",
        reply_markup=YES_SKIP_KEYBOARD,
    )
    return ADD_NOTES


async def add_current_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    balance = parse_float(update.message.text)
    if balance is None:
        await update.message.reply_text("Введи корректное число. Например: 12.5")
        return ADD_CURRENT_BALANCE
    pending = context.user_data["pending_subscription"]
    pending["current_balance"] = balance
    pending["balance_updated_at"] = today_local()
    await update.message.reply_text(
        "Введи минимальный порог. Когда баланс станет меньше или равен этому значению, я напомню о пополнении.",
        reply_markup=ReplyKeyboardMarkup([["10", "/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_MIN_BALANCE


async def add_min_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    minimum = parse_float(update.message.text)
    if minimum is None:
        await update.message.reply_text("Введи корректное число. Например: 10")
        return ADD_MIN_BALANCE
    context.user_data["pending_subscription"]["min_balance"] = minimum
    await update.message.reply_text(
        "Выбери тип контроля расхода для баланса:",
        reply_markup=BALANCE_MODE_KEYBOARD,
    )
    return ADD_BALANCE_MODE


async def add_balance_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    mode = get_balance_mode_from_label(update.message.text)
    if mode is None:
        await update.message.reply_text(
            "Выбери один из вариантов кнопкой ниже.",
            reply_markup=BALANCE_MODE_KEYBOARD,
        )
        return ADD_BALANCE_MODE

    pending = context.user_data["pending_subscription"]
    pending["spending_mode"] = mode

    if mode == "manual":
        await update.message.reply_text(
            "Добавь заметку или отправь '-' чтобы пропустить.",
            reply_markup=YES_SKIP_KEYBOARD,
        )
        return ADD_NOTES

    if mode == "fixed":
        await update.message.reply_text(
            "Введи сумму одного списания.\nНапример: 250",
            reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
        )
        return ADD_SPEND_AMOUNT

    await update.message.reply_text(
        "Введи средний расход в день.\nНапример: 20",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return ADD_SPEND_AMOUNT


async def add_spend_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = parse_float(update.message.text)
    if value is None:
        await update.message.reply_text("Введи корректную сумму. Например: 20")
        return ADD_SPEND_AMOUNT

    pending = context.user_data["pending_subscription"]
    pending["spend_amount"] = value

    if pending.get("spending_mode") == "fixed":
        await update.message.reply_text(
            "Введи период списания в днях.\nНапример: 30",
            reply_markup=ReplyKeyboardMarkup([["7", "30", "/cancel"]], resize_keyboard=True, one_time_keyboard=True),
        )
        return ADD_SPEND_PERIOD

    pending["spend_period_days"] = 1
    await update.message.reply_text(
        "Добавь заметку или отправь '-' чтобы пропустить.",
        reply_markup=YES_SKIP_KEYBOARD,
    )
    return ADD_NOTES


async def add_spend_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = parse_int(update.message.text)
    if value is None or value <= 0:
        await update.message.reply_text("Введи положительное число дней. Например: 30")
        return ADD_SPEND_PERIOD
    context.user_data["pending_subscription"]["spend_period_days"] = value
    await update.message.reply_text(
        "Добавь заметку или отправь '-' чтобы пропустить.",
        reply_markup=YES_SKIP_KEYBOARD,
    )
    return ADD_NOTES


async def add_notes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    pending = context.user_data.pop("pending_subscription", {})

    notes = "" if update.message.text.strip() == "-" else update.message.text.strip()
    subscription = Subscription(
        id=uuid4().hex[:8],
        name=pending["name"],
        kind=pending["kind"],
        amount=pending["amount"],
        currency=pending["currency"],
        project=pending["project"],
        notes=notes,
        created_at=now_local(),
        next_charge_date=pending.get("next_charge_date"),
        remind_before_days=pending.get("remind_before_days", 3),
        current_balance=pending.get("current_balance"),
        min_balance=pending.get("min_balance"),
        balance_updated_at=pending.get("balance_updated_at"),
        spending_mode=pending.get("spending_mode"),
        spend_amount=pending.get("spend_amount"),
        spend_period_days=pending.get("spend_period_days"),
    )
    store.subscriptions[subscription.id] = subscription

    await update.message.reply_text(
        "Подписка сохранена:\n\n" + render_subscription(subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=MENU,
    )
    return ConversationHandler.END


async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    if not store.subscriptions:
        await update.message.reply_text("Подписок пока нет. Добавь первую через /add.", reply_markup=MENU)
        return

    active = [sub for sub in store.subscriptions.values() if sub.active]
    paused = [sub for sub in store.subscriptions.values() if not sub.active]

    await update.message.reply_text(
        f"Всего подписок: {len(store.subscriptions)} | Активных: {len(active)} | На паузе: {len(paused)}",
        reply_markup=MENU,
    )
    for subscription in sorted(
        store.subscriptions.values(), key=lambda item: (not item.active, item.name.lower())
    ):
        await update.message.reply_text(
            render_subscription(subscription),
            parse_mode=ParseMode.HTML,
            reply_markup=build_inline_actions(subscription),
        )


async def soon_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscriptions = upcoming_subscriptions(store, SOON_DAYS)
    if not subscriptions:
        await update.message.reply_text(
            f"В ближайшие {SOON_DAYS} дней списаний нет.", reply_markup=MENU
        )
        return
    await update.message.reply_text(
        f"Списания в ближайшие {SOON_DAYS} дней:", reply_markup=MENU
    )
    today = today_local()
    for subscription in subscriptions:
        delta = (subscription.next_charge_date - today).days if subscription.next_charge_date else 0
        if delta < 0:
            extra = f"Просрочено на {abs(delta)} дн."
        elif delta == 0:
            extra = "Списание сегодня"
        else:
            extra = f"Через {delta} дн."
        text = render_subscription(subscription) + f"\n{extra}"
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=build_inline_actions(subscription),
        )


async def topup_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscriptions = balance_subscriptions(store)
    if not subscriptions:
        await update.message.reply_text(
            "Балансовых сервисов пока нет. Добавь их через /add.", reply_markup=MENU
        )
        return
    subscriptions.sort(
        key=lambda item: (
            effective_balance(item) is None,
            effective_balance(item) if effective_balance(item) is not None else 10**9,
        )
    )
    warning = balance_warning_subscriptions(store, BALANCE_WARNING_DAYS)
    warning_ids = {sub.id for sub in warning}
    await update.message.reply_text(
        f"Балансовые сервисы. Отдельно отмечены те, что дойдут до порога примерно за {BALANCE_WARNING_DAYS} дн. или уже ниже него:",
        reply_markup=MENU,
    )
    for subscription in subscriptions:
        prefix = "⏰ Скоро порог\n" if subscription.id in warning_ids else ""
        await update.message.reply_text(
            prefix + render_subscription(subscription),
            parse_mode=ParseMode.HTML,
            reply_markup=build_inline_actions(subscription),
        )


EVENT_TYPE_LABELS = {
    "payment": "оплата",
    "topup": "пополнение",
}


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    if not store.history:
        await update.message.reply_text("История пока пустая.", reply_markup=MENU)
        return
    lines = ["Последние траты:"]
    for event in sorted(store.history, key=lambda item: item.timestamp, reverse=True)[:15]:
        label = EVENT_TYPE_LABELS.get(event.event_type, event.event_type)
        lines.append(
            f"• {event.timestamp.strftime('%d.%m %H:%M')} — {escape(event.subscription_name)} — "
            f"{format_money(event.amount, event.currency)} ({label})"
        )
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU
    )


async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)

    total = len(store.subscriptions)
    active_total = len(active_subscriptions(store))
    paused_total = total - active_total
    due_soon = len(upcoming_subscriptions(store, 7))
    low_balance = len(low_balance_subscriptions(store))
    forecast_warning = len(balance_warning_subscriptions(store, BALANCE_WARNING_DAYS))
    month_events = history_for_month(store)
    month_totals = summarize_amounts(month_events)
    top_soon = upcoming_subscriptions(store, 7)[:3]

    lines = [
        "<b>Сводка</b>",
        f"Подписок всего: {total}",
        f"Активных: {active_total}",
        f"На паузе: {paused_total}",
        f"Скоро списаний (7 дн.): {due_soon}",
        f"Низкий баланс: {low_balance}",
        f"До порога примерно за {BALANCE_WARNING_DAYS} дн.: {forecast_warning}",
        f"Потрачено в этом месяце: {format_currency_totals(month_totals)}",
    ]
    if top_soon:
        lines.append("\n<b>Ближайшие оплаты</b>")
        for subscription in top_soon:
            if subscription.next_charge_date is None:
                continue
            delta = (subscription.next_charge_date - today_local()).days
            lines.append(
                f"• {escape(subscription.name)} — {subscription.next_charge_date.strftime('%d.%m.%Y')} ({delta} дн.)"
            )
    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU
    )


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    events = history_for_month(store)
    if not events:
        await update.message.reply_text(
            "В этом месяце трат пока нет. После /pay здесь появится отчёт.",
            reply_markup=MENU,
        )
        return

    totals = summarize_amounts(events)
    grouped = summarize_by_project(events)

    lines = [
        f"<b>Отчёт за {now_local().strftime('%m.%Y')}</b>",
        f"Всего потрачено: {format_currency_totals(totals)}",
        f"Операций: {len(events)}",
        "\n<b>По проектам</b>",
    ]
    for project, amounts in sorted(grouped.items()):
        lines.append(f"• {escape(project)} — {format_currency_totals(amounts)}")

    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU
    )


async def pay_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscriptions = active_subscriptions(store)
    if not subscriptions:
        await update.message.reply_text("Нет активных подписок для оплаты.", reply_markup=MENU)
        return ConversationHandler.END

    buttons = [[f"{subscription.name} [{subscription.id}]"] for subscription in subscriptions]
    buttons.append(["/cancel"])
    await update.message.reply_text(
        "Выбери сервис для фиксации оплаты или пополнения:",
        reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True),
    )
    return PAY_SELECT


async def pay_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    subscription_id = query.data.split(":", maxsplit=1)[1]
    context.user_data["pending_payment_id"] = subscription_id
    await query.message.reply_text(
        "Введи сумму оплаты или пополнения.",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return PAY_AMOUNT


async def pay_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    match = re.search(r"\[([0-9a-f]{8})\]$", update.message.text.strip())
    if not match:
        await update.message.reply_text("Выбери сервис кнопкой из списка.")
        return PAY_SELECT
    context.user_data["pending_payment_id"] = match.group(1)
    await update.message.reply_text(
        "Введи сумму оплаты или пополнения.",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return PAY_AMOUNT


async def pay_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount = parse_float(update.message.text)
    if amount is None:
        await update.message.reply_text("Введи корректную сумму. Например: 25")
        return PAY_AMOUNT

    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_payment_id")
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await update.message.reply_text("Не нашёл сервис. Попробуй ещё раз через /pay.", reply_markup=MENU)
        return ConversationHandler.END

    context.user_data["pending_payment_amount"] = amount
    if subscription.kind == "balance":
        await update.message.reply_text(
            "Введи новый текущий баланс после пополнения.\n"
            "Или отправь '-' — тогда я прибавлю сумму пополнения к текущему прогнозному балансу и возьму сегодняшнюю дату как новую точку отсчёта.",
            reply_markup=YES_SKIP_KEYBOARD,
        )
        return PAY_BALANCE

    record_history(store, subscription, amount, "payment")
    advance_next_charge(subscription)
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    await update.message.reply_text(
        "Оплата сохранена.\n\n" + render_subscription(subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=MENU,
    )
    return ConversationHandler.END


async def pay_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_payment_id")
    amount = context.user_data.get("pending_payment_amount")
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await update.message.reply_text("Не нашёл сервис. Попробуй ещё раз через /pay.", reply_markup=MENU)
        return ConversationHandler.END

    raw = update.message.text.strip()
    if raw == "-":
        current = effective_balance(subscription) or 0.0
        subscription.current_balance = current + amount
    else:
        new_balance = parse_float(raw)
        if new_balance is None:
            await update.message.reply_text("Введи число или '-'.")
            return PAY_BALANCE
        subscription.current_balance = new_balance

    subscription.balance_updated_at = today_local()
    record_history(store, subscription, amount, "topup")
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    await update.message.reply_text(
        "Пополнение сохранено.\n\n" + render_subscription(subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=MENU,
    )
    return ConversationHandler.END


async def set_balance_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscriptions = [sub for sub in balance_subscriptions(store) if sub.active]
    if not subscriptions:
        await update.message.reply_text(
            "Нет активных балансовых сервисов.", reply_markup=MENU
        )
        return ConversationHandler.END

    buttons = [[f"{subscription.name} [{subscription.id}]"] for subscription in subscriptions]
    buttons.append(["/cancel"])
    await update.message.reply_text(
        "Выбери сервис, у которого нужно обновить баланс:",
        reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True),
    )
    return BALANCE_SELECT


async def set_balance_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    subscription_id = query.data.split(":", maxsplit=1)[1]
    context.user_data["pending_balance_id"] = subscription_id
    await query.message.reply_text(
        "Введи новый текущий баланс. Я сохраню его как новую точку отсчёта на сегодня.",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return BALANCE_VALUE


async def set_balance_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    match = re.search(r"\[([0-9a-f]{8})\]$", update.message.text.strip())
    if not match:
        await update.message.reply_text("Выбери сервис кнопкой из списка.")
        return BALANCE_SELECT
    context.user_data["pending_balance_id"] = match.group(1)
    await update.message.reply_text(
        "Введи новый текущий баланс. Я сохраню его как новую точку отсчёта на сегодня.",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
    )
    return BALANCE_VALUE


async def set_balance_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = parse_float(update.message.text)
    if value is None:
        await update.message.reply_text("Введи корректное число. Например: 6.5")
        return BALANCE_VALUE

    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_balance_id")
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await update.message.reply_text("Сервис не найден.", reply_markup=MENU)
        return ConversationHandler.END

    subscription.current_balance = value
    subscription.balance_updated_at = today_local()
    context.user_data.pop("pending_balance_id", None)
    await update.message.reply_text(
        "Баланс обновлён.\n\n" + render_subscription(subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=MENU,
    )
    return ConversationHandler.END


async def subscription_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    query = update.callback_query
    await query.answer()
    action, subscription_id = query.data.split(":", maxsplit=1)

    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await query.message.reply_text("Подписка не найдена.", reply_markup=MENU)
        return

    if action == "pause":
        subscription.active = False
        text = f"Поставил на паузу: {subscription.name}"
    elif action == "resume":
        subscription.active = True
        text = f"Возобновил: {subscription.name}"
    elif action == "delete":
        del store.subscriptions[subscription_id]
        text = f"Удалил: {subscription.name}"
    else:
        text = "Неизвестное действие."

    await query.message.reply_text(text, reply_markup=MENU)


async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    text = update.message.text.strip()
    routes = {
        "📋 Подписки": list_command,
        "⏰ Скоро списания": soon_command,
        "🪫 Низкий баланс": topup_command,
        "💼 Сводка": dashboard_command,
        "📈 Отчёт": report_command,
        "🧾 История": history_command,
        "⚙️ Помощь": help_command,
    }
    handler = routes.get(text)
    if handler:
        await handler(update, context)
        return
    await update.message.reply_text(
        "Не понял команду. Используй кнопки меню или /help.", reply_markup=MENU
    )


def build_alert_key(kind: str, subscription_id: str) -> str:
    return f"{kind}:{subscription_id}:{today_local().isoformat()}"


async def alerts_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    for store in RUNTIME_USERS.values():
        if store.chat_id is None:
            continue
        for subscription in store.subscriptions.values():
            if not subscription.active:
                continue
            if subscription.kind == "balance":
                current_balance = effective_balance(subscription)
                if current_balance is None or subscription.min_balance is None:
                    continue

                if current_balance <= subscription.min_balance:
                    key = build_alert_key("low", subscription.id)
                    if store.sent_alerts.get(key) == today_local():
                        continue
                    store.sent_alerts[key] = today_local()
                    await context.bot.send_message(
                        chat_id=store.chat_id,
                        text=(
                            "🪫 Баланс ниже порога\n\n" + render_subscription(subscription)
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=build_inline_actions(subscription),
                    )
                    continue

                days_left = days_until_balance_threshold(subscription)
                if days_left is not None and 0 < days_left <= BALANCE_WARNING_DAYS:
                    key = build_alert_key("balance_warn", subscription.id)
                    if store.sent_alerts.get(key) == today_local():
                        continue
                    store.sent_alerts[key] = today_local()
                    await context.bot.send_message(
                        chat_id=store.chat_id,
                        text=(
                            f"⏰ Баланс дойдёт до порога примерно через {days_left} дн.\n\n"
                            + render_subscription(subscription)
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=build_inline_actions(subscription),
                    )
                continue

            if subscription.next_charge_date is None:
                continue
            delta = (subscription.next_charge_date - today_local()).days
            if delta > subscription.remind_before_days:
                continue
            key = build_alert_key("due", subscription.id)
            if store.sent_alerts.get(key) == today_local():
                continue
            store.sent_alerts[key] = today_local()
            if delta < 0:
                prefix = f"🔴 Просрочено на {abs(delta)} дн."
            elif delta == 0:
                prefix = "🟠 Списание сегодня"
            else:
                prefix = f"⏰ До списания {delta} дн."
            await context.bot.send_message(
                chat_id=store.chat_id,
                text=prefix + "\n\n" + render_subscription(subscription),
                parse_mode=ParseMode.HTML,
                reply_markup=build_inline_actions(subscription),
            )


async def daily_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    for store in RUNTIME_USERS.values():
        if store.chat_id is None:
            continue
        total = len(store.subscriptions)
        if total == 0:
            continue
        due_soon = len(upcoming_subscriptions(store, 7))
        low_balance = len(low_balance_subscriptions(store))
        forecast_warning = len(balance_warning_subscriptions(store, BALANCE_WARNING_DAYS))
        month_totals = summarize_amounts(history_for_month(store))
        lines = [
            "<b>Ежедневная сводка</b>",
            f"Подписок всего: {total}",
            f"Скоро списаний (7 дн.): {due_soon}",
            f"Низкий баланс: {low_balance}",
            f"До порога примерно за {BALANCE_WARNING_DAYS} дн.: {forecast_warning}",
            f"Потрачено в этом месяце: {format_currency_totals(month_totals)}",
        ]
        await context.bot.send_message(
            chat_id=store.chat_id,
            text="\n".join(lines),
            parse_mode=ParseMode.HTML,
        )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    LOGGER.exception("Unhandled error: %s", context.error)
    if isinstance(update, Update):
        target = update.effective_message
        if target:
            await target.reply_text("Произошла ошибка. Попробуй ещё раз или начни с /start.", reply_markup=MENU)


def add_handlers(application: Application) -> None:
    application.add_handler(MessageHandler(filters.ALL, usage_message_tracker), group=-1)
    application.add_handler(CallbackQueryHandler(usage_callback_tracker), group=-1)

    add_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("add", add_start),
            MessageHandler(filters.Regex(r"^➕ Добавить$"), add_start),
        ],
        states={
            ADD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_name)],
            ADD_KIND: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_kind)],
            ADD_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_amount)],
            ADD_CURRENCY: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_currency)],
            ADD_PROJECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_project)],
            ADD_NEXT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_next_date)],
            ADD_REMIND_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_remind_days)],
            ADD_CURRENT_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_current_balance)],
            ADD_MIN_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_min_balance)],
            ADD_BALANCE_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_balance_mode)],
            ADD_SPEND_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_spend_amount)],
            ADD_SPEND_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_spend_period)],
            ADD_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_notes)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_subscription",
        persistent=False,
    )

    pay_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("pay", pay_start),
            MessageHandler(filters.Regex(r"^💸 Отметить оплату$"), pay_start),
            CallbackQueryHandler(pay_from_callback, pattern=r"^pay:"),
        ],
        states={
            PAY_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_select)],
            PAY_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_amount)],
            PAY_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_balance)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="pay_subscription",
        persistent=False,
    )

    balance_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("setbalance", set_balance_start),
            CallbackQueryHandler(set_balance_from_callback, pattern=r"^setbalance:"),
        ],
        states={
            BALANCE_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_balance_select)],
            BALANCE_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, set_balance_value)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="set_balance",
        persistent=False,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("demo", demo_command))
    application.add_handler(CommandHandler("users", users_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("soon", soon_command))
    application.add_handler(CommandHandler("topup", topup_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("history", history_command))
    application.add_handler(add_conversation)
    application.add_handler(pay_conversation)
    application.add_handler(balance_conversation)
    application.add_handler(
        CallbackQueryHandler(subscription_action_callback, pattern=r"^(pause|resume|delete):")
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))
    application.add_error_handler(error_handler)


def schedule_jobs(application: Application) -> None:
    if application.job_queue is None:
        return
    application.job_queue.run_daily(
        alerts_job,
        time=parse_hhmm(ALERTS_TIME),
        name="alerts_job",
    )
    application.job_queue.run_daily(
        daily_summary_job,
        time=parse_hhmm(DAILY_SUMMARY_TIME),
        name="daily_summary_job",
    )


def main() -> None:
    if not TOKEN:
        raise RuntimeError("Не задан TG_BOT_API_KEY в .env")

    defaults = Defaults(tzinfo=TZ)
    application = (
        ApplicationBuilder()
        .token(TOKEN)
        .defaults(defaults)
        .concurrent_updates(False)
        .build()
    )
    add_handlers(application)
    schedule_jobs(application)
    LOGGER.info("Bot started in timezone %s", TIMEZONE_NAME)
    application.run_polling()


if __name__ == "__main__":
    main()
