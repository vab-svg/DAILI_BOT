import csv
import json
import logging
import math
import os
import re
import warnings
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from html import escape
from io import BytesIO, StringIO
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram.warnings import PTBUserWarning
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
    Message,
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


ACTIVE_UI_MESSAGES: dict[int, int] = {}
ACTIVE_PAIRS: dict[int, dict] = {}
_ORIGINAL_MESSAGE_REPLY_TEXT = Message.reply_text


def remember_active_ui(chat_id: int, message_id: int) -> None:
    ACTIVE_UI_MESSAGES[chat_id] = message_id


def forget_active_ui(chat_id: int, message_id: int | None = None) -> None:
    if message_id is None:
        ACTIVE_UI_MESSAGES.pop(chat_id, None)
        return
    if ACTIVE_UI_MESSAGES.get(chat_id) == message_id:
        ACTIVE_UI_MESSAGES.pop(chat_id, None)


def get_pair(chat_id: int) -> dict:
    return ACTIVE_PAIRS.setdefault(chat_id, {"action_id": None, "user_message_id": None, "bot_message_ids": [], "sticky": False})


async def delete_current_pair(bot, chat_id: int) -> None:
    pair = ACTIVE_PAIRS.get(chat_id)
    if not pair:
        return
    if pair.get("sticky"):
        ACTIVE_PAIRS[chat_id] = {"action_id": None, "user_message_id": None, "bot_message_ids": [], "sticky": False}
        return
    user_message_id = pair.get("user_message_id")
    if user_message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=user_message_id)
        except Exception:
            pass
    for message_id in list(pair.get("bot_message_ids", [])):
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception:
            pass
        forget_active_ui(chat_id, message_id)
    ACTIVE_PAIRS[chat_id] = {"action_id": None, "user_message_id": None, "bot_message_ids": [], "sticky": False}


def should_sticky_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    return False


async def start_new_action(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    pair = get_pair(chat.id)
    if update.message is not None:
        action_id = f"m:{update.message.message_id}"
        if pair.get("action_id") != action_id:
            await delete_current_pair(context.bot, chat.id)
            keep_user_message = all(
                getattr(update.message, attr, None) is None
                for attr in ("document", "photo", "video", "audio", "voice", "sticker", "animation")
            )
            ACTIVE_PAIRS[chat.id] = {
                "action_id": action_id,
                "user_message_id": update.message.message_id if keep_user_message else None,
                "bot_message_ids": [],
                "sticky": should_sticky_action(update, context),
            }
    elif update.callback_query is not None:
        action_id = f"c:{update.callback_query.id}"
        if pair.get("action_id") != action_id:
            await delete_current_pair(context.bot, chat.id)
            source_message_id = update.callback_query.message.message_id if update.callback_query.message else None
            ACTIVE_PAIRS[chat.id] = {
                "action_id": action_id,
                "user_message_id": None,
                "bot_message_ids": [source_message_id] if source_message_id else [],
                "sticky": False,
            }
            if source_message_id:
                remember_active_ui(chat.id, source_message_id)


def append_pair_bot_message(chat_id: int, message_id: int) -> None:
    pair = get_pair(chat_id)
    ids = pair.setdefault("bot_message_ids", [])
    if message_id not in ids:
        ids.append(message_id)


async def _compact_reply_text(self: Message, *args, **kwargs):
    chat = self.chat
    if chat is None:
        return await _ORIGINAL_MESSAGE_REPLY_TEXT(self, *args, **kwargs)

    action_id = f"m:{self.message_id}"
    pair = get_pair(chat.id)
    if pair.get("action_id") != action_id:
        await delete_current_pair(self.get_bot(), chat.id)
        keep_user_message = all(
            getattr(self, attr, None) is None
            for attr in ("document", "photo", "video", "audio", "voice", "sticker", "animation")
        )
        ACTIVE_PAIRS[chat.id] = {
            "action_id": action_id,
            "user_message_id": self.message_id if keep_user_message else None,
            "bot_message_ids": [],
            "sticky": False,
        }

    sent = await _ORIGINAL_MESSAGE_REPLY_TEXT(self, *args, **kwargs)
    append_pair_bot_message(chat.id, sent.message_id)
    remember_active_ui(chat.id, sent.message_id)
    return sent


Message.reply_text = _compact_reply_text

# Suppress a noisy PTB warning for mixed message/callback conversations.
# The bot intentionally starts some conversations from inline buttons and continues in chat.
warnings.filterwarnings(
    "ignore",
    message=r"If 'per_message=False'.*CallbackQueryHandler.*",
    category=PTBUserWarning,
)

TOKEN = os.getenv("TG_BOT_API_KEY", "").strip()
OWNER_USER_ID_RAW = os.getenv("OWNER_USER_ID", "").strip()
OWNER_USER_ID = int(OWNER_USER_ID_RAW) if OWNER_USER_ID_RAW else None
TIMEZONE_NAME = os.getenv("TIMEZONE", "Europe/Berlin").strip() or "Europe/Berlin"
DAILY_SUMMARY_TIME = os.getenv("DAILY_SUMMARY_TIME", "09:00").strip() or "09:00"
ALERTS_TIME = os.getenv("ALERTS_TIME", "08:30").strip() or "08:30"
WEEKLY_SUMMARY_TIME = os.getenv("WEEKLY_SUMMARY_TIME", "09:10").strip() or "09:10"
WEEKLY_SUMMARY_WEEKDAY = int(os.getenv("WEEKLY_SUMMARY_WEEKDAY", "0"))
MONTHLY_SUMMARY_TIME = os.getenv("MONTHLY_SUMMARY_TIME", "09:20").strip() or "09:20"
MONTHLY_SUMMARY_DAY = int(os.getenv("MONTHLY_SUMMARY_DAY", "1"))
SOON_DAYS = int(os.getenv("SOON_DAYS", "14"))
BALANCE_WARNING_DAYS = int(os.getenv("BALANCE_WARNING_DAYS", "3"))
FORECAST_DAYS = int(os.getenv("FORECAST_DAYS", "30"))
YEAR_FORECAST_DAYS = int(os.getenv("YEAR_FORECAST_DAYS", "365"))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_FILE = os.getenv("DATA_FILE", os.path.join(BASE_DIR, "subscription_data.json")).strip() or os.path.join(BASE_DIR, "subscription_data.json")
DEFAULT_REMINDER_OFFSETS = [7, 3, 1, 0]

TZ = ZoneInfo(TIMEZONE_NAME)

MENU = ReplyKeyboardMarkup(
    [
        ["➕ Добавить", "📋 Подписки"],
        ["📅 Сегодня", "⏰ Скоро списания"],
        ["🪫 Низкий баланс", "💸 Отметить оплату"],
        ["💼 Сводка", "🔮 Прогноз"],
        ["🗓 События на год"],
        ["📈 Отчёт", "🧾 История"],
        ["🗃 Архив", "📤 Экспорт"],
        ["❓ Помощь"],
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

CATEGORY_KEYBOARD = ReplyKeyboardMarkup(
    [["Инфраструктура", "Связь"], ["AI/API", "Домены"], ["Маркетинг", "Личное"], ["Прочее", "/cancel"]],
    resize_keyboard=True,
    one_time_keyboard=True,
)

YES_SKIP_KEYBOARD = ReplyKeyboardMarkup(
    [["-", "/cancel"]], resize_keyboard=True, one_time_keyboard=True
)

YES_NO_KEYBOARD = ReplyKeyboardMarkup(
    [["Да", "Нет"], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True
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


HELP_BUTTON = "❓ Помощь"


async def safe_delete_message(message) -> None:
    if message is None:
        return
    try:
        chat_id = getattr(message, "chat_id", None)
        message_id = getattr(message, "message_id", None)
        await message.delete()
        if chat_id is not None:
            forget_active_ui(chat_id, message_id)
    except Exception:
        pass


def remember_ui_message(context: ContextTypes.DEFAULT_TYPE, message) -> None:
    if message is None:
        return
    context.user_data["ui_message_id"] = message.message_id
    context.user_data["ui_chat_id"] = message.chat_id
    remember_active_ui(message.chat_id, message.message_id)
    append_pair_bot_message(message.chat_id, message.message_id)


async def ui_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    *,
    reply_markup=None,
    parse_mode=None,
    force_new: bool = False,
) -> None:
    chat = update.effective_chat
    if chat is None:
        return

    await start_new_action(update, context)
    uses_reply_keyboard = isinstance(reply_markup, ReplyKeyboardMarkup) or reply_markup is MENU

    if force_new:
        ui_chat_id = context.user_data.get("ui_chat_id")
        ui_message_id = context.user_data.get("ui_message_id")
        if ui_chat_id == chat.id and ui_message_id:
            try:
                await context.bot.delete_message(chat_id=ui_chat_id, message_id=ui_message_id)
            except Exception:
                pass
            forget_active_ui(ui_chat_id, ui_message_id)
            context.user_data.pop("ui_chat_id", None)
            context.user_data.pop("ui_message_id", None)

    query = update.callback_query
    if query is not None and query.message is not None and not force_new and not uses_reply_keyboard:
        try:
            sent = await query.message.edit_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            remember_ui_message(context, sent)
            return
        except Exception:
            pass

    ui_chat_id = context.user_data.get("ui_chat_id")
    ui_message_id = context.user_data.get("ui_message_id")
    if ui_chat_id == chat.id and ui_message_id and not force_new and not uses_reply_keyboard:
        try:
            await context.bot.edit_message_text(
                chat_id=ui_chat_id,
                message_id=ui_message_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
            )
            return
        except Exception:
            pass

    if ui_chat_id == chat.id and ui_message_id and uses_reply_keyboard:
        try:
            await context.bot.delete_message(chat_id=ui_chat_id, message_id=ui_message_id)
        except Exception:
            pass
        forget_active_ui(ui_chat_id, ui_message_id)
        context.user_data.pop("ui_chat_id", None)
        context.user_data.pop("ui_message_id", None)

    sent = await chat.send_message(
        text=text,
        reply_markup=reply_markup,
        parse_mode=parse_mode,
        disable_web_page_preview=True,
    )
    remember_ui_message(context, sent)

def keyboard_with_help(rows: list[list[str]]) -> ReplyKeyboardMarkup:
    normalized = [row[:] for row in rows]
    if not normalized or normalized[-1] != [HELP_BUTTON, "/cancel"]:
        normalized.append([HELP_BUTTON, "/cancel"])
    return ReplyKeyboardMarkup(normalized, resize_keyboard=True, one_time_keyboard=True)


def step_text(step: int, total: int, title: str, hint: str | None = None) -> str:
    lines = [f"Шаг {step}", f"{title}:"]
    if hint:
        lines.append(hint)
    return "\n".join(lines)


def is_help_request(text: str | None) -> bool:
    return (text or "").strip().lower() in {HELP_BUTTON.lower(), "/help", "помощь"}


ADD_HELP_TEXT = {
    "name": "Как назвать сервис? Коротко и понятно: OpenAI, Spotify, МТС, VPS.",
    "kind": "Выбери формат оплаты: обычная подписка, годовая оплата или сервис с балансом.",
    "amount": "Укажи сумму числом. Без валюты и лишнего текста.",
    "currency": "Выбери валюту кнопкой или введи код: KZT, RUB, EUR, USD, TRY.",
    "project": "Проект помогает группировать подписки. Например: Личное, Работа, Bot A.",
    "category": "Категория нужна для порядка и отчётов. Можно выбрать кнопку или написать свою.",
    "tags": "Теги — короткие метки через запятую. Например: связь, ai, личное. Если не нужны, отправь -.",
    "next_date": "Укажи дату списания. Подойдут форматы ДД.ММ.ГГГГ и ГГГГ-ММ-ДД.",
    "reminders": "Напиши, за сколько дней напоминать. Например: 7,3,1,0.",
    "repeat": "Если платёж просрочен, бот может напоминать каждый день, пока ты не отметишь оплату.",
    "balance": "Укажи текущий остаток на сервисе числом.",
    "min_balance": "Когда баланс дойдёт до этого порога, бот напомнит о пополнении.",
    "balance_mode": "Ручной контроль — обновляешь баланс сам. Списание по расписанию — расход раз в N дней. Расход в день — средний дневной расход.",
    "spend_amount": "Укажи сумму списания или расхода в день. Только число.",
    "spend_period": "Через сколько дней повторяется списание. Например: 30.",
    "notes": "Короткая заметка для себя. Если не нужна, отправь -.",
    "site_url": "Ссылка на сайт или личный кабинет. Если не нужна, отправь -.",
    "payment_url": "Прямая ссылка на оплату. Если не нужна, отправь -.",
    "confirm": "Проверь карточку ниже. Можно сохранить, отменить или выбрать поле для правки.",
}



async def send_add_help(update: Update, context: ContextTypes.DEFAULT_TYPE, key: str) -> None:
    await ui_send(update, context, ADD_HELP_TEXT[key])

(
    ADD_NAME,
    ADD_KIND,
    ADD_AMOUNT,
    ADD_CURRENCY,
    ADD_PROJECT,
    ADD_CATEGORY,
    ADD_TAGS,
    ADD_NEXT_DATE,
    ADD_REMIND_DAYS,
    ADD_REPEAT_UNTIL_PAID,
    ADD_CURRENT_BALANCE,
    ADD_MIN_BALANCE,
    ADD_BALANCE_MODE,
    ADD_SPEND_AMOUNT,
    ADD_SPEND_PERIOD,
    ADD_NOTES,
    ADD_SITE_URL,
    ADD_PAYMENT_URL,
    ADD_CONFIRM,
    ADD_CONFIRM_EDIT_FIELD,
    PAY_SELECT,
    PAY_AMOUNT,
    PAY_PERIODS,
    PAY_BALANCE,
    BALANCE_SELECT,
    BALANCE_VALUE,
    EDIT_SELECT,
    EDIT_FIELD,
    EDIT_VALUE,
) = range(29)


@dataclass
class Subscription:
    id: str
    name: str
    kind: str
    amount: float
    currency: str
    project: str
    category: str = "Прочее"
    tags: List[str] = field(default_factory=list)
    notes: str = ""
    created_at: datetime = field(default_factory=datetime.now)
    active: bool = True
    next_charge_date: Optional[date] = None
    remind_before_days: int = 3
    current_balance: Optional[float] = None
    min_balance: Optional[float] = None
    balance_updated_at: Optional[date] = None
    spending_mode: Optional[str] = None
    spend_amount: Optional[float] = None
    spend_period_days: Optional[int] = None
    reminder_offsets: List[int] = field(default_factory=lambda: DEFAULT_REMINDER_OFFSETS.copy())
    repeat_daily_until_paid: bool = True
    snoozed_until: Optional[date] = None
    site_url: str = ""
    payment_url: str = ""


@dataclass
class ExpenseEvent:
    timestamp: datetime
    subscription_id: str
    subscription_name: str
    amount: float
    currency: str
    project: str
    category: str = "Прочее"
    event_type: str = "payment"
    note: str = ""


@dataclass
class UserStore:
    user_id: int
    chat_id: Optional[int] = None
    subscriptions: Dict[str, Subscription] = field(default_factory=dict)
    archived_subscriptions: Dict[str, Subscription] = field(default_factory=dict)
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


def normalize_reminder_offsets(offsets: Optional[List[int]]) -> List[int]:
    if not offsets:
        return DEFAULT_REMINDER_OFFSETS.copy()
    cleaned = sorted({int(value) for value in offsets if int(value) >= 0}, reverse=True)
    return cleaned or DEFAULT_REMINDER_OFFSETS.copy()


def format_reminder_offsets(offsets: Optional[List[int]]) -> str:
    items = normalize_reminder_offsets(offsets)
    return ", ".join(str(item) for item in items)


def parse_reminder_offsets(text: str) -> Optional[List[int]]:
    raw = text.strip().replace(" ", "")
    parts = [part for part in raw.split(",") if part != ""]
    if not parts:
        return None
    values: List[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        values.append(int(part))
    return normalize_reminder_offsets(values)


def parse_yes_no(text: str) -> Optional[bool]:
    value = text.strip().lower()
    if value in {"да", "yes", "y", "1"}:
        return True
    if value in {"нет", "no", "n", "0"}:
        return False
    return None


def parse_balance_mode_input(text: str) -> Optional[str]:
    direct = get_balance_mode_from_label(text)
    if direct is not None:
        return direct
    mapping = {
        "manual": "manual",
        "fixed": "fixed",
        "daily_avg": "daily_avg",
        "daily": "daily_avg",
        "ручной": "manual",
        "фикс": "fixed",
        "средний": "daily_avg",
    }
    return mapping.get(text.strip().lower())


def current_reminder_offsets(subscription: Subscription) -> List[int]:
    return normalize_reminder_offsets(subscription.reminder_offsets or [subscription.remind_before_days, 0])


def max_reminder_window(subscription: Subscription) -> int:
    offsets = current_reminder_offsets(subscription)
    return max(offsets) if offsets else 0


def is_snoozed(subscription: Subscription, today: Optional[date] = None) -> bool:
    target = today or today_local()
    return subscription.snoozed_until is not None and target <= subscription.snoozed_until


def date_to_iso(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def datetime_to_iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def parse_iso_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date.fromisoformat(value)


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)

def clean_optional_url(text: str) -> Optional[str]:
    raw = (text or "").strip()
    if raw in {"", "-", "—"}:
        return ""
    if not re.match(r"^https?://", raw, flags=re.IGNORECASE):
        raise ValueError("Ссылка должна начинаться с http:// или https://, либо отправь '-' чтобы пропустить.")
    return raw


def build_link_buttons(subscription: "Subscription") -> list[list[InlineKeyboardButton]]:
    rows: list[list[InlineKeyboardButton]] = []
    if subscription.payment_url:
        rows.append([InlineKeyboardButton("🌐 Оплатить", url=subscription.payment_url)])
    if subscription.site_url:
        rows.append([InlineKeyboardButton("🔗 Открыть сайт", url=subscription.site_url)])
    return rows


def subscription_to_dict(subscription: Subscription) -> dict:
    return {
        "id": subscription.id,
        "name": subscription.name,
        "kind": subscription.kind,
        "amount": subscription.amount,
        "currency": subscription.currency,
        "project": subscription.project,
        "category": subscription.category,
        "tags": subscription.tags,
        "notes": subscription.notes,
        "created_at": datetime_to_iso(subscription.created_at),
        "active": subscription.active,
        "next_charge_date": date_to_iso(subscription.next_charge_date),
        "remind_before_days": subscription.remind_before_days,
        "current_balance": subscription.current_balance,
        "min_balance": subscription.min_balance,
        "balance_updated_at": date_to_iso(subscription.balance_updated_at),
        "spending_mode": subscription.spending_mode,
        "spend_amount": subscription.spend_amount,
        "spend_period_days": subscription.spend_period_days,
        "reminder_offsets": current_reminder_offsets(subscription),
        "repeat_daily_until_paid": subscription.repeat_daily_until_paid,
        "snoozed_until": date_to_iso(subscription.snoozed_until),
        "site_url": subscription.site_url,
        "payment_url": subscription.payment_url,
    }


def subscription_from_dict(data: dict) -> Subscription:
    reminder_offsets = data.get("reminder_offsets")
    if reminder_offsets is None:
        legacy = data.get("remind_before_days", 3)
        reminder_offsets = normalize_reminder_offsets([legacy, 0])
    return Subscription(
        id=data["id"],
        name=data["name"],
        kind=data["kind"],
        amount=float(data["amount"]),
        currency=data["currency"],
        project=data.get("project", "Личное"),
        category=data.get("category", "Прочее"),
        tags=[str(item).strip() for item in data.get("tags", []) if str(item).strip()],
        notes=data.get("notes", ""),
        created_at=parse_iso_datetime(data.get("created_at")) or now_local(),
        active=bool(data.get("active", True)),
        next_charge_date=parse_iso_date(data.get("next_charge_date")),
        remind_before_days=int(data.get("remind_before_days", 3)),
        current_balance=data.get("current_balance"),
        min_balance=data.get("min_balance"),
        balance_updated_at=parse_iso_date(data.get("balance_updated_at")),
        spending_mode=data.get("spending_mode"),
        spend_amount=data.get("spend_amount"),
        spend_period_days=data.get("spend_period_days"),
        reminder_offsets=normalize_reminder_offsets(reminder_offsets),
        repeat_daily_until_paid=bool(data.get("repeat_daily_until_paid", True)),
        snoozed_until=parse_iso_date(data.get("snoozed_until")),
        site_url=data.get("site_url", "") or "",
        payment_url=data.get("payment_url", "") or "",
    )


def expense_event_to_dict(event: ExpenseEvent) -> dict:
    return {
        "timestamp": datetime_to_iso(event.timestamp),
        "subscription_id": event.subscription_id,
        "subscription_name": event.subscription_name,
        "amount": event.amount,
        "currency": event.currency,
        "project": event.project,
        "category": event.category,
        "event_type": event.event_type,
        "note": event.note,
    }


def expense_event_from_dict(data: dict) -> ExpenseEvent:
    return ExpenseEvent(
        timestamp=parse_iso_datetime(data.get("timestamp")) or now_local(),
        subscription_id=data.get("subscription_id", ""),
        subscription_name=data.get("subscription_name", ""),
        amount=float(data.get("amount", 0)),
        currency=data.get("currency", "USD"),
        project=data.get("project", "Личное"),
        category=data.get("category", "Прочее"),
        event_type=data.get("event_type", "payment"),
        note=data.get("note", ""),
    )


def user_store_to_dict(store: UserStore) -> dict:
    return {
        "user_id": store.user_id,
        "chat_id": store.chat_id,
        "subscriptions": {key: subscription_to_dict(value) for key, value in store.subscriptions.items()},
        "archived_subscriptions": {key: subscription_to_dict(value) for key, value in store.archived_subscriptions.items()},
        "history": [expense_event_to_dict(item) for item in store.history],
        "sent_alerts": {key: date_to_iso(value) for key, value in store.sent_alerts.items()},
    }


def user_store_from_dict(data: dict) -> UserStore:
    store = UserStore(user_id=int(data["user_id"]), chat_id=data.get("chat_id"))
    store.subscriptions = {key: subscription_from_dict(value) for key, value in data.get("subscriptions", {}).items()}
    store.archived_subscriptions = {key: subscription_from_dict(value) for key, value in data.get("archived_subscriptions", {}).items()}
    store.history = [expense_event_from_dict(item) for item in data.get("history", [])]
    store.sent_alerts = {key: parse_iso_date(value) or today_local() for key, value in data.get("sent_alerts", {}).items()}
    return store


def user_activity_to_dict(activity: UserActivity) -> dict:
    return {
        "user_id": activity.user_id,
        "username": activity.username,
        "full_name": activity.full_name,
        "chat_id": activity.chat_id,
        "first_seen_at": datetime_to_iso(activity.first_seen_at),
        "last_seen_at": datetime_to_iso(activity.last_seen_at),
        "message_count": activity.message_count,
        "command_count": activity.command_count,
        "callback_count": activity.callback_count,
        "start_count": activity.start_count,
        "last_action": activity.last_action,
    }


def user_activity_from_dict(data: dict) -> UserActivity:
    return UserActivity(
        user_id=int(data["user_id"]),
        username=data.get("username", ""),
        full_name=data.get("full_name", ""),
        chat_id=data.get("chat_id"),
        first_seen_at=parse_iso_datetime(data.get("first_seen_at")),
        last_seen_at=parse_iso_datetime(data.get("last_seen_at")),
        message_count=int(data.get("message_count", 0)),
        command_count=int(data.get("command_count", 0)),
        callback_count=int(data.get("callback_count", 0)),
        start_count=int(data.get("start_count", 0)),
        last_action=data.get("last_action", ""),
    )


def save_state() -> None:
    payload = {
        "users": {str(user_id): user_store_to_dict(store) for user_id, store in RUNTIME_USERS.items()},
        "activity": {str(user_id): user_activity_to_dict(activity) for user_id, activity in USER_ACTIVITY_LOG.items()},
    }
    target = Path(DATA_FILE)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(target.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(target)


def load_state() -> None:
    target = Path(DATA_FILE)
    if not target.exists():
        return
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("Could not load state from %s: %s", DATA_FILE, exc)
        return

    RUNTIME_USERS.clear()
    USER_ACTIVITY_LOG.clear()
    for user_id, store_data in payload.get("users", {}).items():
        try:
            RUNTIME_USERS[int(user_id)] = user_store_from_dict(store_data)
        except Exception as exc:
            LOGGER.warning("Skip broken user store %s: %s", user_id, exc)
    for user_id, activity_data in payload.get("activity", {}).items():
        try:
            USER_ACTIVITY_LOG[int(user_id)] = user_activity_from_dict(activity_data)
        except Exception as exc:
            LOGGER.warning("Skip broken user activity %s: %s", user_id, exc)


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
    save_state()


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


def advance_next_charge(subscription: Subscription, periods: int = 1) -> None:
    base = subscription.next_charge_date or today_local()
    periods = max(1, int(periods or 1))
    if subscription.kind == "monthly":
        subscription.next_charge_date = add_months(base, periods)
    elif subscription.kind == "yearly":
        subscription.next_charge_date = add_years(base, periods)


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


def normalize_weekly_weekday(value: int) -> int:
    """Convert a human-friendly Monday=0..Sunday=6 value to PTB Sunday=0..Saturday=6."""
    normalized = value % 7
    return (normalized + 1) % 7


def clamp_month_day(value: int) -> int:
    if value < 1:
        return 1
    if value > 31:
        return 31
    return value


def get_kind_from_label(label: str) -> Optional[str]:
    mapping = {
        "Ежемесячная": "monthly",
        "📅 Ежемесячная": "monthly",
        "Годовая": "yearly",
        "🗓 Годовая": "yearly",
        "Балансовый сервис": "balance",
        "💳 С балансом": "balance",
    }
    return mapping.get(label.strip())


def get_balance_mode_from_label(label: str) -> Optional[str]:
    mapping = {
        "Ручной контроль": "manual",
        "Фиксированное списание": "fixed",
        "Средний расход в день": "daily_avg",
        "🖐 Ручной контроль": "manual",
        "📆 Списание по расписанию": "fixed",
        "📉 Расход в день": "daily_avg",
    }
    return mapping.get(label.strip())



def normalize_tags(tags: Optional[List[str]]) -> List[str]:
    if not tags:
        return []
    result: List[str] = []
    seen = set()
    for item in tags:
        value = str(item).strip().lstrip('#')
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def parse_tags_input(text: str) -> List[str]:
    raw = text.strip()
    if raw == '-' or not raw:
        return []
    parts = re.split(r'[,;\n]+', raw)
    return normalize_tags(parts)


def tags_text(subscription: Subscription) -> str:
    return ', '.join('#' + escape(tag) for tag in normalize_tags(subscription.tags)) if subscription.tags else '—'


def current_week_bounds() -> tuple[datetime, datetime]:
    now = now_local()
    start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=7)
    return start, end


def last_seven_days_bounds() -> tuple[datetime, datetime]:
    end = now_local()
    start = end - timedelta(days=7)
    return start, end


def previous_month_bounds() -> tuple[datetime, datetime]:
    now = now_local()
    current_month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    previous_month_end = current_month_start
    previous_month_last_day = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_last_day.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return previous_month_start, previous_month_end


def history_for_period(store: UserStore, start: datetime, end: datetime) -> List[ExpenseEvent]:
    return [event for event in store.history if start <= event.timestamp < end]


def summarize_by_category(events: List[ExpenseEvent]) -> Dict[str, Dict[str, float]]:
    grouped: Dict[str, Dict[str, float]] = {}
    for event in events:
        grouped.setdefault(event.category, {})
        grouped[event.category][event.currency] = grouped[event.category].get(event.currency, 0.0) + event.amount
    return grouped


def summarize_subscription_inventory_by_category(subscriptions: List[Subscription]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for subscription in subscriptions:
        counts[subscription.category] = counts.get(subscription.category, 0) + 1
    return counts


def build_period_summary_lines(store: UserStore, title: str, start: datetime, end: datetime) -> List[str]:
    events = history_for_period(store, start, end)
    totals = summarize_amounts(events)
    by_project = summarize_by_project(events)
    by_category = summarize_by_category(events)
    active = active_subscriptions(store)
    lines = [
        f'<b>{title}</b>',
        f'Период: {start.strftime("%d.%m.%Y")} — {(end - timedelta(seconds=1)).strftime("%d.%m.%Y")}',
        f'Активных подписок: {len(active)}',
        f'Скоро списаний ({SOON_DAYS} дн.): {len(upcoming_subscriptions(store, SOON_DAYS))}',
        f'Низкий баланс: {len(low_balance_subscriptions(store))}',
        f'Потрачено: {format_currency_totals(totals)}',
        f'Операций: {len(events)}',
    ]
    category_inventory = summarize_subscription_inventory_by_category(active)
    if category_inventory:
        lines.append('\n<b>Категории подписок</b>')
        for category, count in sorted(category_inventory.items(), key=lambda item: (-item[1], item[0].lower())):
            lines.append(f'• {escape(category)} — {count}')
    if by_project:
        lines.append('\n<b>Траты по проектам</b>')
        for project, amounts in sorted(by_project.items()):
            lines.append(f'• {escape(project)} — {format_currency_totals(amounts)}')
    if by_category:
        lines.append('\n<b>Траты по категориям</b>')
        for category, amounts in sorted(by_category.items()):
            lines.append(f'• {escape(category)} — {format_currency_totals(amounts)}')
    return lines


def matches_search(subscription: Subscription, query: str) -> bool:
    haystack = ' | '.join([
        subscription.id,
        subscription.name,
        subscription.project,
        subscription.category,
        ' '.join(subscription.tags),
        subscription.notes,
        subscription.currency,
        KIND_LABELS.get(subscription.kind, subscription.kind),
    ]).casefold()
    return query.casefold() in haystack


def apply_filters(items: List[Subscription], criteria: List[str]) -> List[Subscription]:
    result = list(items)
    for criterion in criteria:
        token = criterion.strip()
        if not token:
            continue
        token_cf = token.casefold()
        if token_cf in {'active', 'активные'}:
            result = [item for item in result if item.active]
            continue
        if token_cf in {'paused', 'pause', 'напаузе', 'пауза'}:
            result = [item for item in result if not item.active]
            continue
        if token_cf in {'balance', 'monthly', 'yearly'}:
            result = [item for item in result if item.kind == token_cf]
            continue
        if ':' in token:
            key, value = token.split(':', 1)
        elif '=' in token:
            key, value = token.split('=', 1)
        else:
            result = [item for item in result if matches_search(item, token)]
            continue
        key_cf = key.strip().casefold()
        value_cf = value.strip().casefold()
        if key_cf in {'currency', 'валюта'}:
            result = [item for item in result if item.currency.casefold() == value_cf]
        elif key_cf in {'project', 'проект'}:
            result = [item for item in result if value_cf in item.project.casefold()]
        elif key_cf in {'category', 'категория'}:
            result = [item for item in result if value_cf in item.category.casefold()]
        elif key_cf in {'tag', 'тег'}:
            result = [item for item in result if any(value_cf in tag.casefold() for tag in item.tags)]
        elif key_cf in {'name', 'название'}:
            result = [item for item in result if value_cf in item.name.casefold()]
    return result


def archive_count(store: UserStore) -> int:
    return len(store.archived_subscriptions)

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


def sum_forecast_items(items: List[dict]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for item in items:
        currency = str(item.get("currency", "USD"))
        amount = float(item.get("amount", 0.0))
        totals[currency] = totals.get(currency, 0.0) + amount
    return totals


def summarize_forecast_by_project(items: List[dict]) -> Dict[str, Dict[str, float]]:
    grouped: Dict[str, Dict[str, float]] = {}
    for item in items:
        project = str(item.get("project", "Личное"))
        currency = str(item.get("currency", "USD"))
        amount = float(item.get("amount", 0.0))
        grouped.setdefault(project, {})
        grouped[project][currency] = grouped[project].get(currency, 0.0) + amount
    return grouped


def recurring_next_date(subscription: Subscription, source: date) -> date:
    if subscription.kind == "monthly":
        return add_months(source, 1)
    return add_years(source, 1)


def forecast_regular_charge_items(subscription: Subscription, window_days: int = FORECAST_DAYS) -> List[dict]:
    if not subscription.active or subscription.kind not in {"monthly", "yearly"}:
        return []
    if subscription.next_charge_date is None:
        return []

    start_date = today_local()
    end_date = start_date + timedelta(days=window_days)
    due_date = subscription.next_charge_date
    effective_due = start_date if due_date < start_date else due_date
    if effective_due > end_date:
        return []
    note = "Ожидает оплаты" if due_date <= start_date else "Ближайшее списание"
    return [{
        "type": "charge",
        "subscription_id": subscription.id,
        "name": subscription.name,
        "project": subscription.project,
        "category": subscription.category,
        "currency": subscription.currency,
        "amount": subscription.amount,
        "due_date": effective_due,
        "note": note,
    }]


def forecast_balance_topup_items(subscription: Subscription, window_days: int = FORECAST_DAYS) -> List[dict]:
    if not subscription.active or subscription.kind != "balance":
        return []
    if subscription.current_balance is None or subscription.min_balance is None:
        return []

    topup_amount = subscription.amount or 0.0
    if topup_amount <= 0:
        return []

    start_date = today_local()
    end_date = start_date + timedelta(days=window_days)
    balance = effective_balance(subscription, start_date)
    if balance is None:
        return []

    threshold = subscription.min_balance
    items: List[dict] = []

    def add_item(due_date: date, note: str) -> None:
        items.append({
            "type": "topup",
            "subscription_id": subscription.id,
            "name": subscription.name,
            "project": subscription.project,
            "category": subscription.category,
            "currency": subscription.currency,
            "amount": topup_amount,
            "due_date": due_date,
            "note": note,
        })

    if balance <= threshold:
        add_item(start_date, "Вероятное пополнение: баланс уже ниже порога")
        balance += topup_amount

    mode = subscription.spending_mode or "manual"
    if mode == "manual":
        return items

    if mode == "daily_avg":
        spend = subscription.spend_amount or 0.0
        if spend <= 0:
            return items
        for day_offset in range(1, window_days + 1):
            due_date = start_date + timedelta(days=day_offset)
            balance -= spend
            if balance <= threshold:
                add_item(due_date, "Вероятное пополнение по среднему дневному расходу")
                balance += topup_amount
        return items

    if mode == "fixed":
        spend = subscription.spend_amount or 0.0
        period_days = subscription.spend_period_days or 0
        if spend <= 0 or period_days <= 0:
            return items
        due_date = next_fixed_charge_date(subscription, start_date)
        loops = 0
        while due_date is not None and due_date <= end_date and loops < 365:
            balance -= spend
            if balance <= threshold:
                add_item(due_date, "Вероятное пополнение после фиксированного списания")
                balance += topup_amount
            due_date = due_date + timedelta(days=period_days)
            loops += 1
    return items


def build_forecast_payload(store: UserStore, window_days: int = FORECAST_DAYS) -> dict:
    regular_items: List[dict] = []
    topup_items: List[dict] = []
    unmodelled_balance = 0

    for subscription in store.subscriptions.values():
        if not subscription.active:
            continue
        if subscription.kind in {"monthly", "yearly"}:
            regular_items.extend(forecast_regular_charge_items(subscription, window_days))
        elif subscription.kind == "balance":
            current_balance = effective_balance(subscription)
            if subscription.spending_mode in {None, "manual"} and (
                subscription.current_balance is None
                or subscription.min_balance is None
                or current_balance is None
                or current_balance > (subscription.min_balance or 0)
            ):
                unmodelled_balance += 1
            topup_items.extend(forecast_balance_topup_items(subscription, window_days))

    all_items = sorted(regular_items + topup_items, key=lambda item: (item["due_date"], item["name"].lower(), item["type"]))
    regular_totals = sum_forecast_items(regular_items)
    topup_totals = sum_forecast_items(topup_items)
    total_totals = sum_forecast_items(all_items)
    return {
        "window_days": window_days,
        "regular_items": regular_items,
        "topup_items": topup_items,
        "all_items": all_items,
        "regular_totals": regular_totals,
        "topup_totals": topup_totals,
        "total_totals": total_totals,
        "by_project": summarize_forecast_by_project(all_items),
        "unmodelled_balance": unmodelled_balance,
    }


def format_month_heading(value: date) -> str:
    month_names = [
        "январь", "февраль", "март", "апрель", "май", "июнь",
        "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
    ]
    return f"{month_names[value.month - 1].capitalize()} {value.year}"


def months_ahead(count: int = 12) -> List[date]:
    start = today_local().replace(day=1)
    return [add_months(start, idx) for idx in range(count)]


def build_year_month_keyboard() -> InlineKeyboardMarkup:
    rows = []
    row = []
    for month_date in months_ahead(12):
        row.append(InlineKeyboardButton(month_date.strftime('%m.%Y'), callback_data=f"yearmonth:{month_date.strftime('%Y-%m')}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("⬅️ Меню", callback_data="yearmenu:back")])
    return InlineKeyboardMarkup(rows)


def build_month_events_lines(store: UserStore, year: int, month: int) -> List[str]:
    start = date(year, month, 1)
    end = add_months(start, 1) - timedelta(days=1)
    payload = build_forecast_payload(store, YEAR_FORECAST_DAYS)
    regular = [item for item in payload["regular_items"] if start <= item["due_date"] <= end]
    topups = [item for item in payload["topup_items"] if start <= item["due_date"] <= end]
    all_items = sorted(regular + topups, key=lambda item: (item["due_date"], item["name"].lower(), item["type"]))

    lines = [
        f"<b>{format_month_heading(start)}</b>",
        f"Период: {start.strftime('%d.%m.%Y')} — {end.strftime('%d.%m.%Y')}",
    ]
    if not all_items:
        lines.append("На этот месяц событий пока нет.")
        return lines

    if regular:
        lines.append("\n<b>Обязательные списания</b>")
        for item in regular:
            lines.append(
                f"• {item['due_date'].strftime('%d.%m')} — {escape(item['name'])} — {format_money(float(item['amount']), str(item['currency']))}"
            )
    if topups:
        lines.append("\n<b>Прогнозируемые пополнения</b>")
        for item in topups:
            lines.append(
                f"• {item['due_date'].strftime('%d.%m')} — {escape(item['name'])} — {format_money(float(item['amount']), str(item['currency']))}"
            )
    lines.append("\n<b>Итого за месяц</b>")
    if regular:
        lines.append(f"• Списания: {format_currency_totals(sum_forecast_items(regular))}")
    if topups:
        lines.append(f"• Пополнения: {format_currency_totals(sum_forecast_items(topups))}")
    lines.append(f"• Всего: {format_currency_totals(sum_forecast_items(all_items))}")
    return lines


def build_year_events_lines(store: UserStore, title: str = "Прогнозируемые события на год", window_days: int = YEAR_FORECAST_DAYS) -> List[str]:
    payload = build_forecast_payload(store, window_days)
    start_date = today_local()
    end_date = start_date + timedelta(days=window_days)
    lines = [
        f"<b>{title}</b>",
        f"Период: {start_date.strftime('%d.%m.%Y')} — {end_date.strftime('%d.%m.%Y')}",
        f"Всего событий: {len(payload['all_items'])}",
        f"Обязательные списания: {format_currency_totals(payload['regular_totals'])}",
        f"Вероятные пополнения: {format_currency_totals(payload['topup_totals'])}",
        f"Общий прогноз: {format_currency_totals(payload['total_totals'])}",
    ]
    if payload['unmodelled_balance']:
        lines.append(f"Без прогноза по части балансовых сервисов: {payload['unmodelled_balance']}")

    if payload['by_project']:
        lines.append("\n<b>По проектам</b>")
        for project, amounts in sorted(payload['by_project'].items()):
            lines.append(f"• {escape(project)} — {format_currency_totals(amounts)}")

    if not payload['all_items']:
        lines.append("\nСобытий на выбранный период нет.")
        return lines

    lines.append("\n<b>Все прогнозируемые события</b>")
    current_month = None
    for item in payload['all_items']:
        due_date = item['due_date']
        month_key = (due_date.year, due_date.month)
        if month_key != current_month:
            current_month = month_key
            lines.append(f"\n<b>{format_month_heading(due_date)}</b>")
        icon = "💳" if item['type'] == 'charge' else "🪫"
        lines.append(
            f"• {icon} {due_date.strftime('%d.%m.%Y')} — {escape(item['name'])} — "
            f"{format_money(float(item['amount']), str(item['currency']))}"
        )
    return lines


async def reply_html_chunks(message, lines: List[str], reply_markup=None) -> None:
    chunks: List[str] = []
    current = ""
    for line in lines:
        candidate = line if not current else current + "\n" + line
        if current and len(candidate) > 3900:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    for index, chunk in enumerate(chunks):
        kwargs = {"parse_mode": ParseMode.HTML}
        if index == 0 and reply_markup is not None:
            kwargs["reply_markup"] = reply_markup
        await message.reply_text(chunk, **kwargs)


def build_forecast_lines(store: UserStore, title: str = "Прогноз расходов", window_days: int = FORECAST_DAYS) -> List[str]:
    payload = build_forecast_payload(store, window_days)
    lines = [
        f"<b>{title}</b>",
        f"Период: {window_days} дн.",
        f"Обязательные списания: {format_currency_totals(payload['regular_totals'])}",
        f"Вероятные пополнения: {format_currency_totals(payload['topup_totals'])}",
        f"Общий прогноз: {format_currency_totals(payload['total_totals'])}",
        f"Событий: {len(payload['all_items'])}",
    ]
    if payload['unmodelled_balance']:
        lines.append(f"Без прогноза по части балансовых сервисов: {payload['unmodelled_balance']}")

    by_project = payload['by_project']
    if by_project:
        lines.append("\n<b>По проектам</b>")
        for project, amounts in sorted(by_project.items()):
            lines.append(f"• {escape(project)} — {format_currency_totals(amounts)}")

    if payload['all_items']:
        lines.append("\n<b>Ближайшие события</b>")
        for item in payload['all_items'][:10]:
            icon = "💳" if item['type'] == 'charge' else "🪫"
            lines.append(
                f"• {icon} {item['due_date'].strftime('%d.%m.%Y')} — {escape(item['name'])} — {format_money(float(item['amount']), str(item['currency']))}"
            )

    return lines


def build_today_lines(store: UserStore) -> List[str]:
    today = today_local()
    overdue = [
        item for item in store.subscriptions.values()
        if item.active and item.kind != 'balance' and item.next_charge_date is not None and item.next_charge_date < today
    ]
    due_today = [
        item for item in store.subscriptions.values()
        if item.active and item.kind != 'balance' and item.next_charge_date == today
    ]
    low_now = low_balance_subscriptions(store)
    threshold_today = [
        item for item in balance_warning_subscriptions(store, 1)
        if item not in low_now
    ]

    lines = [
        "<b>Сегодня</b>",
        f"Просрочено: {len(overdue)}",
        f"Списаний сегодня: {len(due_today)}",
        f"Нужно пополнить сейчас: {len(low_now)}",
        f"Порог в течение 1 дня: {len(threshold_today)}",
    ]

    if overdue:
        lines.append("\n<b>Просроченные подписки</b>")
        for subscription in sorted(overdue, key=lambda item: item.next_charge_date or today)[:5]:
            delta = (today - (subscription.next_charge_date or today)).days
            lines.append(f"• {escape(subscription.name)} — просрочено на {delta} дн.")

    if due_today:
        lines.append("\n<b>Нужно оплатить сегодня</b>")
        for subscription in sorted(due_today, key=lambda item: item.name.lower())[:5]:
            lines.append(f"• {escape(subscription.name)} — {format_money(subscription.amount, subscription.currency)}")

    if low_now:
        lines.append("\n<b>Нужно пополнить</b>")
        for subscription in low_now[:5]:
            lines.append(f"• {escape(subscription.name)} — {format_optional_money(effective_balance(subscription), subscription.currency)}")

    if threshold_today:
        lines.append("\n<b>Скоро упрётся в порог</b>")
        for subscription in threshold_today[:5]:
            days_left = days_until_balance_threshold(subscription)
            lines.append(f"• {escape(subscription.name)} — примерно {days_left} дн.")

    if len(lines) == 5:
        lines.append("\nНа сегодня срочных действий нет.")
    return lines


def parse_boolish(value: str, default: bool = False) -> bool:
    cleaned = str(value).strip().casefold()
    if cleaned in {"1", "true", "yes", "y", "да"}:
        return True
    if cleaned in {"0", "false", "no", "n", "нет"}:
        return False
    return default


def export_user_payload(user_id: int) -> dict:
    store = RUNTIME_USERS.get(user_id, UserStore(user_id=user_id))
    activity = USER_ACTIVITY_LOG.get(user_id, UserActivity(user_id=user_id))
    return {
        "format": "subscription-bot-user-v1",
        "exported_at": datetime_to_iso(now_local()),
        "user_store": user_store_to_dict(store),
        "user_activity": user_activity_to_dict(activity),
    }


def subscriptions_csv_text(store: UserStore) -> str:
    output = StringIO()
    fieldnames = [
        "source", "id", "name", "kind", "amount", "currency", "project", "category", "tags", "notes",
        "active", "next_charge_date", "remind_before_days", "reminder_offsets", "repeat_daily_until_paid",
        "current_balance", "min_balance", "balance_updated_at", "spending_mode", "spend_amount",
        "spend_period_days", "snoozed_until", "site_url", "payment_url",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for source_name, pool in (("active", store.subscriptions), ("archived", store.archived_subscriptions)):
        for subscription in sorted(pool.values(), key=lambda item: item.name.lower()):
            writer.writerow({
                "source": source_name,
                "id": subscription.id,
                "name": subscription.name,
                "kind": subscription.kind,
                "amount": subscription.amount,
                "currency": subscription.currency,
                "project": subscription.project,
                "category": subscription.category,
                "tags": ", ".join(subscription.tags),
                "notes": subscription.notes,
                "active": int(subscription.active),
                "next_charge_date": date_to_iso(subscription.next_charge_date) or "",
                "remind_before_days": subscription.remind_before_days,
                "reminder_offsets": format_reminder_offsets(current_reminder_offsets(subscription)),
                "repeat_daily_until_paid": int(subscription.repeat_daily_until_paid),
                "current_balance": "" if subscription.current_balance is None else subscription.current_balance,
                "min_balance": "" if subscription.min_balance is None else subscription.min_balance,
                "balance_updated_at": date_to_iso(subscription.balance_updated_at) or "",
                "spending_mode": subscription.spending_mode or "",
                "spend_amount": "" if subscription.spend_amount is None else subscription.spend_amount,
                "spend_period_days": "" if subscription.spend_period_days is None else subscription.spend_period_days,
                "snoozed_until": date_to_iso(subscription.snoozed_until) or "",
                "site_url": subscription.site_url,
                "payment_url": subscription.payment_url,
            })
    return output.getvalue()


def history_csv_text(store: UserStore) -> str:
    output = StringIO()
    fieldnames = ["timestamp", "subscription_id", "subscription_name", "amount", "currency", "project", "category", "event_type", "note"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for event in sorted(store.history, key=lambda item: item.timestamp):
        writer.writerow(expense_event_to_dict(event))
    return output.getvalue()


def subscription_from_csv_row(row: dict) -> Subscription:
    raw_id = (row.get("id") or "").strip() or uuid4().hex[:8]
    tags = [item.strip() for item in str(row.get("tags", "")).split(",") if item.strip()]
    reminder_offsets = parse_reminder_offsets(str(row.get("reminder_offsets", "")))
    kind = (row.get("kind") or "monthly").strip()
    if kind not in KIND_LABELS:
        labels_map = {value.casefold(): key for key, value in KIND_LABELS.items()}
        kind = labels_map.get(kind.casefold(), "monthly")
    subscription = Subscription(
        id=raw_id,
        name=(row.get("name") or "Без названия").strip(),
        kind=kind,
        amount=float(row.get("amount") or 0.0),
        currency=(parse_currency_input(str(row.get("currency", "USD"))) or "USD"),
        project=(row.get("project") or "Личное").strip() or "Личное",
        category=(row.get("category") or "Прочее").strip() or "Прочее",
        tags=tags,
        notes=(row.get("notes") or "").strip(),
        created_at=now_local(),
        active=parse_boolish(str(row.get("active", "1")), True),
        next_charge_date=parse_iso_date((row.get("next_charge_date") or "").strip()),
        remind_before_days=int(row.get("remind_before_days") or 3),
        current_balance=(None if str(row.get("current_balance", "")).strip() == "" else float(row.get("current_balance"))),
        min_balance=(None if str(row.get("min_balance", "")).strip() == "" else float(row.get("min_balance"))),
        balance_updated_at=parse_iso_date((row.get("balance_updated_at") or "").strip()),
        spending_mode=((row.get("spending_mode") or "").strip() or None),
        spend_amount=(None if str(row.get("spend_amount", "")).strip() == "" else float(row.get("spend_amount"))),
        spend_period_days=(None if str(row.get("spend_period_days", "")).strip() == "" else int(float(row.get("spend_period_days")))),
        reminder_offsets=normalize_reminder_offsets(reminder_offsets or [int(row.get("remind_before_days") or 3), 0]),
        repeat_daily_until_paid=parse_boolish(str(row.get("repeat_daily_until_paid", "1")), True),
        snoozed_until=parse_iso_date((row.get("snoozed_until") or "").strip()),
        site_url=(row.get("site_url") or "").strip(),
        payment_url=(row.get("payment_url") or "").strip(),
    )
    return subscription


def apply_import_payload(target_user_id: int, payload: dict, chat_id: Optional[int]) -> tuple[int, int, int]:
    store_data = payload.get("user_store")
    activity_data = payload.get("user_activity")

    if store_data is None and "users" in payload:
        users_payload = payload.get("users") or {}
        key = str(target_user_id)
        if key in users_payload:
            store_data = users_payload[key]
        elif len(users_payload) == 1:
            store_data = next(iter(users_payload.values()))
        else:
            raise ValueError("Не удалось выбрать пользователя из JSON-дампа")
        activities_payload = payload.get("activity") or {}
        if key in activities_payload:
            activity_data = activities_payload[key]
        elif len(activities_payload) == 1:
            activity_data = next(iter(activities_payload.values()))

    if store_data is None:
        raise ValueError("В JSON нет блока user_store")

    imported_store = user_store_from_dict(store_data)
    imported_store.user_id = target_user_id
    imported_store.chat_id = chat_id
    RUNTIME_USERS[target_user_id] = imported_store

    if activity_data is not None:
        imported_activity = user_activity_from_dict(activity_data)
        imported_activity.user_id = target_user_id
        imported_activity.chat_id = chat_id
        imported_activity.last_seen_at = now_local()
        USER_ACTIVITY_LOG[target_user_id] = imported_activity

    save_state()
    return len(imported_store.subscriptions), len(imported_store.archived_subscriptions), len(imported_store.history)


def apply_import_csv(target_user_id: int, csv_text: str, chat_id: Optional[int]) -> tuple[int, int]:
    reader = csv.DictReader(StringIO(csv_text))
    store = get_store(target_user_id, chat_id)
    imported_active = 0
    imported_archived = 0
    for row in reader:
        if not row:
            continue
        subscription = subscription_from_csv_row(row)
        source = str(row.get("source", "active")).strip().casefold()
        if source == "archived":
            store.archived_subscriptions[subscription.id] = subscription
            imported_archived += 1
        else:
            store.subscriptions[subscription.id] = subscription
            imported_active += 1
    save_state()
    return imported_active, imported_archived


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
    if is_snoozed(subscription):
        return f"🔕 Отложено до {subscription.snoozed_until.strftime('%d.%m.%Y')}"
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
    if delta <= max_reminder_window(subscription):
        return f"🟡 Скоро списание ({delta} дн.)"
    return "🟢 Активна"


def render_subscription(subscription: Subscription) -> str:
    kind_label = KIND_LABELS[subscription.kind]
    lines = [
        f"<b>{escape(subscription.name)}</b>",
        f"<code>{subscription.id}</code> · {subscription_status(subscription)}",
        f"🧾 Формат: {escape(kind_label)}",
        f"📁 Проект: {escape(subscription.project)}",
        f"🏷 Категория: {escape(subscription.category)}",
    ]
    tags = tags_text(subscription)
    if tags != "—":
        lines.append(f"🔖 Теги: {tags}")

    if subscription.kind == "balance":
        lines.append(f"💳 Обычное пополнение: {format_money(subscription.amount, subscription.currency)}")
        lines.append(
            "💰 Баланс: "
            f"{format_optional_money(effective_balance(subscription), subscription.currency)}"
        )
        lines.append(
            "⚠️ Порог: "
            f"{format_optional_money(subscription.min_balance, subscription.currency)}"
        )
        for line in balance_projection_lines(subscription):
            lines.append(f"📉 {escape(line)}")
    else:
        lines.append(f"💳 Стоимость: {format_money(subscription.amount, subscription.currency)}")
        if subscription.next_charge_date is not None:
            lines.append(f"📅 Следующее списание: {subscription.next_charge_date.strftime('%d.%m.%Y')}")
        lines.append(f"⏰ Напоминания: {format_reminder_offsets(current_reminder_offsets(subscription))} дн.")
        lines.append(
            "🔁 Просрочка: напоминать каждый день"
            if subscription.repeat_daily_until_paid
            else "🔁 Просрочка: без ежедневного повтора"
        )
    if subscription.snoozed_until is not None and subscription.snoozed_until >= today_local():
        lines.append(f"😴 Пауза напоминаний до: {subscription.snoozed_until.strftime('%d.%m.%Y')}")
    if subscription.notes:
        lines.append(f"📝 Заметка: {escape(subscription.notes)}")
    if subscription.payment_url:
        lines.append(f"🌐 Оплата: {escape(subscription.payment_url)}")
    if subscription.site_url:
        lines.append(f"🔗 Сайт: {escape(subscription.site_url)}")
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
            category=subscription.category,
            event_type=event_type,
            note=note,
        )
    )


def apply_default_payment(store: UserStore, subscription: Subscription) -> None:
    amount = float(subscription.amount)
    if subscription.kind == "balance":
        current = effective_balance(subscription) or 0.0
        subscription.current_balance = current + amount
        subscription.balance_updated_at = today_local()
        record_history(store, subscription, amount, "topup")
    else:
        record_history(store, subscription, amount, "payment")
        advance_next_charge(subscription)
    subscription.snoozed_until = None
    save_state()


def build_inline_actions(subscription: Subscription) -> InlineKeyboardMarkup:
    pause_text = "Возобновить" if not subscription.active else "Пауза"
    pause_action = "resume" if not subscription.active else "pause"
    if subscription.kind == "balance":

        rows = [
            [
                InlineKeyboardButton("💸 Пополнил", callback_data=f"pay:{subscription.id}"),
                InlineKeyboardButton("🪫 Изм. баланс", callback_data=f"setbalance:{subscription.id}"),
            ],
            [
                InlineKeyboardButton("🧾 История", callback_data=f"historysub:{subscription.id}"),
                InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit:{subscription.id}"),
            ],
            [
                InlineKeyboardButton("⏰ Отложить", callback_data=f"snooze:{subscription.id}"),
                InlineKeyboardButton(f"⏯ {pause_text}", callback_data=f"{pause_action}:{subscription.id}"),
            ],
        ]
        rows.extend(build_link_buttons(subscription))
        rows.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"delete:{subscription.id}")])
        return InlineKeyboardMarkup(rows)

    rows = [
        [
            InlineKeyboardButton("💸 Оплатил", callback_data=f"pay:{subscription.id}"),
            InlineKeyboardButton("🧾 История", callback_data=f"historysub:{subscription.id}"),
        ],
        [
            InlineKeyboardButton("✏️ Редактировать", callback_data=f"edit:{subscription.id}"),
            InlineKeyboardButton("⏰ Отложить", callback_data=f"snooze:{subscription.id}"),
        ],
        [
            InlineKeyboardButton(f"⏯ {pause_text}", callback_data=f"{pause_action}:{subscription.id}"),
            InlineKeyboardButton("🗑 Удалить", callback_data=f"delete:{subscription.id}"),
        ],
    ]
    rows.extend(build_link_buttons(subscription))
    return InlineKeyboardMarkup(rows)


def build_archive_actions(subscription: Subscription) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton('♻️ Восстановить', callback_data=f'restore:{subscription.id}'),
            InlineKeyboardButton('🧾 История', callback_data=f'historysub:{subscription.id}'),
        ],
        [InlineKeyboardButton('❌ Удалить навсегда', callback_data=f'purge:{subscription.id}')],
    ]
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
            await ui_send(update, context, text, reply_markup=MENU)
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
        "• хранить подписки, балансовые сервисы, категории и теги\n"
        "• напоминать о ближайших списаниях и низком балансе\n"
        "• показывать, что нужно сделать сегодня\n"
        f"• строить прогноз расходов на {FORECAST_DAYS} дней\n"
        "• фиксировать оплаты, пополнения и хранить историю\n"
        "• делать экспорт и импорт бэкапа\n"
        "• показывать сводку, отчёты и архив подписок\n\n"
        f"Данные сохраняются локально в JSON: {escape(DATA_FILE)}"
    )
    if not store.subscriptions:
        message += "\n\nДля быстрого теста можешь запустить /demo."
    await ui_send(update, context, message, reply_markup=MENU)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    text = (
        "Команды:\n"
        "/start — главное меню\n"
        "/add — добавить подписку\n"
        "/list — показать подписки\n"
        "/today — что требует внимания сегодня\n"
        "/soon — ближайшие списания\n"
        "/topup — сервисы с низким балансом и прогнозом\n"
        "/pay — отметить оплату или пополнение\n"
        "/setbalance — обновить текущий баланс\n"
        "/edit — изменить подписку\n"
        "/dashboard — расширенная сводка\n"
        "/forecast — прогноз расходов на ближайший период\n"
        "/year — все прогнозируемые события на год\n"
        "/report — отчёт за текущий месяц\n"
        "/weekly — сводка за 7 дней\n"
        "/monthly — сводка за месяц\n"
        "/archive — архив подписок\n"
        "/find текст — поиск по названию, проекту, категории, тегам\n"
        "/filter критерии — фильтр, например currency:RUB category:AI tag:личное\n"
        "/history — история последних трат\n"
        "/export [json|csv] — выгрузить резервную копию\n"
        "/import — загрузить JSON или CSV экспорт обратно в бота\n"
        "/users — кто пользуется ботом\n"
        "/demo — добавить демо-набор подписок\n"
        "/cancel — отменить текущий диалог"
    )
    await ui_send(update, context, text, reply_markup=MENU)


async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    if not await ensure_owner_access(update):
        return

    if not USER_ACTIVITY_LOG:
        await ui_send(update, context, "Пока нет данных по пользователям.", reply_markup=MENU)
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
            category="Инфраструктура",
            tags=["сервер", "prod"],
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
            category="AI/API",
            tags=["openai", "api"],
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
            category="Связь",
            tags=["симка", "мобильный"],
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
            category="Домены",
            tags=["домен"],
            notes="Демо годовой подписки",
            created_at=now_local(),
            next_charge_date=today + timedelta(days=25),
            remind_before_days=14,
        ),
    ]
    for subscription in samples:
        store.subscriptions[subscription.id] = subscription
    save_state()

    await update.message.reply_text(
        "Добавил демо-набор с месячной подпиской, годовым сервисом и двумя балансовыми моделями.",
        reply_markup=MENU,
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.pop("pending_subscription", None)
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    context.user_data.pop("pending_payment_default", None)
    context.user_data.pop("pending_balance_id", None)
    context.user_data.pop("pending_edit_id", None)
    context.user_data.pop("pending_edit_field", None)
    await update.message.reply_text("Ок, остановил текущее действие.", reply_markup=MENU)
    return ConversationHandler.END


CONFIRM_SAVE = "✅ Сохранить"
CONFIRM_EDIT = "✏️ Изменить"
CONFIRM_CANCEL = "❌ Отмена"


def pending_total_steps(pending: dict) -> int:
    kind = pending.get("kind")
    if kind != "balance":
        return 12 if kind else 12
    mode = pending.get("spending_mode")
    if mode == "fixed":
        return 15
    if mode in {"manual", "daily"}:
        return 14
    return 14


def pending_subscription_preview(pending: dict) -> str:
    lines = ["<b>Проверь подписку</b>"]
    if pending.get("name"):
        lines.append(f"• Сервис: {escape(str(pending['name']))}")
    if pending.get("kind"):
        lines.append(f"• Тип: {escape(KIND_LABELS[pending['kind']])}")
    if pending.get("amount") is not None and pending.get("currency"):
        amount_label = "Пополнение" if pending.get("kind") == "balance" else "Стоимость"
        lines.append(f"• {amount_label}: {format_money(float(pending['amount']), str(pending['currency']))}")
    if pending.get("project"):
        lines.append(f"• Проект: {escape(str(pending['project']))}")
    if pending.get("category"):
        lines.append(f"• Категория: {escape(str(pending['category']))}")
    tags = pending.get("tags") or []
    lines.append(f"• Теги: {escape(', '.join(tags) if tags else '—')}")

    if pending.get("kind") == "balance":
        if pending.get("current_balance") is not None and pending.get("currency"):
            lines.append(f"• Баланс: {format_money(float(pending['current_balance']), str(pending['currency']))}")
        if pending.get("min_balance") is not None and pending.get("currency"):
            lines.append(f"• Порог: {format_money(float(pending['min_balance']), str(pending['currency']))}")
        mode = pending.get("spending_mode")
        if mode:
            lines.append(f"• Расход: {escape(BALANCE_MODE_LABELS.get(mode, mode))}")
        if pending.get("spend_amount") is not None and pending.get("currency"):
            label = "Списание" if pending.get("spending_mode") == "fixed" else "Расход"
            suffix = " / день" if pending.get("spending_mode") == "daily_avg" else ""
            lines.append(f"• {label}: {format_money(float(pending['spend_amount']), str(pending['currency']))}{suffix}")
        if pending.get("spending_mode") == "fixed" and pending.get("spend_period_days"):
            lines.append(f"• Период: каждые {int(pending['spend_period_days'])} дн.")
    else:
        if pending.get("next_charge_date"):
            lines.append(f"• Следующее списание: {pending['next_charge_date'].strftime('%d.%m.%Y')}")
        if pending.get("reminder_offsets"):
            lines.append(f"• Напоминания: {format_reminder_offsets(list(pending['reminder_offsets']))} дн.")
        repeat = pending.get("repeat_daily_until_paid")
        if repeat is not None:
            lines.append("• Просрочка: напоминать каждый день" if repeat else "• Просрочка: без ежедневного повтора")

    if pending.get("notes"):
        lines.append(f"• Заметка: {escape(str(pending['notes']))}")
    if pending.get("site_url"):
        lines.append(f"• Сайт: {escape(str(pending['site_url']))}")
    if pending.get("payment_url"):
        lines.append(f"• Оплата: {escape(str(pending['payment_url']))}")
    return "\n".join(lines)


def confirm_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([[CONFIRM_SAVE, CONFIRM_EDIT], [CONFIRM_CANCEL]], resize_keyboard=True, one_time_keyboard=True)


def confirm_edit_keyboard(pending: dict) -> ReplyKeyboardMarkup:
    rows = [
        ["Название", "Сумма"],
        ["Валюта", "Проект"],
        ["Категория", "Теги"],
    ]
    if pending.get("kind") == "balance":
        rows.extend([["Баланс", "Порог"], ["Режим расхода", "Расход"], ["Период", "Заметка"]])
    else:
        rows.extend([["Дата списания", "Напоминания"], ["Повтор просрочки", "Заметка"]])
    rows.extend([["Сайт", "Оплата"], ["⬅️ Назад", "/cancel"]])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, one_time_keyboard=True)


def map_confirm_field(choice: str, pending: dict) -> tuple[str | None, int | None, str | None, str | None]:
    choice = (choice or "").strip().lower()
    mapping = {
        "название": ("name", ADD_NAME, "Название сервиса:", "Коротко и понятно"),
        "сумма": ("amount", ADD_AMOUNT, "Сумма:", "Только число"),
        "валюта": ("currency", ADD_CURRENCY, "Валюта:", "KZT, RUB, EUR, USD или TRY"),
        "проект": ("project", ADD_PROJECT, "Проект или группа:", "Личное, Работа, Bot A"),
        "категория": ("category", ADD_CATEGORY, "Категория:", "Можно выбрать кнопку или ввести свою"),
        "теги": ("tags", ADD_TAGS, "Теги:", "Через запятую или -"),
        "заметка": ("notes", ADD_NOTES, "Заметка:", "- чтобы пропустить"),
        "сайт": ("site_url", ADD_SITE_URL, "Ссылка на сайт:", "https://... или -"),
        "оплата": ("payment_url", ADD_PAYMENT_URL, "Ссылка на оплату:", "https://... или -"),
        "дата списания": ("next_charge_date", ADD_NEXT_DATE, "Дата следующего списания:", "ДД.ММ.ГГГГ или ГГГГ-ММ-ДД"),
        "напоминания": ("reminder_offsets", ADD_REMIND_DAYS, "Дни напоминаний:", "7,3,1,0"),
        "повтор просрочки": ("repeat_daily_until_paid", ADD_REPEAT_UNTIL_PAID, "Повторять просрочку?", "Да или Нет"),
        "баланс": ("current_balance", ADD_CURRENT_BALANCE, "Текущий баланс:", "Только число"),
        "порог": ("min_balance", ADD_MIN_BALANCE, "Минимальный порог:", "Только число"),
        "режим расхода": ("spending_mode", ADD_BALANCE_MODE, "Как считать расход:", None),
        "расход": ("spend_amount", ADD_SPEND_AMOUNT, "Сумма расхода:", "Только число"),
        "период": ("spend_period_days", ADD_SPEND_PERIOD, "Период списания в днях:", "Например: 30"),
    }
    item = mapping.get(choice)
    if item is None:
        return None, None, None, None
    field, state, title, hint = item
    if field == "period" and pending.get("spending_mode") != "fixed":
        return None, None, None, None
    return item


def in_confirm_edit_mode(context: ContextTypes.DEFAULT_TYPE) -> bool:
    return bool(context.user_data.get("confirm_edit_mode"))


async def maybe_return_to_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int | None:
    if context.user_data.pop("confirm_edit_mode", False):
        return await ask_add_confirmation(update, context)
    return None


async def ask_add_confirmation(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    pending = context.user_data.get("pending_subscription", {})
    await ui_send(update, context, pending_subscription_preview(pending), parse_mode=ParseMode.HTML, reply_markup=confirm_keyboard())
    return ADD_CONFIRM


async def add_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    context.user_data["pending_subscription"] = {}
    await ui_send(
        update,
        context,
        "Шаг 1\nНазвание сервиса:",
        reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True),
        force_new=True,
    )
    return ADD_NAME


async def add_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "name")
        return ADD_NAME
    name = update.message.text.strip()
    if len(name) < 2:
        await ui_send(update, context, "Название слишком короткое. Попробуй ещё раз.")
        return ADD_NAME
    context.user_data["pending_subscription"]["name"] = name
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(2, 10, "Тип сервиса"), reply_markup=keyboard_with_help([["📅 Ежемесячная", "🗓 Годовая"], ["💳 С балансом"]]))
    return ADD_KIND


async def add_kind(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "kind")
        return ADD_KIND
    kind = get_kind_from_label(update.message.text)
    if kind is None:
        await ui_send(update, context, "Выбери тип кнопкой ниже.", reply_markup=KIND_KEYBOARD)
        return ADD_KIND
    context.user_data["pending_subscription"]["kind"] = kind
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    prompt = "Сумма подписки" if kind != "balance" else "Обычная сумма пополнения"
    await ui_send(update, context, step_text(3, 10, prompt), reply_markup=keyboard_with_help([]))
    return ADD_AMOUNT


async def add_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "amount")
        return ADD_AMOUNT
    amount = parse_float(update.message.text)
    if amount is None:
        await ui_send(update, context, "Нужна положительная сумма.")
        return ADD_AMOUNT
    context.user_data["pending_subscription"]["amount"] = amount
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(4, 10, "Валюта"), reply_markup=keyboard_with_help([["₸ Тенге", "₽ Рубли", "€ Евро"], ["$ Доллары", "₺ Лиры"]]))
    return ADD_CURRENCY


async def add_currency(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "currency")
        return ADD_CURRENCY
    currency = parse_currency_input(update.message.text)
    if currency is None:
        await ui_send(update, context, step_text(4, 10, "Валюта"), reply_markup=keyboard_with_help([["₸ Тенге", "₽ Рубли", "€ Евро"], ["$ Доллары", "₺ Лиры"]]))
        return ADD_CURRENCY
    context.user_data["pending_subscription"]["currency"] = currency
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(5, 10, "Проект или группа"), reply_markup=keyboard_with_help([["Все проекты", "Личное"]]))
    return ADD_PROJECT


async def add_project(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "project")
        return ADD_PROJECT
    project = update.message.text.strip()
    if len(project) < 1:
        await ui_send(update, context, "Проект не должен быть пустым.")
        return ADD_PROJECT
    pending = context.user_data["pending_subscription"]
    pending["project"] = project
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(6, 10, "Категория", "Можно выбрать кнопку или ввести свою"), reply_markup=keyboard_with_help([["Инфраструктура", "Связь"], ["AI/API", "Домены"], ["Маркетинг", "Личное"], ["Прочее"]]))
    return ADD_CATEGORY


async def add_category(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "category")
        return ADD_CATEGORY
    category = update.message.text.strip()
    if len(category) < 1:
        await ui_send(update, context, "Категория не должна быть пустой.", reply_markup=CATEGORY_KEYBOARD)
        return ADD_CATEGORY
    pending = context.user_data["pending_subscription"]
    pending["category"] = category
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(7, 10, "Теги", "Через запятую или -"), reply_markup=keyboard_with_help([["-"]]))
    return ADD_TAGS


async def add_tags(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "tags")
        return ADD_TAGS
    pending = context.user_data["pending_subscription"]
    pending["tags"] = parse_tags_input(update.message.text)
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    if pending["kind"] == "balance":
        await ui_send(update, context, step_text(8, 10, "Текущий баланс"), reply_markup=keyboard_with_help([]))
        return ADD_CURRENT_BALANCE
    await ui_send(update, context, step_text(8, 10, "Дата следующего списания", "ДД.ММ.ГГГГ или ГГГГ-ММ-ДД"), reply_markup=keyboard_with_help([]))
    return ADD_NEXT_DATE


async def add_next_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "next_date")
        return ADD_NEXT_DATE
    value = parse_date_input(update.message.text)
    if value is None:
        await ui_send(update, context, "Не смог распознать дату. Попробуй ещё раз.")
        return ADD_NEXT_DATE
    context.user_data["pending_subscription"]["next_charge_date"] = value
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(9, 10, "Когда напомнить", "7,3,1,0"), reply_markup=keyboard_with_help([["7,3,1,0"], ["3,1,0"]]))
    return ADD_REMIND_DAYS


async def add_remind_days(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "reminders")
        return ADD_REMIND_DAYS
    offsets = parse_reminder_offsets(update.message.text)
    if offsets is None:
        await ui_send(update, context, "Введи дни через запятую. Например: 7,3,1,0")
        return ADD_REMIND_DAYS
    context.user_data["pending_subscription"]["reminder_offsets"] = offsets
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(10, 10, "Повторять после даты списания", "Да — напоминать каждый день"), reply_markup=keyboard_with_help([["Да", "Нет"]]))
    return ADD_REPEAT_UNTIL_PAID


async def add_repeat_until_paid(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "repeat")
        return ADD_REPEAT_UNTIL_PAID
    repeat = parse_yes_no(update.message.text)
    if repeat is None:
        await ui_send(update, context, "Ответь Да или Нет.", reply_markup=YES_NO_KEYBOARD)
        return ADD_REPEAT_UNTIL_PAID
    context.user_data["pending_subscription"]["repeat_daily_until_paid"] = repeat
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(pending_total_steps(context.user_data["pending_subscription"]) - 2, pending_total_steps(context.user_data["pending_subscription"]), "Заметка", "- чтобы пропустить"), reply_markup=keyboard_with_help([["-"]]))
    return ADD_NOTES


async def add_current_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "balance")
        return ADD_CURRENT_BALANCE
    balance = parse_float(update.message.text)
    if balance is None:
        await ui_send(update, context, "Введи корректное число.")
        return ADD_CURRENT_BALANCE
    pending = context.user_data["pending_subscription"]
    pending["current_balance"] = balance
    pending["balance_updated_at"] = today_local()
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(9, 12, "Минимальный порог", "Когда баланс дойдёт до порога, бот напомнит"), reply_markup=keyboard_with_help([["10"]]))
    return ADD_MIN_BALANCE


async def add_min_balance(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "min_balance")
        return ADD_MIN_BALANCE
    minimum = parse_float(update.message.text)
    if minimum is None:
        await ui_send(update, context, "Введи корректное число.")
        return ADD_MIN_BALANCE
    context.user_data["pending_subscription"]["min_balance"] = minimum
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(10, 12, "Как считать расход", "Выбери самый удобный вариант"), reply_markup=keyboard_with_help([["🖐 Ручной контроль"], ["📆 Списание по расписанию"], ["📉 Расход в день"]]))
    return ADD_BALANCE_MODE


async def add_balance_mode(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "balance_mode")
        return ADD_BALANCE_MODE
    mode = get_balance_mode_from_label(update.message.text)
    if mode is None:
        await ui_send(update, context, "Выбери один из вариантов.", reply_markup=keyboard_with_help([["🖐 Ручной контроль"], ["📆 Списание по расписанию"], ["📉 Расход в день"]]))
        return ADD_BALANCE_MODE

    pending = context.user_data["pending_subscription"]
    pending["spending_mode"] = mode
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state

    if mode == "manual":
        await ui_send(update, context, step_text(13, 13, "Заметка", "- чтобы пропустить"), reply_markup=keyboard_with_help([["-"]]))
        return ADD_NOTES

    if mode == "fixed":
        await ui_send(update, context, step_text(11, 13, "Сумма списания"), reply_markup=keyboard_with_help([]))
        return ADD_SPEND_AMOUNT

    await ui_send(update, context, step_text(11, 13, "Расход в день"), reply_markup=keyboard_with_help([]))
    return ADD_SPEND_AMOUNT


async def add_spend_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "spend_amount")
        return ADD_SPEND_AMOUNT
    value = parse_float(update.message.text)
    if value is None:
        await ui_send(update, context, "Введи корректную сумму.")
        return ADD_SPEND_AMOUNT

    pending = context.user_data["pending_subscription"]
    pending["spend_amount"] = value
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state

    if pending.get("spending_mode") == "fixed":
        await ui_send(update, context, "Введи период списания в днях.", reply_markup=ReplyKeyboardMarkup([["7", "30", "/cancel"]], resize_keyboard=True, one_time_keyboard=True))
        return ADD_SPEND_PERIOD

    pending["spend_period_days"] = 1
    await ui_send(update, context, step_text(pending_total_steps(context.user_data["pending_subscription"]) - 2, pending_total_steps(context.user_data["pending_subscription"]), "Заметка", "- чтобы пропустить"), reply_markup=keyboard_with_help([["-"]]))
    return ADD_NOTES


async def add_spend_period(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "spend_period")
        return ADD_SPEND_PERIOD
    value = parse_int(update.message.text)
    if value is None or value <= 0:
        await ui_send(update, context, "Введи положительное число дней.")
        return ADD_SPEND_PERIOD
    context.user_data["pending_subscription"]["spend_period_days"] = value
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(pending_total_steps(context.user_data["pending_subscription"]) - 2, pending_total_steps(context.user_data["pending_subscription"]), "Заметка", "- чтобы пропустить"), reply_markup=keyboard_with_help([["-"]]))
    return ADD_NOTES


async def add_notes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "notes")
        return ADD_NOTES
    pending = context.user_data["pending_subscription"]
    pending["notes"] = "" if update.message.text.strip() == "-" else update.message.text.strip()
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    await ui_send(update, context, step_text(pending_total_steps(pending) - 1, pending_total_steps(pending), "Ссылка на сайт", "- чтобы пропустить"), reply_markup=keyboard_with_help([["-"]]))
    return ADD_SITE_URL


async def add_site_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "site_url")
        return ADD_SITE_URL
    try:
        context.user_data["pending_subscription"]["site_url"] = clean_optional_url(update.message.text)
    except ValueError as exc:
        await ui_send(update, context, str(exc), reply_markup=YES_SKIP_KEYBOARD)
        return ADD_SITE_URL
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    pending = context.user_data["pending_subscription"]
    await ui_send(update, context, step_text(pending_total_steps(pending), pending_total_steps(pending), "Ссылка на оплату", "- чтобы пропустить"), reply_markup=keyboard_with_help([["-"]]))
    return ADD_PAYMENT_URL


async def add_payment_url(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if is_help_request(update.message.text):
        await send_add_help(update, context, "payment_url")
        return ADD_PAYMENT_URL
    try:
        payment_url = clean_optional_url(update.message.text)
    except ValueError as exc:
        await ui_send(update, context, str(exc), reply_markup=YES_SKIP_KEYBOARD)
        return ADD_PAYMENT_URL

    pending = context.user_data.get("pending_subscription", {})
    pending["payment_url"] = payment_url
    maybe_state = await maybe_return_to_confirmation(update, context)
    if maybe_state is not None:
        return maybe_state
    return await ask_add_confirmation(update, context)


async def add_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = (update.message.text or "").strip()
    if is_help_request(text):
        await send_add_help(update, context, "confirm")
        return ADD_CONFIRM

    if text == CONFIRM_SAVE:
        user = update.effective_user
        chat = update.effective_chat
        store = get_store(user.id, chat.id if chat else None)
        pending = context.user_data.pop("pending_subscription", {})
        subscription = Subscription(
            id=uuid4().hex[:8],
            name=pending["name"],
            kind=pending["kind"],
            amount=pending["amount"],
            currency=pending["currency"],
            project=pending["project"],
            category=pending.get("category", "Прочее"),
            tags=normalize_tags(pending.get("tags")),
            notes=pending.get("notes", ""),
            created_at=now_local(),
            next_charge_date=pending.get("next_charge_date"),
            remind_before_days=(pending.get("reminder_offsets") or [3])[0],
            current_balance=pending.get("current_balance"),
            min_balance=pending.get("min_balance"),
            balance_updated_at=pending.get("balance_updated_at"),
            spending_mode=pending.get("spending_mode"),
            spend_amount=pending.get("spend_amount"),
            spend_period_days=pending.get("spend_period_days"),
            reminder_offsets=normalize_reminder_offsets(pending.get("reminder_offsets")),
            repeat_daily_until_paid=pending.get("repeat_daily_until_paid", True),
            site_url=pending.get("site_url", ""),
            payment_url=pending.get("payment_url", ""),
        )
        store.subscriptions[subscription.id] = subscription
        save_state()
        await ui_send(update, context, "Готово, подписка сохранена ✅\n\n" + render_subscription(subscription), parse_mode=ParseMode.HTML, reply_markup=MENU, force_new=True)
        return ConversationHandler.END

    if text == CONFIRM_EDIT:
        pending = context.user_data.get("pending_subscription", {})
        await ui_send(update, context, "Что хочешь поправить?", reply_markup=confirm_edit_keyboard(pending))
        return ADD_CONFIRM_EDIT_FIELD

    if text == CONFIRM_CANCEL:
        context.user_data.pop("pending_subscription", None)
        await ui_send(update, context, "Ок, не сохраняю эту подписку.", reply_markup=MENU, force_new=True)
        return ConversationHandler.END

    await ui_send(update, context, "Выбери: сохранить, изменить или отменить.", reply_markup=confirm_keyboard())
    return ADD_CONFIRM


async def add_confirm_edit_field(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = (update.message.text or "").strip()
    if choice == "⬅️ Назад":
        return await ask_add_confirmation(update, context)
    if is_help_request(choice):
        await send_add_help(update, context, "confirm")
        return ADD_CONFIRM_EDIT_FIELD

    pending = context.user_data.get("pending_subscription", {})
    field, state, title, hint = map_confirm_field(choice, pending)
    if state is None:
        await ui_send(update, context, "Выбери поле кнопкой ниже.", reply_markup=confirm_edit_keyboard(pending))
        return ADD_CONFIRM_EDIT_FIELD

    keyboard = keyboard_with_help([])
    if state == ADD_KIND:
        keyboard = keyboard_with_help([["📅 Ежемесячная", "🗓 Годовая"], ["💳 С балансом"]])
    elif state == ADD_CURRENCY:
        keyboard = keyboard_with_help([["₸ Тенге", "₽ Рубли", "€ Евро"], ["$ Доллары", "₺ Лиры"]])
    elif state == ADD_PROJECT:
        keyboard = keyboard_with_help([["Все проекты", "Личное"]])
    elif state == ADD_CATEGORY:
        keyboard = keyboard_with_help([["Инфраструктура", "Связь"], ["AI/API", "Домены"], ["Маркетинг", "Личное"], ["Прочее"]])
    elif state == ADD_BALANCE_MODE:
        keyboard = keyboard_with_help([["🖐 Ручной контроль"], ["📆 Списание по расписанию"], ["📉 Расход в день"]])
    elif state == ADD_REPEAT_UNTIL_PAID:
        keyboard = keyboard_with_help([["Да", "Нет"]])
    elif state in {ADD_SITE_URL, ADD_PAYMENT_URL, ADD_TAGS, ADD_NOTES}:
        keyboard = keyboard_with_help([["-"]])
    elif state == ADD_REMIND_DAYS:
        keyboard = keyboard_with_help([["7,3,1,0"], ["3,1,0"]])
    elif state == ADD_SPEND_PERIOD:
        keyboard = keyboard_with_help([["7", "30"]])

    context.user_data["confirm_edit_mode"] = True
    await ui_send(update, context, step_text(1, 1, title.rstrip(':'), hint), reply_markup=keyboard)
    return state


async def list_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    if not store.subscriptions:
        await ui_send(update, context, "Пока пусто. Нажми «➕ Добавить», чтобы создать первую подписку.", reply_markup=MENU, force_new=True)
        return

    active = [sub for sub in store.subscriptions.values() if sub.active]
    paused = [sub for sub in store.subscriptions.values() if not sub.active]

    await update.message.reply_text(
        f"Вот твои подписки:\nВсего: {len(store.subscriptions)} · Активных: {len(active)} · На паузе: {len(paused)} · В архиве: {archive_count(store)}",
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
            f"На ближайшие {SOON_DAYS} дней списаний не найдено.", reply_markup=MENU
        )
        return
    await update.message.reply_text(
        f"Что скоро оплатить в ближайшие {SOON_DAYS} дней:", reply_markup=MENU
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


def subscription_history_text(store: UserStore, subscription: Subscription) -> str:
    events = [item for item in store.history if item.subscription_id == subscription.id]
    month_start, month_end = current_month_bounds()
    month_events = [item for item in events if month_start <= item.timestamp < month_end]
    month_totals = summarize_amounts(month_events)
    lines = [
        f"<b>История: {escape(subscription.name)}</b>",
        f"Операций всего: {len(events)}",
        f"За текущий месяц: {format_currency_totals(month_totals)}",
        "",
    ]
    if not events:
        lines.append("История пока пустая.")
        return "\n".join(lines)
    for event in sorted(events, key=lambda item: item.timestamp, reverse=True)[:10]:
        label = EVENT_TYPE_LABELS.get(event.event_type, event.event_type)
        note_part = f" — {escape(event.note)}" if event.note else ""
        lines.append(
            f"• {event.timestamp.strftime('%d.%m.%Y %H:%M')} — {format_money(event.amount, event.currency)} ({label}){note_part}"
        )
    return "\n".join(lines)


async def history_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    if not store.history:
        await ui_send(update, context, "История пока пустая.", reply_markup=MENU)
        return
    lines = ["Последние траты:"]
    for event in sorted(store.history, key=lambda item: item.timestamp, reverse=True)[:15]:
        label = EVENT_TYPE_LABELS.get(event.event_type, event.event_type)
        lines.append(
            f"• {event.timestamp.strftime('%d.%m %H:%M')} — {escape(event.subscription_name)} — "
            f"{format_money(event.amount, event.currency)} ({label}, {escape(event.category)})"
        )
    await ui_send(update, context, "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)

    total = len(store.subscriptions)
    active_total = len(active_subscriptions(store))
    paused_total = total - active_total
    archived_total = archive_count(store)
    today_lines = build_today_lines(store)
    forecast = build_forecast_payload(store, FORECAST_DAYS)
    month_events = history_for_month(store)
    month_totals = summarize_amounts(month_events)
    categories = summarize_subscription_inventory_by_category(active_subscriptions(store))
    top_soon = upcoming_subscriptions(store, 7)[:3]

    lines = [
        "<b>Сводка</b>",
        f"Подписок всего: {total}",
        f"Активных: {active_total}",
        f"На паузе: {paused_total}",
        f"В архиве: {archived_total}",
        f"Потрачено в этом месяце: {format_currency_totals(month_totals)}",
        f"Прогноз на {FORECAST_DAYS} дн.: {format_currency_totals(forecast['total_totals'])}",
        f"Из них обязательные списания: {format_currency_totals(forecast['regular_totals'])}",
        f"Вероятные пополнения: {format_currency_totals(forecast['topup_totals'])}",
    ]

    lines.append("\n<b>Что требует внимания сегодня</b>")
    lines.extend(today_lines[1:5])

    if categories:
        lines.append("\n<b>Категории</b>")
        for category, count in sorted(categories.items(), key=lambda item: (-item[1], item[0].lower()))[:5]:
            lines.append(f"• {escape(category)} — {count}")

    if top_soon:
        lines.append("\n<b>Ближайшие оплаты</b>")
        for subscription in top_soon:
            if subscription.next_charge_date is None:
                continue
            delta = (subscription.next_charge_date - today_local()).days
            lines.append(
                f"• {escape(subscription.name)} — {subscription.next_charge_date.strftime('%d.%m.%Y')} ({delta} дн.)"
            )

    if forecast['all_items']:
        lines.append("\n<b>Прогнозируемые события</b>")
        for item in forecast['all_items'][:5]:
            icon = '💳' if item['type'] == 'charge' else '🪫'
            lines.append(
                f"• {icon} {item['due_date'].strftime('%d.%m.%Y')} — {escape(item['name'])} — {format_money(float(item['amount']), str(item['currency']))}"
            )

    await ui_send(update, context, "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    events = history_for_month(store)
    if not events:
        await ui_send(update, context, "В этом месяце трат пока нет. После /pay здесь появится отчёт.", reply_markup=MENU)
        return

    totals = summarize_amounts(events)
    grouped = summarize_by_project(events)
    by_category = summarize_by_category(events)

    lines = [
        f"<b>Отчёт за {now_local().strftime('%m.%Y')}</b>",
        f"Всего потрачено: {format_currency_totals(totals)}",
        f"Операций: {len(events)}",
        "\n<b>По проектам</b>",
    ]
    for project, amounts in sorted(grouped.items()):
        lines.append(f"• {escape(project)} — {format_currency_totals(amounts)}")
    if by_category:
        lines.append("\n<b>По категориям</b>")
        for category, amounts in sorted(by_category.items()):
            lines.append(f"• {escape(category)} — {format_currency_totals(amounts)}")

    await ui_send(update, context, "\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def archive_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    if not store.archived_subscriptions:
        await ui_send(update, context, 'Архив пуст.', reply_markup=MENU)
        return
    await ui_send(update, context, f'В архиве: {len(store.archived_subscriptions)}', reply_markup=MENU)
    for subscription in sorted(store.archived_subscriptions.values(), key=lambda item: item.name.lower()):
        await update.message.reply_text(
            render_subscription(subscription),
            parse_mode=ParseMode.HTML,
            reply_markup=build_archive_actions(subscription),
        )


async def weekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    start, end = last_seven_days_bounds()
    lines = build_period_summary_lines(store, 'Сводка за 7 дней', start, end)
    await ui_send(update, context, '\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def monthly_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    start, end = current_month_bounds()
    lines = build_period_summary_lines(store, 'Сводка за месяц', start, end)
    await ui_send(update, context, '\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def forecast_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    lines = build_forecast_lines(store, f'Прогнозируемые события на {FORECAST_DAYS} дн.', FORECAST_DAYS)
    await ui_send(update, context, '\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def year_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    await ui_send(update, context, 'Выбери месяц. Я покажу прогнозируемые события на него.', reply_markup=build_year_month_keyboard())


async def year_menu_back_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    query = update.callback_query
    if query is not None:
        try:
            await query.answer()
        except Exception:
            pass
    await start(update, context)


async def year_month_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    query = update.callback_query
    await query.answer()
    raw = query.data.split(':', maxsplit=1)[1]
    year_str, month_str = raw.split('-', maxsplit=1)
    year = int(year_str)
    month = int(month_str)
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    lines = build_month_events_lines(store, year, month)
    await ui_send(update, context, '\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=build_year_month_keyboard())


async def today_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    lines = build_today_lines(store)
    await ui_send(update, context, '\n'.join(lines), parse_mode=ParseMode.HTML, reply_markup=MENU)


async def export_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    await start_new_action(update, context)
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    mode = (context.args[0].strip().lower() if context.args else 'json')
    stamp = now_local().strftime('%Y%m%d_%H%M')

    if mode == 'csv':
        subscriptions_bytes = subscriptions_csv_text(store).encode('utf-8')
        history_bytes = history_csv_text(store).encode('utf-8')
        await update.message.reply_document(
            document=InputFile(BytesIO(subscriptions_bytes), filename=f'subscriptions_{stamp}.csv'),
            caption='Экспорт подписок в CSV.',
        )
        await update.message.reply_document(
            document=InputFile(BytesIO(history_bytes), filename=f'history_{stamp}.csv'),
            caption='Экспорт истории в CSV.',
            reply_markup=MENU,
        )
        return

    payload = export_user_payload(user.id)
    raw = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
    await update.message.reply_document(
        document=InputFile(BytesIO(raw), filename=f'subscription_backup_{stamp}.json'),
        caption='JSON-бэкап готов. Для CSV используй /export csv.',
        reply_markup=MENU,
    )


async def import_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    context.user_data['awaiting_import'] = True
    await ui_send(
        update,
        context,
        'Пришли JSON-бэкап или CSV-файл с экспортом подписок. JSON восстановит подписки, архив и историю. CSV импортирует только подписки.',
        reply_markup=MENU,
    )


async def import_document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    await start_new_action(update, context)
    if not context.user_data.get('awaiting_import'):
        return
    message = update.effective_message
    document = message.document if message else None
    if document is None:
        return

    tg_file = await document.get_file()
    buffer = BytesIO()
    await tg_file.download_to_memory(out=buffer)
    raw = buffer.getvalue()
    filename = (document.file_name or '').lower()

    user = update.effective_user
    chat = update.effective_chat
    try:
        if filename.endswith('.json'):
            payload = json.loads(raw.decode('utf-8'))
            active_count, archived_count, history_count = apply_import_payload(user.id, payload, chat.id if chat else None)
            await ui_send(
                update,
                context,
                f'Импорт JSON завершён. Активных: {active_count}, в архиве: {archived_count}, операций истории: {history_count}.',
                reply_markup=MENU,
            )
        elif filename.endswith('.csv'):
            imported_active, imported_archived = apply_import_csv(user.id, raw.decode('utf-8'), chat.id if chat else None)
            await ui_send(
                update,
                context,
                f'Импорт CSV завершён. Активных: {imported_active}, архивных: {imported_archived}. История не менялась.',
                reply_markup=MENU,
            )
        else:
            await ui_send(update, context, 'Поддерживаются только файлы .json и .csv.', reply_markup=MENU)
            return
    except Exception as exc:
        LOGGER.exception('Import failed: %s', exc)
        await ui_send(update, context, f'Не удалось импортировать файл: {escape(str(exc))}', parse_mode=ParseMode.HTML, reply_markup=MENU)
        return
    finally:
        context.user_data['awaiting_import'] = False


async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    query = ' '.join(context.args).strip()
    if not query:
        await update.message.reply_text('Используй: /find текст', reply_markup=MENU)
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    active = [item for item in store.subscriptions.values() if matches_search(item, query)]
    archived = [item for item in store.archived_subscriptions.values() if matches_search(item, query)]
    if not active and not archived:
        await update.message.reply_text('Ничего не нашёл.', reply_markup=MENU)
        return
    await update.message.reply_text(
        f'Найдено: активных {len(active)}, архивных {len(archived)}',
        reply_markup=MENU,
    )
    for subscription in sorted(active, key=lambda item: item.name.lower()):
        await update.message.reply_text(render_subscription(subscription), parse_mode=ParseMode.HTML, reply_markup=build_inline_actions(subscription))
    for subscription in sorted(archived, key=lambda item: item.name.lower()):
        await update.message.reply_text('🗃 Архив\n' + render_subscription(subscription), parse_mode=ParseMode.HTML, reply_markup=build_archive_actions(subscription))


async def filter_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    criteria = context.args
    if not criteria:
        await update.message.reply_text(
            'Используй: /filter active | /filter currency:RUB | /filter category:AI/API | /filter tag:сервер | /filter project:Личное',
            reply_markup=MENU,
        )
        return
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    include_archived = any(arg.casefold() in {'archived', 'архив'} for arg in criteria)
    pool = list(store.archived_subscriptions.values()) if include_archived else list(store.subscriptions.values())
    matches = apply_filters(pool, [arg for arg in criteria if arg.casefold() not in {'archived', 'архив'}])
    if not matches:
        await update.message.reply_text('По фильтру ничего не нашёл.', reply_markup=MENU)
        return
    await update.message.reply_text(f'Найдено по фильтру: {len(matches)}', reply_markup=MENU)
    for subscription in sorted(matches, key=lambda item: (not item.active, item.name.lower())):
        markup = build_archive_actions(subscription) if include_archived else build_inline_actions(subscription)
        await update.message.reply_text(render_subscription(subscription), parse_mode=ParseMode.HTML, reply_markup=markup)


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
    await ui_send(update, context, "Выбери сервис для фиксации оплаты или пополнения:", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True), force_new=True)
    return PAY_SELECT


async def pay_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    action, subscription_id = query.data.split(":", maxsplit=1)
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription = store.subscriptions.get(subscription_id) or store.archived_subscriptions.get(subscription_id)
    if subscription is None:
        await ui_send(update, context, "Не нашёл сервис. Попробуй ещё раз через /pay.", reply_markup=MENU, force_new=True)
        return ConversationHandler.END
    if action == "paydefault":
        apply_default_payment(store, subscription)
        text = "Пополнение сохранено.\n\n" if subscription.kind == "balance" else "Оплата сохранена.\n\n"
        await ui_send(update, context, text + render_subscription(subscription), parse_mode=ParseMode.HTML, reply_markup=MENU, force_new=True)
        return ConversationHandler.END
    context.user_data["pending_payment_id"] = subscription_id
    context.user_data["pending_payment_default"] = float(subscription.amount)
    await ui_send(update, context, f"Укажи сумму оплаты или пополнения. Стандартно: {format_money(float(subscription.amount), subscription.currency)}", reply_markup=ReplyKeyboardMarkup([[str(subscription.amount)], ["/cancel"]], resize_keyboard=True, one_time_keyboard=True))
    return PAY_AMOUNT


async def pay_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    match = re.search(r"\[([0-9a-f]{8})\]$", update.message.text.strip())
    if not match:
        await ui_send(update, context, "Выбери сервис кнопкой из списка.")
        return PAY_SELECT
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = match.group(1)
    context.user_data["pending_payment_id"] = subscription_id
    subscription = store.subscriptions.get(subscription_id) or store.archived_subscriptions.get(subscription_id)
    default_amount = float(subscription.amount) if subscription else 0.0
    context.user_data["pending_payment_default"] = default_amount
    buttons = [[str(default_amount)], ["/cancel"]] if default_amount else [["/cancel"]]
    await ui_send(update, context, "Укажи сумму оплаты или пополнения.", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True))
    return PAY_AMOUNT


async def pay_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    amount = parse_float(update.message.text)
    if amount is None:
        await ui_send(update, context, "Введи корректную сумму.")
        return PAY_AMOUNT

    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_payment_id")
    subscription = store.subscriptions.get(subscription_id) or store.archived_subscriptions.get(subscription_id)
    if subscription is None:
        await ui_send(update, context, "Не нашёл сервис. Попробуй ещё раз через /pay.", reply_markup=MENU, force_new=True)
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
    subscription.snoozed_until = None
    save_state()
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    context.user_data.pop("pending_payment_default", None)
    await update.message.reply_text(
        "Оплата сохранена.\n\n" + render_subscription(subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=MENU,
    )
    return ConversationHandler.END


async def pay_periods(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    periods = parse_int(update.message.text)
    if periods is None or periods <= 0:
        await ui_send(update, context, 'Введи количество периодов числом: 1, 2, 3...')
        return PAY_PERIODS

    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_payment_id")
    amount = context.user_data.get("pending_payment_amount")
    subscription = store.subscriptions.get(subscription_id) or store.archived_subscriptions.get(subscription_id)
    if subscription is None:
        await ui_send(update, context, "Не нашёл сервис. Попробуй ещё раз через /pay.", reply_markup=MENU, force_new=True)
        return ConversationHandler.END

    record_history(store, subscription, amount, "payment", note=f"Периодов: {periods}")
    advance_next_charge(subscription, periods)
    subscription.snoozed_until = None
    save_state()
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    context.user_data.pop("pending_payment_default", None)
    context.user_data.pop("pending_payment_default", None)
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
        await ui_send(update, context, "Не нашёл сервис. Попробуй ещё раз через /pay.", reply_markup=MENU, force_new=True)
        return ConversationHandler.END

    raw = update.message.text.strip()
    if raw == "-":
        current = effective_balance(subscription) or 0.0
        subscription.current_balance = current + amount
    else:
        new_balance = parse_float(raw)
        if new_balance is None:
            await ui_send(update, context, "Введи число или '-'.")
            return PAY_BALANCE
        subscription.current_balance = new_balance

    subscription.balance_updated_at = today_local()
    subscription.snoozed_until = None
    record_history(store, subscription, amount, "topup")
    save_state()
    context.user_data.pop("pending_payment_id", None)
    context.user_data.pop("pending_payment_amount", None)
    context.user_data.pop("pending_payment_default", None)
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
        await ui_send(update, context, "Нет активных балансовых сервисов.", reply_markup=MENU, force_new=True)
        return ConversationHandler.END

    buttons = [[f"{subscription.name} [{subscription.id}]"] for subscription in subscriptions]
    buttons.append(["/cancel"])
    await ui_send(update, context, "Выбери сервис, у которого нужно обновить баланс:", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True))
    return BALANCE_SELECT


async def set_balance_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    subscription_id = query.data.split(":", maxsplit=1)[1]
    context.user_data["pending_balance_id"] = subscription_id
    await ui_send(update, context, "Введи новый текущий баланс. Я сохраню его как новую точку отсчёта на сегодня.", reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True))
    return BALANCE_VALUE


async def set_balance_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    match = re.search(r"\[([0-9a-f]{8})\]$", update.message.text.strip())
    if not match:
        await ui_send(update, context, "Выбери сервис кнопкой из списка.")
        return BALANCE_SELECT
    context.user_data["pending_balance_id"] = match.group(1)
    await ui_send(update, context, "Введи новый текущий баланс. Я сохраню его как новую точку отсчёта на сегодня.", reply_markup=ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True))
    return BALANCE_VALUE


async def set_balance_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    value = parse_float(update.message.text)
    if value is None:
        await ui_send(update, context, "Введи корректное число.")
        return BALANCE_VALUE

    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_balance_id")
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await ui_send(update, context, "Сервис не найден.", reply_markup=MENU, force_new=True)
        return ConversationHandler.END

    subscription.current_balance = value
    subscription.balance_updated_at = today_local()
    subscription.snoozed_until = None
    save_state()
    context.user_data.pop("pending_balance_id", None)
    context.user_data.pop("pending_edit_id", None)
    context.user_data.pop("pending_edit_field", None)
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
    active_subscription = store.subscriptions.get(subscription_id)
    archived_subscription = store.archived_subscriptions.get(subscription_id)
    subscription = active_subscription or archived_subscription
    if subscription is None:
        await query.message.reply_text("Подписка не найдена.", reply_markup=MENU)
        return

    if archived_subscription is not None and active_subscription is None and action not in {"restore", "purge"}:
        await query.message.reply_text("Эта подписка уже в архиве. Используй восстановление или удаление навсегда.", reply_markup=MENU)
        return

    if action == "pause":
        subscription.active = False
        text = f"Поставил на паузу: {subscription.name}"
    elif action == "resume":
        subscription.active = True
        text = f"Возобновил: {subscription.name}"
    elif action == "snooze":
        subscription.snoozed_until = today_local() + timedelta(days=1)
        text = f"Отложил напоминания до {subscription.snoozed_until.strftime('%d.%m.%Y')}: {subscription.name}"
    elif action == "delete":
        store.subscriptions.pop(subscription_id, None)
        subscription.active = False
        subscription.snoozed_until = None
        store.archived_subscriptions[subscription_id] = subscription
        text = f"Перенёс в архив: {subscription.name}"
    elif action == "restore":
        store.archived_subscriptions.pop(subscription_id, None)
        subscription.active = True
        store.subscriptions[subscription_id] = subscription
        text = f"Восстановил из архива: {subscription.name}"
    elif action == "purge":
        store.archived_subscriptions.pop(subscription_id, None)
        text = f"Удалил навсегда из архива: {subscription.name}"
    else:
        text = "Неизвестное действие."

    save_state()
    await query.message.reply_text(text, reply_markup=MENU)


async def show_subscription_history_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    query = update.callback_query
    await query.answer()
    subscription_id = query.data.split(":", maxsplit=1)[1]
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription = store.subscriptions.get(subscription_id) or store.archived_subscriptions.get(subscription_id)
    if subscription is None:
        await query.message.reply_text("Подписка не найдена.", reply_markup=MENU)
        return
    markup = build_inline_actions(subscription) if subscription_id in store.subscriptions else build_archive_actions(subscription)
    await query.message.reply_text(
        subscription_history_text(store, subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=markup,
    )


def build_edit_keyboard(subscription: Subscription) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("Название", callback_data=f"editfield:name:{subscription.id}"),
            InlineKeyboardButton("Проект", callback_data=f"editfield:project:{subscription.id}"),
        ],
        [
            InlineKeyboardButton("Категория", callback_data=f"editfield:category:{subscription.id}"),
            InlineKeyboardButton("Теги", callback_data=f"editfield:tags:{subscription.id}"),
        ],
        [
            InlineKeyboardButton("Сумма", callback_data=f"editfield:amount:{subscription.id}"),
            InlineKeyboardButton("Валюта", callback_data=f"editfield:currency:{subscription.id}"),
        ],
        [InlineKeyboardButton("Заметка", callback_data=f"editfield:notes:{subscription.id}")],
        [
            InlineKeyboardButton("Сайт", callback_data=f"editfield:site_url:{subscription.id}"),
            InlineKeyboardButton("Ссылка на оплату", callback_data=f"editfield:payment_url:{subscription.id}"),
        ],
    ]
    if subscription.kind == "balance":
        rows.extend([
            [
                InlineKeyboardButton("Текущий баланс", callback_data=f"editfield:current_balance:{subscription.id}"),
                InlineKeyboardButton("Мин. порог", callback_data=f"editfield:min_balance:{subscription.id}"),
            ],
            [
                InlineKeyboardButton("Тип расхода", callback_data=f"editfield:spending_mode:{subscription.id}"),
                InlineKeyboardButton("Сумма расхода", callback_data=f"editfield:spend_amount:{subscription.id}"),
            ],
            [InlineKeyboardButton("Период расхода", callback_data=f"editfield:spend_period_days:{subscription.id}")],
        ])
    else:
        rows.extend([
            [
                InlineKeyboardButton("Дата списания", callback_data=f"editfield:next_charge_date:{subscription.id}"),
                InlineKeyboardButton("Напоминания", callback_data=f"editfield:reminders:{subscription.id}"),
            ],
            [InlineKeyboardButton("Повтор просрочки", callback_data=f"editfield:repeat:{subscription.id}")],
        ])
    return InlineKeyboardMarkup(rows)


def edit_field_prompt(field_name: str, subscription: Subscription) -> str:
    prompts = {
        "name": "Введи новое название.",
        "project": "Введи новый проект.",
        "category": "Введи новую категорию.",
        "tags": "Введи теги через запятую или '-' чтобы очистить.",
        "amount": "Введи новую сумму.",
        "currency": "Введи валюту: KZT, RUB, EUR, USD или TRY.",
        "notes": "Введи новую заметку или '-' чтобы очистить.",
        "site_url": "Введи ссылку на сайт или '-' чтобы очистить.",
        "payment_url": "Введи ссылку на оплату или '-' чтобы очистить.",
        "next_charge_date": "Введи новую дату списания. Формат: DD.MM.YYYY или YYYY-MM-DD",
        "reminders": "Введи дни напоминаний через запятую. Пример: 7,3,1,0",
        "repeat": "Повторять просрочку ежедневно? Ответь Да или Нет.",
        "current_balance": "Введи новый текущий баланс.",
        "min_balance": "Введи новый минимальный порог.",
        "spending_mode": "Введи тип расхода: manual, fixed или daily_avg. Можно и по-русски: Ручной контроль / Фиксированное списание / Средний расход в день.",
        "spend_amount": "Введи новую сумму расхода.",
        "spend_period_days": "Введи новый период списания в днях.",
    }
    return prompts[field_name]


async def edit_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    if not store.subscriptions:
        await update.message.reply_text("Подписок пока нет.", reply_markup=MENU)
        return ConversationHandler.END
    buttons = [[f"{subscription.name} [{subscription.id}]"] for subscription in sorted(store.subscriptions.values(), key=lambda x: x.name.lower())]
    buttons.append(["/cancel"])
    await update.message.reply_text(
        "Выбери подписку для редактирования:",
        reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True),
    )
    return EDIT_SELECT


async def edit_from_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    subscription_id = query.data.split(":", maxsplit=1)[1]
    context.user_data["pending_edit_id"] = subscription_id
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await query.message.reply_text("Подписка не найдена.", reply_markup=MENU)
        return ConversationHandler.END
    await query.message.reply_text(
        "Что изменить?",
        reply_markup=build_edit_keyboard(subscription),
    )
    return EDIT_FIELD


async def edit_select(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    match = re.search(r"\[([0-9a-f]{8})\]$", update.message.text.strip())
    if not match:
        await update.message.reply_text("Выбери подписку кнопкой из списка.")
        return EDIT_SELECT
    subscription_id = match.group(1)
    context.user_data["pending_edit_id"] = subscription_id
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await update.message.reply_text("Подписка не найдена.", reply_markup=MENU)
        return ConversationHandler.END
    await update.message.reply_text("Что изменить?", reply_markup=build_edit_keyboard(subscription))
    return EDIT_FIELD


async def edit_field_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if not await ensure_authorized(update):
        return ConversationHandler.END
    query = update.callback_query
    await query.answer()
    _, field_name, subscription_id = query.data.split(":", maxsplit=2)
    context.user_data["pending_edit_id"] = subscription_id
    context.user_data["pending_edit_field"] = field_name
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None:
        await query.message.reply_text("Подписка не найдена.", reply_markup=MENU)
        return ConversationHandler.END
    prompt = edit_field_prompt(field_name, subscription)
    reply_markup = CURRENCY_KEYBOARD if field_name == "currency" else YES_NO_KEYBOARD if field_name == "repeat" else YES_SKIP_KEYBOARD if field_name in {"site_url", "payment_url", "notes"} else ReplyKeyboardMarkup([["/cancel"]], resize_keyboard=True, one_time_keyboard=True)
    await query.message.reply_text(prompt, reply_markup=reply_markup)
    return EDIT_VALUE


async def edit_value(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    user = update.effective_user
    chat = update.effective_chat
    store = get_store(user.id, chat.id if chat else None)
    subscription_id = context.user_data.get("pending_edit_id")
    field_name = context.user_data.get("pending_edit_field")
    subscription = store.subscriptions.get(subscription_id)
    if subscription is None or not field_name:
        await update.message.reply_text("Не удалось продолжить редактирование. Запусти /edit ещё раз.", reply_markup=MENU)
        return ConversationHandler.END

    raw = update.message.text.strip()
    try:
        if field_name in {"name", "project", "category"}:
            if not raw:
                raise ValueError("Значение не должно быть пустым.")
            setattr(subscription, field_name, raw)
        elif field_name == "tags":
            subscription.tags = parse_tags_input(raw)
        elif field_name == "amount":
            value = parse_float(raw)
            if value is None:
                raise ValueError("Нужна корректная сумма.")
            subscription.amount = value
        elif field_name == "currency":
            value = parse_currency_input(raw)
            if value is None:
                raise ValueError("Выбери KZT, RUB, EUR, USD или TRY.")
            subscription.currency = value
        elif field_name == "notes":
            subscription.notes = "" if raw == "-" else raw
        elif field_name == "site_url":
            subscription.site_url = clean_optional_url(raw) or ""
        elif field_name == "payment_url":
            subscription.payment_url = clean_optional_url(raw) or ""
        elif field_name == "next_charge_date":
            value = parse_date_input(raw)
            if value is None:
                raise ValueError("Не смог распознать дату.")
            subscription.next_charge_date = value
            subscription.snoozed_until = None
        elif field_name == "reminders":
            value = parse_reminder_offsets(raw)
            if value is None:
                raise ValueError("Нужен список дней через запятую.")
            subscription.reminder_offsets = value
            subscription.remind_before_days = value[0]
        elif field_name == "repeat":
            value = parse_yes_no(raw)
            if value is None:
                raise ValueError("Ответь Да или Нет.")
            subscription.repeat_daily_until_paid = value
        elif field_name == "current_balance":
            value = parse_float(raw)
            if value is None:
                raise ValueError("Нужна корректная сумма.")
            subscription.current_balance = value
            subscription.balance_updated_at = today_local()
            subscription.snoozed_until = None
        elif field_name == "min_balance":
            value = parse_float(raw)
            if value is None:
                raise ValueError("Нужна корректная сумма.")
            subscription.min_balance = value
        elif field_name == "spending_mode":
            value = parse_balance_mode_input(raw)
            if value is None:
                raise ValueError("Используй manual, fixed или daily_avg.")
            subscription.spending_mode = value
            if value == "manual":
                subscription.spend_amount = None
                subscription.spend_period_days = None
            elif value == "daily_avg":
                subscription.spend_period_days = 1
        elif field_name == "spend_amount":
            value = parse_float(raw)
            if value is None:
                raise ValueError("Нужна корректная сумма.")
            subscription.spend_amount = value
        elif field_name == "spend_period_days":
            value = parse_int(raw)
            if value is None or value <= 0:
                raise ValueError("Нужно положительное число дней.")
            subscription.spend_period_days = value
        else:
            raise ValueError("Неизвестное поле.")
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return EDIT_VALUE

    save_state()
    context.user_data.pop("pending_edit_id", None)
    context.user_data.pop("pending_edit_field", None)
    await update.message.reply_text(
        "Подписка обновлена.\n\n" + render_subscription(subscription),
        parse_mode=ParseMode.HTML,
        reply_markup=build_inline_actions(subscription),
    )
    return ConversationHandler.END


async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not await ensure_authorized(update):
        return
    text = update.message.text.strip()
    routes = {
        "📋 Подписки": list_command,
        "📅 Сегодня": today_command,
        "⏰ Скоро списания": soon_command,
        "🪫 Низкий баланс": topup_command,
        "💼 Сводка": dashboard_command,
        "🔮 Прогноз": forecast_command,
        "🗓 События на год": year_command,
        "📈 Отчёт": report_command,
        "🧾 История": history_command,
        "🗃 Архив": archive_command,
        "📤 Экспорт": export_command,
        "❓ Помощь": help_command,
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
    today = today_local()
    for store in RUNTIME_USERS.values():
        if store.chat_id is None:
            continue
        for subscription in store.subscriptions.values():
            if not subscription.active or is_snoozed(subscription, today):
                continue
            if subscription.kind == "balance":
                current_balance = effective_balance(subscription)
                if current_balance is None or subscription.min_balance is None:
                    continue

                if current_balance <= subscription.min_balance:
                    key = build_alert_key("low", subscription.id)
                    if store.sent_alerts.get(key) == today:
                        continue
                    store.sent_alerts[key] = today
                    await context.bot.send_message(
                        chat_id=store.chat_id,
                        text=(
                            "🪫 Баланс ниже порога\n\n" + render_subscription(subscription)
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=build_inline_actions(subscription),
                    )
                    save_state()
                    continue

                days_left = days_until_balance_threshold(subscription)
                if days_left is not None and 0 < days_left <= BALANCE_WARNING_DAYS:
                    key = build_alert_key("balance_warn", subscription.id)
                    if store.sent_alerts.get(key) == today:
                        continue
                    store.sent_alerts[key] = today
                    await context.bot.send_message(
                        chat_id=store.chat_id,
                        text=(
                            f"⏰ Баланс дойдёт до порога примерно через {days_left} дн.\n\n"
                            + render_subscription(subscription)
                        ),
                        parse_mode=ParseMode.HTML,
                        reply_markup=build_inline_actions(subscription),
                    )
                    save_state()
                continue

            if subscription.next_charge_date is None:
                continue
            delta = (subscription.next_charge_date - today).days
            offsets = current_reminder_offsets(subscription)
            should_send = delta in offsets
            if delta < 0 and subscription.repeat_daily_until_paid:
                should_send = True
            if not should_send:
                continue
            key = build_alert_key("due", subscription.id)
            if store.sent_alerts.get(key) == today:
                continue
            store.sent_alerts[key] = today
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
            save_state()


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


async def weekly_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    for store in RUNTIME_USERS.values():
        if store.chat_id is None:
            continue
        start, end = last_seven_days_bounds()
        lines = build_period_summary_lines(store, 'Еженедельная сводка', start, end)
        await context.bot.send_message(chat_id=store.chat_id, text='\n'.join(lines), parse_mode=ParseMode.HTML)


async def monthly_summary_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    for store in RUNTIME_USERS.values():
        if store.chat_id is None:
            continue
        start, end = previous_month_bounds()
        lines = build_period_summary_lines(store, 'Месячная сводка', start, end)
        await context.bot.send_message(chat_id=store.chat_id, text='\n'.join(lines), parse_mode=ParseMode.HTML)


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
            ADD_CATEGORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_category)],
            ADD_TAGS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_tags)],
            ADD_NEXT_DATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_next_date)],
            ADD_REMIND_DAYS: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_remind_days)],
            ADD_REPEAT_UNTIL_PAID: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_repeat_until_paid)],
            ADD_CURRENT_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_current_balance)],
            ADD_MIN_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_min_balance)],
            ADD_BALANCE_MODE: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_balance_mode)],
            ADD_SPEND_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_spend_amount)],
            ADD_SPEND_PERIOD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_spend_period)],
            ADD_NOTES: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_notes)],
            ADD_SITE_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_site_url)],
            ADD_PAYMENT_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_payment_url)],
            ADD_CONFIRM: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_confirm)],
            ADD_CONFIRM_EDIT_FIELD: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_confirm_edit_field)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="add_subscription",
        persistent=False,
    )

    pay_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("pay", pay_start),
            MessageHandler(filters.Regex(r"^💸 Отметить оплату$"), pay_start),
            CallbackQueryHandler(pay_from_callback, pattern=r"^(pay|paydefault):"),
        ],
        states={
            PAY_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_select)],
            PAY_AMOUNT: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_amount)],
            PAY_PERIODS: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_periods)],
            PAY_BALANCE: [MessageHandler(filters.TEXT & ~filters.COMMAND, pay_balance)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="pay_subscription",
        persistent=False,
        per_message=False,
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
        per_message=False,
    )


    edit_conversation = ConversationHandler(
        entry_points=[
            CommandHandler("edit", edit_start),
            CallbackQueryHandler(edit_from_callback, pattern=r"^edit:"),
        ],
        states={
            EDIT_SELECT: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_select)],
            EDIT_FIELD: [CallbackQueryHandler(edit_field_callback, pattern=r"^editfield:")],
            EDIT_VALUE: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_value)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        name="edit_subscription",
        persistent=False,
        per_message=False,
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("demo", demo_command))
    application.add_handler(CommandHandler("users", users_command))
    application.add_handler(CommandHandler("list", list_command))
    application.add_handler(CommandHandler("today", today_command))
    application.add_handler(CommandHandler("soon", soon_command))
    application.add_handler(CommandHandler("topup", topup_command))
    application.add_handler(CommandHandler("dashboard", dashboard_command))
    application.add_handler(CommandHandler("forecast", forecast_command))
    application.add_handler(CommandHandler("year", year_command))
    application.add_handler(CommandHandler("report", report_command))
    application.add_handler(CommandHandler("weekly", weekly_command))
    application.add_handler(CommandHandler("monthly", monthly_command))
    application.add_handler(CommandHandler("archive", archive_command))
    application.add_handler(CommandHandler("find", find_command))
    application.add_handler(CommandHandler("filter", filter_command))
    application.add_handler(CommandHandler("history", history_command))
    application.add_handler(CommandHandler("export", export_command))
    application.add_handler(CommandHandler("import", import_command))
    application.add_handler(add_conversation)
    application.add_handler(pay_conversation)
    application.add_handler(CallbackQueryHandler(year_month_callback, pattern=r"^yearmonth:"))
    application.add_handler(CallbackQueryHandler(year_menu_back_callback, pattern=r"^yearmenu:back$"))
    application.add_handler(balance_conversation)
    application.add_handler(edit_conversation)
    application.add_handler(MessageHandler(filters.Document.ALL, import_document_handler))
    application.add_handler(CallbackQueryHandler(show_subscription_history_callback, pattern=r"^historysub:"))
    application.add_handler(
        CallbackQueryHandler(subscription_action_callback, pattern=r"^(pause|resume|delete|snooze|restore|purge):")
    )
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router))
    application.add_error_handler(error_handler)


def schedule_jobs(application: Application) -> None:
    if application.job_queue is None:
        LOGGER.warning("JobQueue is not available; scheduled reminders are disabled")
        return

    weekly_ptb_day = normalize_weekly_weekday(WEEKLY_SUMMARY_WEEKDAY)
    monthly_day = clamp_month_day(MONTHLY_SUMMARY_DAY)

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
    application.job_queue.run_daily(
        weekly_summary_job,
        time=parse_hhmm(WEEKLY_SUMMARY_TIME),
        days=(weekly_ptb_day,),
        name="weekly_summary_job",
    )
    application.job_queue.run_monthly(
        monthly_summary_job,
        when=parse_hhmm(MONTHLY_SUMMARY_TIME),
        day=monthly_day,
        name="monthly_summary_job",
    )
    LOGGER.info(
        "Scheduled jobs: alerts=%s daily=%s weekly=%s (env weekday=%s -> ptb weekday=%s) monthly=%s day=%s",
        ALERTS_TIME,
        DAILY_SUMMARY_TIME,
        WEEKLY_SUMMARY_TIME,
        WEEKLY_SUMMARY_WEEKDAY,
        weekly_ptb_day,
        MONTHLY_SUMMARY_TIME,
        monthly_day,
    )


def main() -> None:
    if not TOKEN:
        raise RuntimeError("Не задан TG_BOT_API_KEY в .env")

    load_state()
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
    LOGGER.info("Bot started in timezone %s | data file %s", TIMEZONE_NAME, DATA_FILE)
    application.run_polling()


if __name__ == "__main__":
    main()
