import logging
import os
import uuid
from telegram.error import TelegramError, Conflict
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from dotenv import load_dotenv, find_dotenv
from ckassa import CKassaPayment
from parser import HoroscopeParser
import json
from datetime import datetime, timedelta
import asyncio
from aiohttp import web
from typing import Optional

# Логирование
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler('/root/TAROBOT/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv(find_dotenv())
BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS = os.getenv('ADMIN_ID', '').split(',')
CHANNELS = [
    {"id": os.getenv('CHANNEL1_ID', ''), "url": os.getenv('CHANNEL1_URL', '')},
    {"id": os.getenv('CHANNEL2_ID', ''), "url": os.getenv('CHANNEL2_URL', '')},
]

# Инициализация CKassa и парсера
ckassa = CKassaPayment()
horoscope_parser = HoroscopeParser()

# Планы подписки
SUBSCRIPTION_PLANS = {
    "week": {"name": "7 дней", "price": 159, "period": "7 дней", "days": 7, "per_day": "22р в день"},
    "month": {"name": "30 дней", "price": 359, "period": "30 дней", "days": 30, "per_day": "11р в день"},
}

# База данных пользователей
class UserDB:
    def __init__(self):
        self.users = {}
        self.db_file = "/root/TAROBOT/users_db.json"
        self._load()

    def _load(self) -> None:
        try:
            with open(self.db_file, 'r') as f:
                self.users = json.load(f)
        except FileNotFoundError:
            self._save()

    def _save(self) -> None:
        os.makedirs(os.path.dirname(self.db_file), exist_ok=True)
        with open(self.db_file, 'w') as f:
            json.dump(self.users, f, indent=4)

    def add_user(self, user_id: int, username: Optional[str] = None) -> None:
        user_id_str = str(user_id)
        if user_id_str not in self.users:
            self.users[user_id_str] = {
                'username': username,
                'subscription': {
                    'active': False,
                    'expires': None,
                    'type': None,
                    'start_date': None,
                    'trial_used': False
                },
                'last_active': datetime.now().isoformat()
            }
            self._save()

    def set_subscription(self, user_id: int, days: int, sub_type: str = 'paid') -> None:
        user_id_str = str(user_id)
        if user_id_str not in self.users:
            self.add_user(user_id)
        expires = datetime.now() + timedelta(days=days)
        start_date = datetime.now()
        self.users[user_id_str]['subscription'].update({
            'active': True,
            'expires': expires.isoformat(),
            'type': sub_type,
            'start_date': start_date.isoformat()
        })
        if sub_type == 'trial':
            self.users[user_id_str]['subscription']['trial_used'] = True
        self._save()
        logger.info(f"Подписка установлена для {user_id_str}: {days} дней, тип={sub_type}, начало={start_date}, истекает {expires}")

    def has_active_subscription(self, user_id: int) -> bool:
        user_id_str = str(user_id)
        if user_id_str not in self.users or not self.users[user_id_str]['subscription']['active']:
            return False
        expires = self.users[user_id_str]['subscription'].get('expires')
        if expires and datetime.now() > datetime.fromisoformat(expires):
            self.users[user_id_str]['subscription']['active'] = False
            self._save()
            return False
        return True

    def get_expiry(self, user_id: int) -> Optional[datetime]:
        user_id_str = str(user_id)
        if user_id_str in self.users and self.users[user_id_str]['subscription']['active']:
            expires = self.users[user_id_str]['subscription'].get('expires')
            return datetime.fromisoformat(expires) if expires else None
        return None

    def get_subscription_start(self, user_id: int) -> Optional[datetime]:
        user_id_str = str(user_id)
        if user_id_str in self.users and self.users[user_id_str]['subscription']['active']:
            start_date = self.users[user_id_str]['subscription'].get('start_date')
            return datetime.fromisoformat(start_date) if start_date else None
        return None

    def get_all_users(self):
        return [{'user_id': k, **v} for k, v in self.users.items()]

    def format_remaining_time(self, expires: datetime) -> str:
        if not expires:
            return ""
        now = datetime.now()
        if now > expires:
            return "истекла"
        diff = expires - now
        days = diff.days
        hours = diff.seconds // 3600
        if days > 0:
            return f"{days}д {hours}ч"
        else:
            minutes = (diff.seconds % 3600) // 60
            return f"{hours}ч {minutes}м"

    def can_use_trial(self, user_id: int) -> bool:
        user_id_str = str(user_id)
        if user_id_str not in self.users:
            self.add_user(user_id)
        return not self.users[user_id_str]['subscription'].get('trial_used', False)

# Менеджер сообщений
class MessageManager:
    def __init__(self):
        self.messages_file = "/root/TAROBOT/messages.json"
        self.messages = self._load()

    def _load(self) -> dict:
        try:
            with open(self.messages_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def _save(self) -> None:
        try:
            with open(self.messages_file, 'w', encoding='utf-8') as f:
                json.dump(self.messages, f, ensure_ascii=False, indent=4)
        except Exception as e:
            logger.error(f"Ошибка сохранения сообщений: {e}")

    def get(self, key: str, default: str = "") -> str:
        return self.messages.get(key, default)

    def set(self, key: str, value: str) -> None:
        self.messages[key] = value
        self._save()

# Инициализация
db = UserDB()
msg_manager = MessageManager()

# Утилиты
def get_main_menu(user_id: int) -> ReplyKeyboardMarkup:
    is_active = db.has_active_subscription(user_id)
    expires = db.get_expiry(user_id)
    sub_text = "💎 Оформить подписку" if not is_active else f"💎 Подписка активна (осталось {db.format_remaining_time(expires)})"
    return ReplyKeyboardMarkup([
        [KeyboardButton("✨ Выбрать расклад/узнать прайс"), KeyboardButton("Психология: как проходит/прайс💜")],
        [KeyboardButton("🌟 Гороскоп на сегодня"), KeyboardButton("🎴 Карта Таро дня")],
        [KeyboardButton(sub_text)],
    ], resize_keyboard=True)

async def check_channel_subscription(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if str(user_id) in ADMIN_IDS:
        return True
    for channel in CHANNELS:
        try:
            chat_id = channel["id"] if channel["id"].startswith('-100') or channel["id"].startswith('@') else '@' + channel["id"]
            member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status not in ['member', 'administrator', 'creator']:
                return False
        except Exception as e:
            logger.error(f"Ошибка проверки канала {channel['id']}: {e}")
            return False
    return True

async def send_subscription_notification(bot, user_id: int, plan_id: str = None, is_trial: bool = False):
    if is_trial:
        days = 3
        expiry_date = (datetime.now() + timedelta(days=days)).strftime('%d.%m.%Y')
        text = (
            f"🎉 Пробный период на 3 дня активирован!\n\n"
            f"📅 Дата окончания: {expiry_date}\n\n"
            f"Теперь вам доступны:\n"
            f"• Гороскоп\n• Таро\n• Предсказания\n\n"
            f"Нажмите /start для начала!"
        )
    else:
        plan = SUBSCRIPTION_PLANS[plan_id]
        days = plan["days"]
        expires = db.get_expiry(user_id)
        expiry_date = expires.strftime('%d.%m.%Y') if expires else "Неизвестно"
        text = (
            f"🎉 Поздравляем! Ваша подписка успешно активирована!\n\n"
            f"✨ План: {plan['period']} за {plan['price']} руб.\n"
            f"📅 Дата окончания: {expiry_date}\n\n"
            f"Теперь вам доступны:\n"
            f"• Гороскоп\n• Таро\n• Предсказания\n\n"
            f"Нажмите /start для начала!"
        )
    try:
        await bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=get_main_menu(user_id)
        )
        logger.info(f"Уведомление отправлено пользователю {user_id}: {'пробный период' if is_trial else 'платная подписка'}")
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления пользователю {user_id}: {e}")

# Callback-обработчик для CKassa
async def handle_callback(request: web.Request) -> web.Response:
    try:
        data = await request.json()
        logger.info(f"Получен callback от CKassa: {json.dumps(data, indent=2)}")

        user_id = data.get("property", {}).get("ИДЕНТИФИКАТОР")
        status = data.get("state")
        amount = data.get("amount")
        reg_pay_num = data.get("regPayNum")

        if not user_id:
            logger.error("❌ user_id не найден в callback!")
            return web.Response(text="user_id is required", status=400)

        if not status:
            logger.error("❌ status не найден в callback!")
            return web.Response(text="status is required", status=400)

        if not amount:
            logger.error("❌ amount не найден в callback!")
            return web.Response(text="amount is required", status=400)

        if status.upper() != "PAYED":
            logger.warning(f"Платеж не подтвержден! Статус: {status}")
            return web.Response(text="Payment not confirmed", status=200)

        amount_rub = float(amount) / 100

        if amount_rub == 359:
            plan_id = "month"
        elif amount_rub == 159:
            plan_id = "week"
        else:
            logger.error(f"Неизвестная сумма платежа: {amount_rub} руб")
            return web.Response(text="Unknown payment amount", status=400)

        days = SUBSCRIPTION_PLANS[plan_id]["days"]
        db.set_subscription(int(user_id), days, sub_type="paid")

        bot = Application.builder().token(BOT_TOKEN).build()
        await send_subscription_notification(bot.bot, int(user_id), plan_id=plan_id, is_trial=False)

        logger.info(
            f"✅ Подписка активирована! "
            f"User ID: {user_id}, "
            f"Тариф: {plan_id} ({days} дней), "
            f"Сумма: {amount_rub} руб, "
            f"Номер платежа: {reg_pay_num}"
        )

        return web.Response(text="OK", status=200)

    except json.JSONDecodeError:
        logger.error("❌ Неверный JSON в callback!")
        return web.Response(text="Invalid JSON", status=400)
    except Exception as e:
        logger.error(f"❌ Ошибка обработки callback: {e}", exc_info=True)
        return web.Response(text="Server error", status=500)

# Обработчики Telegram
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if not await check_channel_subscription(user_id, context):
        keyboard = [[InlineKeyboardButton("📢 Подписаться", url=ch["url"])] for ch in CHANNELS if ch["url"]]
        keyboard.append([InlineKeyboardButton("🔄 Проверить", callback_data="check_sub")])
        await update.message.reply_text(
            "⚠️ Подпишись на каналы:\n1. Подпишись на все\n2. Нажми «Проверить»",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
        return

    db.add_user(user_id, update.effective_user.username)
    await update.message.reply_text(
        msg_manager.get("start_message", "Привет! Я твой помощник по Таро и психологии."),
        reply_markup=get_main_menu(user_id)
    )

async def admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        await update.message.reply_text("⛔️ У вас нет доступа к этой команде.")
        return

    keyboard = [
        [KeyboardButton("👥 Список пользователей"), KeyboardButton("📊 Статистика")],
        [KeyboardButton("📝 Редактировать сообщения"), KeyboardButton("📢 Рассылка")],
        [KeyboardButton("◶ Назад")]
    ]
    await update.message.reply_text(
        "🔧 Панель администратора\n\nВыберите действие:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        return
    users = db.get_all_users()
    total_users = len(users)

    active_today = 0
    today = datetime.now().date()

    for user in users:
        try:
            last_active = user.get('last_active')
            if last_active:
                if isinstance(last_active, str):
                    user_date = datetime.fromisoformat(last_active).date()
                elif isinstance(last_active, datetime):
                    user_date = last_active.date()
                else:
                    continue
                if user_date == today:
                    active_today += 1
        except (ValueError, TypeError):
            continue

    stats_text = (
        "📊 Статистика бота:\n\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"📱 Активных сегодня: {active_today}\n"
    )
    await update.message.reply_text(stats_text, reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True))

async def admin_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    request_id = str(uuid.uuid4())[:8]
    logger.info(f"[{request_id}] admin_users called by user_id: {update.effective_user.id}, ADMIN_IDS: {ADMIN_IDS}")

    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        logger.warning(f"[{request_id}] User {user_id} is not an admin")
        await update.message.reply_text(
            "⛔️ У вас нет доступа к этой команде.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
        )
        return

    try:
        users = db.get_all_users()
        logger.info(f"[{request_id}] Retrieved {len(users)} users from database")

        if not users:
            await update.message.reply_text(
                "👥 Нет зарегистрированных пользователей.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
            )
            logger.info(f"[{request_id}] No users found, response sent")
            return

        filtered_users = []
        for user in users:
            user_id = int(user.get('user_id', 0))
            if not user_id or not db.has_active_subscription(user_id):
                continue
            subscription = user.get('subscription', {})
            sub_type = subscription.get('type', 'paid')
            if sub_type != 'paid':
                continue
            start_date = db.get_subscription_start(user_id)
            if not start_date:
                continue
            filtered_users.append(user)

        logger.info(f"[{request_id}] Filtered users: {len(filtered_users)}")

        if not filtered_users:
            await update.message.reply_text(
                "👥 Нет пользователей с активными платными подписками.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
            )
            logger.info(f"[{request_id}] No active paid users found")
            return

        users_per_message = 15
        for i in range(0, len(filtered_users), users_per_message):
            users_text = "👥 Список пользователей:\n\n" if i == 0 else ""
            for user in filtered_users[i:i + users_per_message]:
                try:
                    username = user.get('username', 'Без имени') or 'Без имени'
                    user_id = user.get('user_id', 'Неизвестно')
                    start_date = db.get_subscription_start(int(user_id))
                    expires = db.get_expiry(int(user_id))
                    start_str = start_date.strftime('%d.%m') if start_date else 'Неизвестно'
                    expires_str = expires.strftime('%d.%m') if expires else 'Неизвестно'

                    users_text += (
                        f"Имя: {username}\n"
                        f"ID: {user_id}\n"
                        f"Срок: {start_str} - {expires_str} Подписка оплачена🟢\n\n"
                    )
                except Exception as e:
                    logger.error(f"[{request_id}] Error processing user {user.get('user_id', 'Unknown')}: {e}")
                    continue

            logger.info(f"[{request_id}] Length of users_text (chunk {i // users_per_message + 1}): {len(users_text)}")
            if len(users_text) > 4000:
                users_text = users_text[:3950] + "\n... (сокращено)"
                logger.warning(f"[{request_id}] Message truncated due to length limit")

            for attempt in range(3):
                try:
                    await update.message.reply_text(
                        users_text,
                        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                    )
                    logger.info(f"[{request_id}] Message chunk {i // users_per_message + 1} sent successfully")
                    break
                except Conflict as ce:
                    logger.error(f"[{request_id}] Conflict error on attempt {attempt + 1}: {ce}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error(f"[{request_id}] Failed to send message chunk {i // users_per_message + 1} after 3 attempts")
                        await update.message.reply_text(
                            "❌ Ошибка отправки части списка: конфликт запросов. Попробуйте позже.",
                            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                        )
                        return
                except TelegramError as te:
                    logger.warning(f"[{request_id}] TelegramError on attempt {attempt + 1}: {te}")
                    if attempt < 2:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        logger.error(f"[{request_id}] Failed to send message chunk {i // users_per_message + 1} after 3 attempts")
                        await update.message.reply_text(
                            "❌ Ошибка отправки части списка. Попробуйте позже.",
                            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                        )
                        break
            await asyncio.sleep(0.5)

        logger.info(f"[{request_id}] admin_users completed successfully")

    except Exception as e:
        logger.error(f"[{request_id}] Error in admin_users: {e}", exc_info=True)
        await update.message.reply_text(
            "❌ Произошла ошибка при получении списка пользователей. Попробуйте позже.",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
        )

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        return

    if 'awaiting_broadcast' in context.user_data:
        del context.user_data['awaiting_broadcast']

    context.user_data['awaiting_broadcast'] = True
    await update.message.reply_text(
        "📢 Введите текст для рассылки:\n\n"
        "Поддерживается базовая HTML-разметка:\n"
        "• <b>жирный текст</b>\n"
        "• <i>курсив</i>\n"
        "• <a href='ссылка'>текст ссылки</a>",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("❌ Отменить")]], resize_keyboard=True)
    )

async def handle_broadcast_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.user_data.get('awaiting_broadcast'):
        return

    broadcast_text = update.message.text

    if broadcast_text == "❌ Отменить":
        del context.user_data['awaiting_broadcast']
        await update.message.reply_text(
            "❌ Рассылка отменена.",
            reply_markup=get_main_menu(update.effective_user.id)
        )
        return

    users = db.get_all_users()
    success_count = 0
    fail_count = 0
    status_message = await update.message.reply_text("📢 Начинаю рассылку...")

    for user in users:
        try:
            user_id = user.get('user_id')
            await context.bot.send_message(chat_id=user_id, text=broadcast_text, parse_mode='HTML')
            success_count += 1
            if success_count % 10 == 0:
                await status_message.edit_text(f"📢 Отправлено: {success_count}\n❌ Ошибок: {fail_count}")
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user.get('user_id')}: {e}")
            fail_count += 1

    if 'awaiting_broadcast' in context.user_data:
        del context.user_data['awaiting_broadcast']

    await status_message.edit_text(
        f"📢 Рассылка завершена\n\n✅ Успешно отправлено: {success_count}\n❌ Ошибок: {fail_count}",
        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
    )

async def admin_edit_messages(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        return
    keyboard = [
        [KeyboardButton("📝 Приветственное сообщение"), KeyboardButton("📝 Текст расклада")],
        [KeyboardButton("📝 Текст психологии"), KeyboardButton("◶ Назад")]
    ]
    await update.message.reply_text(
        "📝 Выберите сообщение для редактирования:",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

async def handle_edit_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    if str(user_id) not in ADMIN_IDS:
        return
    text = update.message.text
    message_map = {
        "📝 Приветственное сообщение": "start_message",
        "📝 Текст расклада": "select_spread",
        "📝 Текст психологии": "how_spread_works"
    }
    if text in message_map:
        context.user_data['editing_message'] = message_map[text]
        current_text = msg_manager.get(message_map[text])
        await update.message.reply_text(
            f"📝 Редактирование '{text}':\n\nТекущий текст:\n{current_text}\n\nОтправьте новый текст:",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
        )
    elif context.user_data.get('editing_message'):
        msg_manager.set(context.user_data['editing_message'], text)
        await update.message.reply_text(
            f"✅ Сообщение '{context.user_data['editing_message']}' обновлено!",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
        )
        context.user_data['editing_message'] = None

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text
    user_id = update.effective_user.id
    logger.info(f"Сообщение от {user_id}: {text}")

    if context.user_data.get('awaiting_broadcast'):
        await handle_broadcast_message(update, context)
        return
    if context.user_data.get('editing_message') or text in ["📝 Приветственное сообщение", "📝 Текст расклада", "📝 Текст психологии"]:
        await handle_edit_message(update, context)
        return

    if text == "◶ Назад":
        await start(update, context)
        return

    if text in ["👥 Список пользователей", "📊 Статистика", "📢 Рассылка", "📝 Редактировать сообщения"]:
        if text == "👥 Список пользователей":
            await admin_users(update, context)
        elif text == "📊 Статистика":
            await admin_stats(update, context)
        elif text == "📢 Рассылка":
            await admin_broadcast(update, context)
        elif text == "📝 Редактировать сообщения":
            await admin_edit_messages(update, context)
        return

    if text in ["✨ Выбрать расклад/узнать прайс", "Психология: как проходит/прайс💜"]:
        key = "select_spread" if "расклад" in text else "how_spread_works"
        await update.message.reply_text(
            msg_manager.get(key),
            reply_markup=ReplyKeyboardMarkup([
                [KeyboardButton("ЗАПИСАТЬСЯ"), KeyboardButton("СПРОСИТЬ")],
                [KeyboardButton("◶ Назад")]
            ], resize_keyboard=True)
        )
        return

    if text in ["🌟 Гороскоп на сегодня", "🎴 Карта Таро дня"]:
        if user_id != 7254288870 and not db.has_active_subscription(user_id):
            keyboard = [
                [KeyboardButton("💎 Оформить подписку")],
                [KeyboardButton("◶ Назад")]
            ]
            if db.can_use_trial(user_id):
                keyboard.insert(0, [KeyboardButton("🎁 Активировать пробный период (3 дня)")])
            await update.message.reply_text(
                "⭐️ Эта функция доступна только подписчикам!\nОформи подписку или активируй пробный период:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        if "Гороскоп" in text:
            keyboard = [[KeyboardButton(f"{emoji} {sign.title()}") for sign, (_, emoji) in row]
                        for row in [list(horoscope_parser.zodiac_signs.items())[i:i + 3] for i in range(0, 12, 3)]]
            keyboard.append([KeyboardButton("◶ Назад")])
            await update.message.reply_text("Выбери знак зодиака:",
                                            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
        else:
            try:
                title, desc, image_path = horoscope_parser.get_tarot()
                logger.info(f"Получено от get_tarot(): title={title}, desc={desc[:50]}..., image_path={image_path}")
                if image_path:
                    logger.debug(f"Попытка открыть файл: {image_path}")
                    with open(image_path, 'rb') as photo:
                        await update.message.reply_photo(
                            photo=photo,
                            caption=f"🎴 *Карта Таро дня*\n\n✨{title}✨\n\n{desc}",
                            parse_mode='Markdown',
                            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                        )
                else:
                    logger.warning(f"Изображение для карты '{title}' не найдено по пути: {image_path}")
                    await update.message.reply_text(
                        f"🎴 *Карта Таро дня*\n\n✨{title}✨\n\n{desc}\n\n(Изображение недоступно)",
                        parse_mode='Markdown',
                        reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                    )

            except Exception as e:
                logger.error(f"Ошибка обработки Таро: {str(e)}", exc_info=True)
                await update.message.reply_text(
                    "❌ Не удалось получить карту Таро. Попробуйте позже.",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                )
        return

    if any(f"{emoji} {sign.title()}" in text for sign, (_, emoji) in horoscope_parser.zodiac_signs.items()):
        if user_id != 7254288870 and not db.has_active_subscription(user_id):
            keyboard = [
                [KeyboardButton("💎 Оформить подписку")],
                [KeyboardButton("◶ Назад")]
            ]
            if db.can_use_trial(user_id):
                keyboard.insert(0, [KeyboardButton("🎁 Активировать пробный период (3 дня)")])
            await update.message.reply_text(
                "⭐️ Эта функция доступна только подписчикам!\nОформи подписку или активируй пробный период:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
            return
        sign = next(
            sign for sign, (_, emoji) in horoscope_parser.zodiac_signs.items() if f"{emoji} {sign.title()}" in text)
        try:
            horoscope = horoscope_parser.get_horoscope(sign)
            if horoscope and "Извините" not in horoscope:
                decorated_horoscope = (
                    f"🌟✨ *Гороскоп для {sign.title()}* ✨🌟\n\n"
                    f"🌙 {horoscope} 🌙\n\n"
                    f"🌈 Удачного дня! 🌞 *{horoscope_parser.zodiac_signs[sign][1]}*"
                )
                await update.message.reply_text(
                    decorated_horoscope,
                    parse_mode='Markdown',
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                )
            else:
                await update.message.reply_text(
                    "❌ Гороскоп не найден. Попробуй позже.",
                    reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
                )
        except Exception as e:
            logger.error(f"Ошибка получения гороскопа для {sign}: {e}")
            await update.message.reply_text(
                "❌ Не удалось получить гороскоп. Попробуйте позже.",
                reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
            )
        return

    if "ЗАПИСАТЬСЯ" in text or "СПРОСИТЬ" in text:
        await update.message.reply_text(
            "Для связи: https://t.me/taro_darinsight",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("◶ Назад")]], resize_keyboard=True)
        )
        return

    if text == "🎁 Активировать пробный период (3 дня)":
        if db.can_use_trial(user_id):
            db.set_subscription(user_id, 3, sub_type='trial')
            await send_subscription_notification(context.bot, user_id, is_trial=True)
            await update.message.reply_text(
                "✨ Пробный период активирован! Теперь вы можете использовать все функции бота 3 дня.",
                reply_markup=get_main_menu(user_id)
            )
        else:
            await update.message.reply_text(
                "❌ Вы уже использовали пробный период. Оформите подписку для продолжения:",
                reply_markup=ReplyKeyboardMarkup([
                    [KeyboardButton("💎 Оформить подписку")],
                    [KeyboardButton("◶ Назад")]
                ], resize_keyboard=True)
            )
        return

    if "💎 7 дней за 159р" in text or "💎 30 дней за 359р" in text:
        plan_id = "week" if "7 дней" in text else "month"
        plan = SUBSCRIPTION_PLANS[plan_id]
        logger.info(f"Инициирую создание платежа: план={plan_id}, user_id={user_id}, сумма={plan['price']} руб.")

        try:
            payment = await ckassa.create_payment(plan['price'], user_id, plan_id)
            logger.debug(f"Результат создания платежа: {payment}")

            if payment and isinstance(payment, dict) and 'paymentUrl' in payment:
                payment_url = payment['paymentUrl']
                logger.info(f"Платеж успешно создан, URL: {payment_url}")
                await update.message.reply_text(
                    f"🌟 Подписка на {plan['period']} за {plan['price']} руб.\n"
                    f"После оплаты вам откроются:\n"
                    f"• Гороскоп\n• Таро\n• Предсказания",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("💳 Оформить подписку", url=payment_url)]
                    ])
                )
                await update.message.reply_text(
                    "После оплаты нажмите 'Назад' для проверки статуса и подождите 5 минут что бы оплата прошла⌛",
                    reply_markup=ReplyKeyboardMarkup(
                        [[KeyboardButton("◶ Назад")]],
                        resize_keyboard=True
                    )
                )
                context.user_data['payment_url'] = payment_url
                context.user_data['pending_plan'] = plan_id
            else:
                logger.error(f"Ошибка создания платежа: payment={payment}")
                await update.message.reply_text(
                    "❌ Не удалось создать платеж. Проверьте логи или свяжитесь с @taro_darinsight.",
                    reply_markup=ReplyKeyboardMarkup(
                        [[KeyboardButton("◶ Назад")]],
                        resize_keyboard=True
                    )
                )
        except Exception as e:
            logger.error(f"Исключение при создании платежа: {str(e)}")
            await update.message.reply_text(
                f"❌ Ошибка: {str(e)}. Попробуйте позже.",
                reply_markup=ReplyKeyboardMarkup(
                    [[KeyboardButton("◶ Назад")]],
                        resize_keyboard=True
                    )
                )
        return

    if "Подписка" in text or "💎" in text:
        keyboard = []
        if db.can_use_trial(user_id):
            keyboard.append([KeyboardButton("🎁 Активировать пробный период (3 дня)")])
        keyboard.extend([
            [KeyboardButton(f"💎 7 дней за 159р ({SUBSCRIPTION_PLANS['week']['per_day']})")],
            [KeyboardButton(f"💎 30 дней за 359р ({SUBSCRIPTION_PLANS['month']['per_day']})")],
            [KeyboardButton("◶ Назад")]
        ])
        await update.message.reply_text(
            "✨ Подписка открывает:\n• Гороскоп\n• Таро\n• Предсказания\nВыбери план:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    if query.data == "check_sub":
        user_id = query.from_user.id
        if await check_channel_subscription(user_id, context):
            await query.edit_message_text("✅ Подписка подтверждена!")
            db.add_user(user_id, query.from_user.username)
            await context.bot.send_message(
                chat_id=user_id,
                text=msg_manager.get("start_message", "Привет! Я твой помощник по Таро и психологии."),
                reply_markup=get_main_menu(user_id)
            )
        else:
            query.edit_message_text("❌ Подпишись на все каналы и проверь снова.")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Ошибка: {context.error}", exc_info=True)

async def run_webserver(app: Application) -> None:
    web_app = web.Application()
    web_app.router.add_post('/callback', handle_callback)
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("Веб-сервер для callback запущен на порту 8080")
    while True:
        await asyncio.sleep(3600)

async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN not found in .env")
        sys.exit(1)

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("admin", admin_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_error_handler(error_handler)

    async def post_init(application: Application) -> None:
        asyncio.create_task(run_webserver(application))

    app.post_init = post_init

    try:
        webhook_info = await app.bot.get_webhook_info()
        if webhook_info.url:
            logger.warning("Webhook is set, deleting it to use polling")
            await app.bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Error checking webhook: {e}")

    for attempt in range(3):
        try:
            await app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
            break
        except Conflict as ce:
            logger.error(f"Conflict detected during polling, attempt {attempt + 1}: {ce}")
            await app.bot.delete_webhook(drop_pending_updates=True)
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"Error in main loop: {e}", exc_info=True)
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
            else:
                logger.error("Failed to start polling after 3 attempts")
                sys.exit(1)
    else:
        logger.error("Polling failed after maximum attempts")
        sys.exit(1)

if __name__ == "__main__":
    import signal
    import sys

    def signal_handler(sig, frame):
        logger.info("Received termination signal, stopping bot")
        if 'app' in globals():
            asyncio.run(app.stop())
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    asyncio.run(main())