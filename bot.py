import os
import random
import sqlite3
from math import ceil
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import discord
from discord.ext import commands
from discord.errors import LoginFailure, PrivilegedIntentsRequired


MOSCOW_TZ = timezone(timedelta(hours=3))
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "city_economy.db"
TOKEN_PATH = BASE_DIR / "discord token.txt"
ENV_PATH = BASE_DIR / ".env"


def load_env_file() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


load_env_file()

CURRENCY_NAME = os.getenv("CITY_CURRENCY_NAME", "Иридиум")
CURRENCY_SHORT = os.getenv("CITY_CURRENCY_SHORT", "ИР")
BOT_PREFIX = os.getenv("BOT_PREFIX", "!")
WITHDRAW_RATE = float(os.getenv("WITHDRAW_RATE", "10.0"))
WORK_COOLDOWN_MINUTES = int(os.getenv("WORK_COOLDOWN_MINUTES", "60"))
TASK_COOLDOWN_MINUTES = int(os.getenv("TASK_COOLDOWN_MINUTES", "30"))
IRIDIUM_TO_RUB = float(os.getenv("IRIDIUM_TO_RUB", str(WITHDRAW_RATE)))
WITHDRAW_MIN_IRIDIUM = int(os.getenv("WITHDRAW_MIN_IRIDIUM", "35"))
WITHDRAW_COMMISSION_PERCENT = float(os.getenv("WITHDRAW_COMMISSION_PERCENT", "7"))
CONTROL_CHANNEL_ID = os.getenv("CONTROL_CHANNEL_ID", "").strip()
SEND_CONTROL_PANEL_ON_READY = os.getenv("SEND_CONTROL_PANEL_ON_READY", "true").lower() == "true"
CONTROL_PANEL_TITLE = "Панель экономики"
ADMIN_PANEL_TITLE = "Панель администрации"


def now_moscow() -> datetime:
    return datetime.now(MOSCOW_TZ)


def format_money(amount: int) -> str:
    return f"{amount:,}".replace(",", " ")


def parse_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_amount(value: str) -> Optional[float]:
    try:
        amount = float(value.replace(",", "."))
    except (AttributeError, ValueError):
        return None
    if amount <= 0:
        return None
    return amount


def format_number(amount: float) -> str:
    if amount == int(amount):
        return format_money(int(amount))
    return f"{amount:,.2f}".replace(",", " ").replace(".00", "").replace(".", ",")


def rub_to_iridium(rub_amount: float) -> float:
    return rub_amount / IRIDIUM_TO_RUB


def iridium_to_rub(iridium_amount: float) -> float:
    return iridium_amount * IRIDIUM_TO_RUB


def normalize_currency(value: str) -> Optional[str]:
    normalized = value.strip().lower().replace(".", "")
    rub_aliases = {"руб", "рубль", "рубли", "рублей", "rub", "rubles", "ruble", "₽"}
    iridium_aliases = {"ир", "иридий", "иридиум", "иридиума", "иридиумы", "ir", "iridium"}
    if normalized in rub_aliases:
        return "rub"
    if normalized in iridium_aliases:
        return "iridium"
    return None


OLD_DEFAULT_TASK_TITLES = {
    "Уборка парка",
    "Раздача листовок",
    "Ремонт остановки",
    "Помощь библиотеке",
    "Субботник",
}

DEFAULT_SHOP_ITEMS = [
    ("Спонсорка", "Покупка спонсорки на сервере.", ceil(300 / IRIDIUM_TO_RUB), True),
    ("Смена аккаунта", "Смена привязанного аккаунта.", ceil(119 / IRIDIUM_TO_RUB), True),
    ("Кейс: 1 значок", "Кейс с одним значком.", ceil(19 / IRIDIUM_TO_RUB), True),
    ("Кейс: 5 значков", "Кейс с пятью значками.", ceil(79 / IRIDIUM_TO_RUB), True),
    ("Кейс: 10 значков", "Кейс с десятью значками.", ceil(149 / IRIDIUM_TO_RUB), True),
    ("Кейс: 25 значков", "Кейс с двадцатью пятью значками.", ceil(329 / IRIDIUM_TO_RUB), True),
    ("Кейс: 100 значков", "Кейс со ста значками.", ceil(949 / IRIDIUM_TO_RUB), True),
]
OLD_DEFAULT_SHOP_NAMES = {
    "Билет на концерт",
    "Кофе жителя",
    "Редкий лутбокс",
    "Премиум роль",
    "Стикерпак",
}

WORK_MESSAGES = [
    "Сегодня ты помог коммунальщикам и получил {reward} {short}.",
    "Ты организовал порядок на городской площади и заработал {reward} {short}.",
    "Твои старания пошли на благо города. Награда: {reward} {short}.",
    "Ты помог жителям с городским проектом и получил {reward} {short}.",
]

TASK_FINISH_MESSAGES = [
    "Задание выполнено, а город стал немного лучше.",
    "Отличная работа. Администрация города довольна.",
    "Жители оценили твой вклад. Заслуженная награда выдана.",
    "Ты закрыл задачу без нареканий. Деньги зачислены.",
]

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix=BOT_PREFIX, intents=intents, help_command=None)
PANEL_VIEWS_REGISTERED = False


@bot.check
async def restrict_commands_to_control_channel(ctx: commands.Context) -> bool:
    channel_id = get_control_channel_id()
    if not channel_id or ctx.guild is None:
        return True
    if isinstance(ctx.author, discord.Member) and is_admin(ctx.author):
        return True
    if getattr(ctx.channel, "id", None) == channel_id:
        return True
    try:
        await ctx.message.delete()
    except discord.DiscordException:
        pass
    try:
        channel_mention = f"<#{channel_id}>"
        await ctx.send(f"{ctx.author.mention}, команды перенесены в панель: {channel_mention}.", delete_after=8)
    except discord.DiscordException:
        pass
    return False


def get_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    return connection


def init_db() -> None:
    with closing(get_connection()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                total_earned INTEGER NOT NULL DEFAULT 0,
                total_spent INTEGER NOT NULL DEFAULT 0,
                total_withdrawn INTEGER NOT NULL DEFAULT 0,
                last_work_at TEXT,
                last_task_at TEXT
            );

            CREATE TABLE IF NOT EXISTS shop_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                price INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS inventory (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, item_id),
                FOREIGN KEY (item_id) REFERENCES shop_items(id)
            );

            CREATE TABLE IF NOT EXISTS task_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                reward_min INTEGER NOT NULL,
                reward_max INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS task_assignments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                template_id INTEGER NOT NULL,
                accepted_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                reward INTEGER NOT NULL,
                FOREIGN KEY (template_id) REFERENCES task_templates(id)
            );

            CREATE TABLE IF NOT EXISTS completed_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                assignment_id INTEGER NOT NULL UNIQUE,
                user_id INTEGER NOT NULL,
                template_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                reward INTEGER NOT NULL,
                accepted_at TEXT NOT NULL,
                completed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS withdrawals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                currency_amount INTEGER NOT NULL,
                rub_amount REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TEXT NOT NULL,
                reviewed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                amount INTEGER NOT NULL,
                details TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.commit()

        shop_count = conn.execute("SELECT COUNT(*) FROM shop_items").fetchone()[0]
        if shop_count == 0:
            conn.executemany(
                "INSERT INTO shop_items(name, description, price, is_active) VALUES (?, ?, ?, ?)",
                DEFAULT_SHOP_ITEMS,
            )
        else:
            active_names = {
                row["name"]
                for row in conn.execute("SELECT name FROM shop_items WHERE is_active = 1").fetchall()
            }
            new_default_names = {item[0] for item in DEFAULT_SHOP_ITEMS}
            if active_names and active_names.issubset(OLD_DEFAULT_SHOP_NAMES) and not active_names & new_default_names:
                conn.execute("UPDATE shop_items SET is_active = 0 WHERE is_active = 1")
                conn.executemany(
                    "INSERT INTO shop_items(name, description, price, is_active) VALUES (?, ?, ?, ?)",
                    DEFAULT_SHOP_ITEMS,
                )

        conn.executemany(
            "UPDATE task_templates SET is_active = 0 WHERE title = ?",
            [(title,) for title in OLD_DEFAULT_TASK_TITLES],
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO completed_tasks(
                assignment_id, user_id, template_id, title, description, reward, accepted_at, completed_at
            )
            SELECT ta.id, ta.user_id, ta.template_id, tt.title, tt.description, ta.reward, ta.accepted_at, ta.accepted_at
            FROM task_assignments ta
            JOIN task_templates tt ON tt.id = ta.template_id
            WHERE ta.status = 'completed'
            """
        )
        conn.commit()


def ensure_user(conn: sqlite3.Connection, user_id: int) -> sqlite3.Row:
    conn.execute("INSERT OR IGNORE INTO users(user_id) VALUES (?)", (user_id,))
    conn.commit()
    return conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()


def add_transaction(conn: sqlite3.Connection, user_id: int, kind: str, amount: int, details: str) -> None:
    conn.execute(
        "INSERT INTO transactions(user_id, kind, amount, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, kind, amount, details, now_moscow().isoformat()),
    )


def archive_completed_task(conn: sqlite3.Connection, assignment: sqlite3.Row, completed_at: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO completed_tasks(
            assignment_id, user_id, template_id, title, description, reward, accepted_at, completed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            assignment["id"],
            assignment["user_id"],
            assignment["template_id"],
            assignment["title"],
            assignment["description"],
            assignment["reward"],
            assignment["accepted_at"],
            completed_at,
        ),
    )


def get_remaining_cooldown(last_time_value: Optional[str], cooldown_minutes: int) -> Optional[timedelta]:
    if not last_time_value:
        return None
    last_time = datetime.fromisoformat(last_time_value)
    remaining = last_time + timedelta(minutes=cooldown_minutes) - now_moscow()
    if remaining.total_seconds() > 0:
        return remaining
    return None


def format_timedelta(delta: timedelta) -> str:
    total_seconds = max(int(delta.total_seconds()), 0)
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    parts = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes:
        parts.append(f"{minutes} мин")
    if seconds and not hours:
        parts.append(f"{seconds} сек")
    return " ".join(parts) if parts else "0 сек"


def is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


def get_control_channel_id() -> Optional[int]:
    if not CONTROL_CHANNEL_ID:
        return None
    return parse_int(CONTROL_CHANNEL_ID)


def build_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title=CONTROL_PANEL_TITLE,
        description=(
            f"Валюта сервера: **{CURRENCY_NAME} ({CURRENCY_SHORT})**.\n"
            "Пользуйся кнопками ниже: баланс, работа, магазин, конвертор и вывод открываются прямо из меню."
        ),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Курс",
        value=f"1 {CURRENCY_SHORT} = {IRIDIUM_TO_RUB:.2f} RUB",
        inline=False,
    )
    embed.set_footer(text="Ответы по личным операциям видны только тому, кто нажал кнопку.")
    return embed


def build_admin_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title=ADMIN_PANEL_TITLE,
        description="Кнопки для заявок на вывод и управления товарами. Видимость действий проверяется по правам администратора.",
        color=discord.Color.dark_teal(),
    )
    return embed


def build_balance_message(member: discord.Member, user: sqlite3.Row) -> str:
    return (
        f"Баланс {member.mention}: **{format_money(user['balance'])} {CURRENCY_SHORT}**\n"
        f"Заработано: {format_money(user['total_earned'])} {CURRENCY_SHORT}\n"
        f"Потрачено: {format_money(user['total_spent'])} {CURRENCY_SHORT}\n"
        f"Выведено: {format_money(user['total_withdrawn'])} {CURRENCY_SHORT}"
    )


def build_rate_message() -> str:
    commission_multiplier = 1 - WITHDRAW_COMMISSION_PERCENT / 100
    min_rub = iridium_to_rub(WITHDRAW_MIN_IRIDIUM)
    min_rub_after_commission = round(min_rub * commission_multiplier, 2)
    return (
        f"Курс: **1 {CURRENCY_NAME} = {IRIDIUM_TO_RUB:.2f} RUB**.\n"
        f"Минимальный вывод: **{WITHDRAW_MIN_IRIDIUM} {CURRENCY_SHORT}** ({min_rub:.2f} RUB).\n"
        f"Комиссия вывода: **{WITHDRAW_COMMISSION_PERCENT:g}%**, на руки с минимума: "
        f"**{min_rub_after_commission:.2f} RUB**."
    )


def convert_text(amount_text: str, source_currency: str) -> str:
    parsed_amount = parse_amount(amount_text)
    source = normalize_currency(source_currency)
    if parsed_amount is None or source is None:
        return "Проверь сумму и валюту. Валюта может быть: руб или иридиум."

    if source == "iridium":
        rub_amount = iridium_to_rub(parsed_amount)
        return f"**{format_number(parsed_amount)} {CURRENCY_SHORT}** = **{format_number(rub_amount)} RUB**."

    iridium_exact = rub_to_iridium(parsed_amount)
    iridium_whole = ceil(iridium_exact)
    return (
        f"**{format_number(parsed_amount)} RUB** = **{format_number(iridium_exact)} {CURRENCY_SHORT}**.\n"
        f"Если продавать только целые {CURRENCY_SHORT}, к оплате: **{format_money(iridium_whole)} {CURRENCY_SHORT}**."
    )


def build_tasks_embed(user_id: int) -> discord.Embed:
    with closing(get_connection()) as conn:
        active = conn.execute(
            """
            SELECT ta.id, tt.title, tt.description, ta.reward
            FROM task_assignments ta
            JOIN task_templates tt ON tt.id = ta.template_id
            WHERE ta.user_id = ? AND ta.status = 'active'
            ORDER BY ta.accepted_at DESC
            """,
            (user_id,),
        ).fetchall()
        available = conn.execute(
            """
            SELECT id, title, description, reward_min, reward_max
            FROM task_templates
            WHERE is_active = 1
            ORDER BY id ASC
            """,
        ).fetchall()

    embed = discord.Embed(title="Городские задания", color=discord.Color.green())
    active_lines = [
        f"`{row['id']}` • **{row['title']}** • награда {format_money(row['reward'])} {CURRENCY_SHORT}\n{row['description']}"
        for row in active
    ]
    available_lines = [
        f"`{row['id']}` • **{row['title']}** • {format_money(row['reward_min'])}-{format_money(row['reward_max'])} {CURRENCY_SHORT}\n{row['description']}"
        for row in available
    ]
    embed.add_field(
        name="У тебя в работе",
        value="\n\n".join(active_lines) if active_lines else "Активных заданий пока нет.",
        inline=False,
    )
    embed.add_field(
        name="Доступные шаблоны",
        value="\n\n".join(available_lines) if available_lines else "Заданий сейчас нет.",
        inline=False,
    )
    return embed


def build_shop_embed() -> discord.Embed:
    with closing(get_connection()) as conn:
        items = conn.execute(
            "SELECT id, name, description, price FROM shop_items WHERE is_active = 1 ORDER BY price ASC"
        ).fetchall()

    embed = discord.Embed(
        title="Магазин",
        description=f"Покупка списывает {CURRENCY_SHORT} с баланса.",
        color=discord.Color.gold(),
    )
    if not items:
        embed.description = "Магазин пока пуст."
        return embed

    for item in items:
        rub_price = iridium_to_rub(item["price"])
        embed.add_field(
            name=f"{item['id']}. {item['name']} — {format_money(item['price'])} {CURRENCY_SHORT} ({rub_price:.0f} RUB)",
            value=item["description"],
            inline=False,
        )
    return embed


def build_inventory_embed(user_id: int, display_name: str) -> discord.Embed:
    with closing(get_connection()) as conn:
        ensure_user(conn, user_id)
        items = conn.execute(
            """
            SELECT si.name, si.description, i.quantity
            FROM inventory i
            JOIN shop_items si ON si.id = i.item_id
            WHERE i.user_id = ? AND i.quantity > 0
            ORDER BY si.name ASC
            """,
            (user_id,),
        ).fetchall()

    embed = discord.Embed(title=f"Инвентарь {display_name}", color=discord.Color.purple())
    if not items:
        embed.description = "Инвентарь пока пуст."
        return embed
    for item in items:
        embed.add_field(name=f"{item['name']} x{item['quantity']}", value=item["description"], inline=False)
    return embed


def build_completed_tasks_message(limit: int = 15) -> str:
    with closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT id, assignment_id, user_id, title, reward, completed_at
            FROM completed_tasks
            ORDER BY completed_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    if not rows:
        return "Выполненных заданий в хранилище пока нет."

    lines = [
        f"#{row['id']} • задание `{row['assignment_id']}` • user `{row['user_id']}` • "
        f"**{row['title']}** • {format_money(row['reward'])} {CURRENCY_SHORT} • {row['completed_at'][:16]}"
        for row in rows
    ]
    return "Последние выполненные задания:\n" + "\n".join(lines)


def do_work(user_id: int) -> str:
    with closing(get_connection()) as conn:
        user = ensure_user(conn, user_id)
        cooldown = get_remaining_cooldown(user["last_work_at"], WORK_COOLDOWN_MINUTES)
        if cooldown:
            return f"До следующей работы осталось: **{format_timedelta(cooldown)}**."

        reward = random.randint(20, 70)
        new_balance = user["balance"] + reward
        conn.execute(
            """
            UPDATE users
            SET balance = ?, total_earned = total_earned + ?, last_work_at = ?
            WHERE user_id = ?
            """,
            (new_balance, reward, now_moscow().isoformat(), user_id),
        )
        add_transaction(conn, user_id, "work", reward, "Городская работа")
        conn.commit()

    message = random.choice(WORK_MESSAGES).format(reward=format_money(reward), short=CURRENCY_SHORT)
    return f"{message}\nНовый баланс: **{format_money(new_balance)} {CURRENCY_SHORT}**"


def accept_task(user_id: int, task_id: int) -> str:
    with closing(get_connection()) as conn:
        user = ensure_user(conn, user_id)
        cooldown = get_remaining_cooldown(user["last_task_at"], TASK_COOLDOWN_MINUTES)
        if cooldown:
            return f"Новое задание можно взять через **{format_timedelta(cooldown)}**."

        template = conn.execute(
            """
            SELECT id, title, description, reward_min, reward_max
            FROM task_templates
            WHERE id = ? AND is_active = 1
            """,
            (task_id,),
        ).fetchone()
        if not template:
            return "Такого активного задания нет."

        reward = random.randint(template["reward_min"], template["reward_max"])
        cursor = conn.execute(
            """
            INSERT INTO task_assignments(user_id, template_id, accepted_at, reward)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, task_id, now_moscow().isoformat(), reward),
        )
        conn.execute("UPDATE users SET last_task_at = ? WHERE user_id = ?", (now_moscow().isoformat(), user_id))
        conn.commit()

    return (
        f"Задание принято.\n"
        f"Номер задания: **{cursor.lastrowid}**\n"
        f"**{template['title']}**\n{template['description']}\n"
        f"Награда после сдачи: **{format_money(reward)} {CURRENCY_SHORT}**"
    )


def finish_task(user_id: int, assignment_id: int) -> str:
    with closing(get_connection()) as conn:
        user = ensure_user(conn, user_id)
        assignment = conn.execute(
            """
            SELECT ta.id, ta.user_id, ta.template_id, ta.accepted_at, ta.reward, ta.status, tt.title, tt.description
            FROM task_assignments ta
            JOIN task_templates tt ON tt.id = ta.template_id
            WHERE ta.id = ? AND ta.user_id = ?
            """,
            (assignment_id, user_id),
        ).fetchone()
        if not assignment:
            return "Это задание не найдено."
        if assignment["status"] != "active":
            return "Это задание уже закрыто."

        new_balance = user["balance"] + assignment["reward"]
        completed_at = now_moscow().isoformat()
        conn.execute("UPDATE task_assignments SET status = 'completed' WHERE id = ?", (assignment_id,))
        archive_completed_task(conn, assignment, completed_at)
        conn.execute(
            "UPDATE users SET balance = ?, total_earned = total_earned + ? WHERE user_id = ?",
            (new_balance, assignment["reward"], user_id),
        )
        add_transaction(conn, user_id, "task", assignment["reward"], assignment["title"])
        conn.commit()

    text = random.choice(TASK_FINISH_MESSAGES)
    return (
        f"{text}\n"
        f"Получено: **{format_money(assignment['reward'])} {CURRENCY_SHORT}**\n"
        f"Баланс: **{format_money(new_balance)} {CURRENCY_SHORT}**"
    )


def buy_item(user_id: int, item_id: int, quantity: int) -> str:
    if quantity <= 0:
        return "Количество должно быть больше нуля."

    with closing(get_connection()) as conn:
        user = ensure_user(conn, user_id)
        item = conn.execute(
            "SELECT id, name, price FROM shop_items WHERE id = ? AND is_active = 1",
            (item_id,),
        ).fetchone()
        if not item:
            return "Такого товара нет."

        total_cost = item["price"] * quantity
        if user["balance"] < total_cost:
            return (
                f"Не хватает средств. Нужно **{format_money(total_cost)} {CURRENCY_SHORT}**, "
                f"а у тебя **{format_money(user['balance'])} {CURRENCY_SHORT}**."
            )

        conn.execute(
            """
            INSERT INTO inventory(user_id, item_id, quantity)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, item_id)
            DO UPDATE SET quantity = quantity + excluded.quantity
            """,
            (user_id, item["id"], quantity),
        )
        conn.execute(
            """
            UPDATE users
            SET balance = balance - ?, total_spent = total_spent + ?
            WHERE user_id = ?
            """,
            (total_cost, total_cost, user_id),
        )
        add_transaction(conn, user_id, "purchase", -total_cost, item["name"])
        conn.commit()

    return f"Покупка прошла успешно: **{item['name']} x{quantity}**.\nПотрачено: **{format_money(total_cost)} {CURRENCY_SHORT}**"


def request_withdrawal(user_id: int, amount: int) -> str:
    if amount <= 0:
        return "Укажи корректную сумму для вывода."
    if amount < WITHDRAW_MIN_IRIDIUM:
        return f"Минимальный вывод: **{WITHDRAW_MIN_IRIDIUM} {CURRENCY_SHORT}**."

    with closing(get_connection()) as conn:
        user = ensure_user(conn, user_id)
        if user["balance"] < amount:
            return "На балансе недостаточно средств для вывода."

        rub_before_commission = iridium_to_rub(amount)
        rub_amount = round(rub_before_commission * (1 - WITHDRAW_COMMISSION_PERCENT / 100), 2)
        conn.execute(
            """
            UPDATE users
            SET balance = balance - ?, total_withdrawn = total_withdrawn + ?
            WHERE user_id = ?
            """,
            (amount, amount, user_id),
        )
        cursor = conn.execute(
            """
            INSERT INTO withdrawals(user_id, currency_amount, rub_amount, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (user_id, amount, rub_amount, now_moscow().isoformat()),
        )
        add_transaction(conn, user_id, "withdraw_request", -amount, f"Заявка #{cursor.lastrowid}")
        conn.commit()

    return (
        f"Заявка на вывод создана.\n"
        f"Заявка: **#{cursor.lastrowid}**\n"
        f"Списано: **{format_money(amount)} {CURRENCY_SHORT}**\n"
        f"До комиссии: **{rub_before_commission:.2f} RUB**\n"
        f"Комиссия: **{WITHDRAW_COMMISSION_PERCENT:g}%**\n"
        f"К выплате: **{rub_amount:.2f} RUB**"
    )


async def send_or_update_control_panel(channel: discord.abc.Messageable) -> None:
    view = ControlPanelView()
    admin_view = AdminPanelView()
    panel_message = None
    admin_message = None
    if isinstance(channel, discord.TextChannel):
        async for message in channel.history(limit=25):
            if message.author == bot.user and message.embeds and message.embeds[0].title == CONTROL_PANEL_TITLE:
                panel_message = message
            if message.author == bot.user and message.embeds and message.embeds[0].title == ADMIN_PANEL_TITLE:
                admin_message = message

    if panel_message:
        await panel_message.edit(embed=build_panel_embed(), view=view)
    else:
        await channel.send(embed=build_panel_embed(), view=view)

    if admin_message:
        await admin_message.edit(embed=build_admin_panel_embed(), view=admin_view)
    else:
        await channel.send(embed=build_admin_panel_embed(), view=admin_view)


async def defer_ephemeral(interaction: discord.Interaction) -> bool:
    if interaction.response.is_done():
        return True
    try:
        await interaction.response.defer(ephemeral=True, thinking=True)
        return True
    except discord.NotFound:
        return False
    except discord.HTTPException as error:
        return getattr(error, "code", None) == 40060


async def send_ephemeral(interaction: discord.Interaction, content: Optional[str] = None, **kwargs) -> None:
    kwargs.setdefault("ephemeral", True)
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, **kwargs)
        else:
            await interaction.response.send_message(content, **kwargs)
    except discord.NotFound:
        return
    except discord.HTTPException as error:
        if getattr(error, "code", None) != 40060:
            raise


async def send_modal_safely(interaction: discord.Interaction, modal: discord.ui.Modal) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send("Нажми кнопку еще раз: Discord уже закрыл это нажатие.", ephemeral=True)
            return
        await interaction.response.send_modal(modal)
    except discord.NotFound:
        return
    except discord.HTTPException as error:
        if getattr(error, "code", None) == 40060:
            await send_ephemeral(interaction, "Нажми кнопку еще раз: Discord уже обработал это нажатие.")
            return
        raise


class ConverterModal(discord.ui.Modal, title="Конвертор"):
    amount = discord.ui.TextInput(label="Сумма", placeholder="Например: 64", max_length=20)
    currency = discord.ui.TextInput(label="Валюта", placeholder="руб или иридиум", max_length=20)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(interaction, convert_text(str(self.amount), str(self.currency)))


class WithdrawModal(discord.ui.Modal, title="Вывод"):
    amount = discord.ui.TextInput(label=f"Сумма в {CURRENCY_SHORT}", placeholder=str(WITHDRAW_MIN_IRIDIUM), max_length=20)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        parsed_amount = parse_int(str(self.amount))
        if not parsed_amount:
            await send_ephemeral(interaction, "Укажи целую сумму для вывода.")
            return
        await send_ephemeral(interaction, request_withdrawal(interaction.user.id, parsed_amount))


class BuyItemModal(discord.ui.Modal, title="Покупка"):
    quantity = discord.ui.TextInput(label="Количество", default="1", max_length=8)

    def __init__(self, item_id: int) -> None:
        super().__init__()
        self.item_id = item_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        parsed_quantity = parse_int(str(self.quantity))
        if not parsed_quantity:
            await send_ephemeral(interaction, "Укажи целое количество.")
            return
        await send_ephemeral(interaction, buy_item(interaction.user.id, self.item_id, parsed_quantity))


class ShopSelect(discord.ui.Select):
    def __init__(self) -> None:
        with closing(get_connection()) as conn:
            items = conn.execute(
                "SELECT id, name, price FROM shop_items WHERE is_active = 1 ORDER BY price ASC LIMIT 25"
            ).fetchall()
        options = [
            discord.SelectOption(
                label=item["name"][:100],
                value=str(item["id"]),
                description=f"{format_money(item['price'])} {CURRENCY_SHORT}"[:100],
            )
            for item in items
        ]
        if not options:
            options = [discord.SelectOption(label="Магазин пуст", value="none")]
        super().__init__(placeholder="Выбери товар", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.values[0] == "none":
            await send_ephemeral(interaction, "Магазин пока пуст.")
            return
        await send_modal_safely(interaction, BuyItemModal(parse_int(self.values[0]) or 0))


class ShopView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=180)
        self.add_item(ShopSelect())


class TaskSelect(discord.ui.Select):
    def __init__(self, user_id: int, mode: str) -> None:
        self.mode = mode
        with closing(get_connection()) as conn:
            if mode == "accept":
                rows = conn.execute(
                    "SELECT id, title, reward_min, reward_max FROM task_templates WHERE is_active = 1 ORDER BY id ASC LIMIT 25"
                ).fetchall()
                options = [
                    discord.SelectOption(
                        label=row["title"][:100],
                        value=str(row["id"]),
                        description=f"{format_money(row['reward_min'])}-{format_money(row['reward_max'])} {CURRENCY_SHORT}"[:100],
                    )
                    for row in rows
                ]
                placeholder = "Взять задание"
            else:
                rows = conn.execute(
                    """
                    SELECT ta.id, tt.title, ta.reward
                    FROM task_assignments ta
                    JOIN task_templates tt ON tt.id = ta.template_id
                    WHERE ta.user_id = ? AND ta.status = 'active'
                    ORDER BY ta.accepted_at DESC
                    LIMIT 25
                    """,
                    (user_id,),
                ).fetchall()
                options = [
                    discord.SelectOption(
                        label=row["title"][:100],
                        value=str(row["id"]),
                        description=f"{format_money(row['reward'])} {CURRENCY_SHORT}"[:100],
                    )
                    for row in rows
                ]
                placeholder = "Сдать задание"
        if not options:
            options = [discord.SelectOption(label="Нет доступных вариантов", value="none")]
        super().__init__(placeholder=placeholder, min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction) -> None:
        if self.values[0] == "none":
            await send_ephemeral(interaction, "Сейчас тут нет доступных вариантов.")
            return
        if not await defer_ephemeral(interaction):
            return
        selected_id = parse_int(self.values[0]) or 0
        if self.mode == "accept":
            await send_ephemeral(interaction, accept_task(interaction.user.id, selected_id))
        else:
            await send_ephemeral(interaction, finish_task(interaction.user.id, selected_id))


class TasksView(discord.ui.View):
    def __init__(self, user_id: int) -> None:
        super().__init__(timeout=180)
        self.add_item(TaskSelect(user_id, "accept"))
        self.add_item(TaskSelect(user_id, "finish"))


class ControlPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(label="Баланс", style=discord.ButtonStyle.primary, custom_id="economy:balance")
    async def balance_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        with closing(get_connection()) as conn:
            user = ensure_user(conn, interaction.user.id)
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        await send_ephemeral(interaction, build_balance_message(member or interaction.user, user))

    @discord.ui.button(label="Работа", style=discord.ButtonStyle.success, custom_id="economy:work")
    async def work_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(interaction, do_work(interaction.user.id))

    @discord.ui.button(label="Задания", style=discord.ButtonStyle.secondary, custom_id="economy:tasks")
    async def tasks_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(
            interaction,
            embed=build_tasks_embed(interaction.user.id),
            view=TasksView(interaction.user.id),
        )

    @discord.ui.button(label="Магазин", style=discord.ButtonStyle.secondary, custom_id="economy:shop")
    async def shop_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(interaction, embed=build_shop_embed(), view=ShopView())

    @discord.ui.button(label="Инвентарь", style=discord.ButtonStyle.secondary, custom_id="economy:inventory")
    async def inventory_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(
            interaction,
            embed=build_inventory_embed(interaction.user.id, interaction.user.display_name),
        )

    @discord.ui.button(label="Курс", style=discord.ButtonStyle.secondary, custom_id="economy:rate")
    async def rate_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(interaction, build_rate_message())

    @discord.ui.button(label="Конвертор", style=discord.ButtonStyle.secondary, custom_id="economy:converter")
    async def converter_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, ConverterModal())

    @discord.ui.button(label="Вывод", style=discord.ButtonStyle.danger, custom_id="economy:withdraw")
    async def withdraw_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, WithdrawModal())


class AdminWithdrawalModal(discord.ui.Modal, title="Обработка вывода"):
    withdrawal_id = discord.ui.TextInput(label="ID заявки", placeholder="Например: 1", max_length=12)

    def __init__(self, approve: bool) -> None:
        super().__init__()
        self.approve = approve

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        if not isinstance(interaction.user, discord.Member) or not is_admin(interaction.user):
            await send_ephemeral(interaction, "Эта кнопка доступна только администрации.")
            return
        parsed_id = parse_int(str(self.withdrawal_id))
        if not parsed_id:
            await send_ephemeral(interaction, "Укажи корректный id заявки.")
            return
        _, message = _review_withdrawal(parsed_id, self.approve)
        await send_ephemeral(interaction, message)


class AdminAddItemModal(discord.ui.Modal, title="Добавить товар"):
    price = discord.ui.TextInput(label=f"Цена в {CURRENCY_SHORT}", max_length=10)
    name = discord.ui.TextInput(label="Название", max_length=80)
    description = discord.ui.TextInput(label="Описание", style=discord.TextStyle.paragraph, max_length=300)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        if not isinstance(interaction.user, discord.Member) or not is_admin(interaction.user):
            await send_ephemeral(interaction, "Эта кнопка доступна только администрации.")
            return
        parsed_price = parse_int(str(self.price))
        if not parsed_price or parsed_price <= 0:
            await send_ephemeral(interaction, "Цена должна быть положительным числом.")
            return
        with closing(get_connection()) as conn:
            cursor = conn.execute(
                "INSERT INTO shop_items(name, description, price, is_active) VALUES (?, ?, ?, 1)",
                (str(self.name), str(self.description), parsed_price),
            )
            conn.commit()
        await send_ephemeral(interaction, f"Товар добавлен. ID: **{cursor.lastrowid}**")


class AdminRemoveItemModal(discord.ui.Modal, title="Удалить товар"):
    item_id = discord.ui.TextInput(label="ID товара", max_length=12)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        if not isinstance(interaction.user, discord.Member) or not is_admin(interaction.user):
            await send_ephemeral(interaction, "Эта кнопка доступна только администрации.")
            return
        parsed_id = parse_int(str(self.item_id))
        if not parsed_id:
            await send_ephemeral(interaction, "Укажи корректный id товара.")
            return
        with closing(get_connection()) as conn:
            result = conn.execute("UPDATE shop_items SET is_active = 0 WHERE id = ?", (parsed_id,))
            conn.commit()
        await send_ephemeral(interaction, "Товар скрыт из магазина." if result.rowcount else "Товар не найден.")


class AdminAddTaskModal(discord.ui.Modal, title="Добавить задание"):
    reward_min = discord.ui.TextInput(label=f"Мин. награда в {CURRENCY_SHORT}", max_length=10)
    reward_max = discord.ui.TextInput(label=f"Макс. награда в {CURRENCY_SHORT}", max_length=10)
    title_text = discord.ui.TextInput(label="Название", max_length=80)
    description = discord.ui.TextInput(label="Описание", style=discord.TextStyle.paragraph, max_length=300)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        if not isinstance(interaction.user, discord.Member) or not is_admin(interaction.user):
            await send_ephemeral(interaction, "Эта кнопка доступна только администрации.")
            return
        parsed_min = parse_int(str(self.reward_min))
        parsed_max = parse_int(str(self.reward_max))
        if not parsed_min or not parsed_max or parsed_min <= 0 or parsed_max < parsed_min:
            await send_ephemeral(interaction, "Проверь диапазон награды.")
            return
        with closing(get_connection()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO task_templates(title, description, reward_min, reward_max, is_active)
                VALUES (?, ?, ?, ?, 1)
                """,
                (str(self.title_text), str(self.description), parsed_min, parsed_max),
            )
            conn.commit()
        await send_ephemeral(interaction, f"Задание добавлено. ID: **{cursor.lastrowid}**")


class AdminRemoveTaskModal(discord.ui.Modal, title="Удалить задание"):
    task_id = discord.ui.TextInput(label="ID задания", max_length=12)

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not await defer_ephemeral(interaction):
            return
        if not isinstance(interaction.user, discord.Member) or not is_admin(interaction.user):
            await send_ephemeral(interaction, "Эта кнопка доступна только администрации.")
            return
        parsed_id = parse_int(str(self.task_id))
        if not parsed_id:
            await send_ephemeral(interaction, "Укажи корректный id задания.")
            return
        with closing(get_connection()) as conn:
            result = conn.execute("UPDATE task_templates SET is_active = 0 WHERE id = ?", (parsed_id,))
            conn.commit()
        await send_ephemeral(interaction, "Задание отключено." if result.rowcount else "Задание не найдено.")


class AdminPanelView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if isinstance(interaction.user, discord.Member) and is_admin(interaction.user):
            return True
        await send_ephemeral(interaction, "Эта панель доступна только администрации.")
        return False

    @discord.ui.button(label="Заявки", style=discord.ButtonStyle.secondary, custom_id="admin:withdrawals")
    async def withdrawals_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        with closing(get_connection()) as conn:
            rows = conn.execute(
                """
                SELECT id, user_id, currency_amount, rub_amount, status, created_at
                FROM withdrawals
                ORDER BY created_at DESC
                LIMIT 15
                """
            ).fetchall()
        if not rows:
            await send_ephemeral(interaction, "Заявок на вывод пока нет.")
            return
        lines = [
            f"#{row['id']} • user `{row['user_id']}` • {format_money(row['currency_amount'])} {CURRENCY_SHORT} "
            f"-> {row['rub_amount']:.2f} RUB • {row['status']}"
            for row in rows
        ]
        await send_ephemeral(interaction, "Последние заявки на вывод:\n" + "\n".join(lines))

    @discord.ui.button(label="История заданий", style=discord.ButtonStyle.secondary, custom_id="admin:completed_tasks")
    async def completed_tasks_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if not await defer_ephemeral(interaction):
            return
        await send_ephemeral(interaction, build_completed_tasks_message())

    @discord.ui.button(label="Одобрить вывод", style=discord.ButtonStyle.success, custom_id="admin:approve_withdrawal")
    async def approve_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, AdminWithdrawalModal(True))

    @discord.ui.button(label="Отклонить вывод", style=discord.ButtonStyle.danger, custom_id="admin:reject_withdrawal")
    async def reject_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, AdminWithdrawalModal(False))

    @discord.ui.button(label="Добавить товар", style=discord.ButtonStyle.primary, custom_id="admin:add_item")
    async def add_item_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, AdminAddItemModal())

    @discord.ui.button(label="Удалить товар", style=discord.ButtonStyle.secondary, custom_id="admin:remove_item")
    async def remove_item_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, AdminRemoveItemModal())

    @discord.ui.button(label="Добавить задание", style=discord.ButtonStyle.primary, custom_id="admin:add_task")
    async def add_task_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, AdminAddTaskModal())

    @discord.ui.button(label="Удалить задание", style=discord.ButtonStyle.secondary, custom_id="admin:remove_task")
    async def remove_task_button(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await send_modal_safely(interaction, AdminRemoveTaskModal())


@bot.event
async def on_ready() -> None:
    global PANEL_VIEWS_REGISTERED
    print(f"Бот запущен как {bot.user} ({bot.user.id})")
    if not PANEL_VIEWS_REGISTERED:
        bot.add_view(ControlPanelView())
        bot.add_view(AdminPanelView())
        PANEL_VIEWS_REGISTERED = True
    channel_id = get_control_channel_id()
    if SEND_CONTROL_PANEL_ON_READY and channel_id:
        channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            await send_or_update_control_panel(channel)


@bot.command(name="помощь")
async def help_command(ctx: commands.Context) -> None:
    embed = discord.Embed(
        title="Городская экономика",
        description=(
            f"Валюта сервера: **{CURRENCY_NAME} ({CURRENCY_SHORT})**.\n"
            "Зарабатывай, помогай городу, покупай вещи и отправляй заявку на вывод."
        ),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Основные команды",
        value=(
            f"`{BOT_PREFIX}баланс [@пользователь]`\n"
            f"`{BOT_PREFIX}работа`\n"
            f"`{BOT_PREFIX}задания`\n"
            f"`{BOT_PREFIX}взять <id>`\n"
            f"`{BOT_PREFIX}сдать <id>`\n"
            f"`{BOT_PREFIX}магазин`\n"
            f"`{BOT_PREFIX}купить <id> [кол-во]`\n"
            f"`{BOT_PREFIX}инвентарь`\n"
            f"`{BOT_PREFIX}конвертор <сумма> <валюта>`\n"
            f"`{BOT_PREFIX}вывести <сумма>`\n"
            f"`{BOT_PREFIX}курс`"
        ),
        inline=False,
    )
    embed.add_field(
        name="Команды администрации",
        value=(
            f"`{BOT_PREFIX}добавитьтовар <цена> <название> | <описание>`\n"
            f"`{BOT_PREFIX}удалитьтовар <id>`\n"
            f"`{BOT_PREFIX}добавитьзадание <мин> <макс> <название> | <описание>`\n"
            f"`{BOT_PREFIX}удалитьзадание <id>`\n"
            f"`{BOT_PREFIX}историязаданий`\n"
            f"`{BOT_PREFIX}панель`\n"
            f"`{BOT_PREFIX}выводы`\n"
            f"`{BOT_PREFIX}одобритьвывод <id>`\n"
            f"`{BOT_PREFIX}отклонитьвывод <id>`"
        ),
        inline=False,
    )
    await ctx.send(embed=embed)


@bot.command(name="панель")
async def panel_command(ctx: commands.Context) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return
    await send_or_update_control_panel(ctx.channel)
    await ctx.send("Панель отправлена или обновлена.", delete_after=10)


@bot.command(name="баланс")
async def balance_command(ctx: commands.Context, member: Optional[discord.Member] = None) -> None:
    target = member or ctx.author
    with closing(get_connection()) as conn:
        user = ensure_user(conn, target.id)
        await ctx.send(
            f"Баланс {target.mention}: **{format_money(user['balance'])} {CURRENCY_SHORT}**\n"
            f"Заработано: {format_money(user['total_earned'])} {CURRENCY_SHORT}\n"
            f"Потрачено: {format_money(user['total_spent'])} {CURRENCY_SHORT}\n"
            f"Выведено: {format_money(user['total_withdrawn'])} {CURRENCY_SHORT}"
        )


@bot.command(name="курс")
async def rate_command(ctx: commands.Context) -> None:
    await ctx.send(build_rate_message() + f"\nКонвертор: `{BOT_PREFIX}конвертор <сумма> <руб|иридиум>`.")


@bot.command(name="конвертор", aliases=["конвертировать", "перевести"])
async def converter_command(ctx: commands.Context, amount: str, source_currency: str) -> None:
    result = convert_text(amount, source_currency)
    if "Проверь сумму" in result:
        await ctx.send(
            f"Используй формат: `{BOT_PREFIX}конвертор <сумма> <руб|иридиум>`.\n"
            f"Например: `{BOT_PREFIX}конвертор 300 руб` или `{BOT_PREFIX}конвертор 30 иридиум`."
        )
        return
    await ctx.send(result)


@bot.command(name="работа")
async def work_command(ctx: commands.Context) -> None:
    with closing(get_connection()) as conn:
        user = ensure_user(conn, ctx.author.id)
        cooldown = get_remaining_cooldown(user["last_work_at"], WORK_COOLDOWN_MINUTES)
        if cooldown:
            await ctx.send(f"До следующей работы осталось: **{format_timedelta(cooldown)}**.")
            return

        reward = random.randint(20, 70)
        new_balance = user["balance"] + reward
        conn.execute(
            """
            UPDATE users
            SET balance = ?, total_earned = total_earned + ?, last_work_at = ?
            WHERE user_id = ?
            """,
            (new_balance, reward, now_moscow().isoformat(), ctx.author.id),
        )
        add_transaction(conn, ctx.author.id, "work", reward, "Городская работа")
        conn.commit()

        message = random.choice(WORK_MESSAGES).format(reward=format_money(reward), short=CURRENCY_SHORT)
        await ctx.send(f"{ctx.author.mention} {message}\nНовый баланс: **{format_money(new_balance)} {CURRENCY_SHORT}**")


@bot.command(name="задания")
async def tasks_command(ctx: commands.Context) -> None:
    with closing(get_connection()) as conn:
        ensure_user(conn, ctx.author.id)
        active = conn.execute(
            """
            SELECT ta.id, tt.title, tt.description, ta.reward
            FROM task_assignments ta
            JOIN task_templates tt ON tt.id = ta.template_id
            WHERE ta.user_id = ? AND ta.status = 'active'
            ORDER BY ta.accepted_at DESC
            """,
            (ctx.author.id,),
        ).fetchall()
        available = conn.execute(
            """
            SELECT id, title, description, reward_min, reward_max
            FROM task_templates
            WHERE is_active = 1
            ORDER BY id ASC
            """,
        ).fetchall()

    embed = discord.Embed(title="Городские задания", color=discord.Color.green())
    if active:
        lines = [
            f"`{row['id']}` • **{row['title']}** • награда {format_money(row['reward'])} {CURRENCY_SHORT}\n{row['description']}"
            for row in active
        ]
        embed.add_field(name="У тебя в работе", value="\n\n".join(lines), inline=False)
    else:
        embed.add_field(name="У тебя в работе", value="Активных заданий пока нет.", inline=False)

    available_lines = [
        f"`{row['id']}` • **{row['title']}** • {format_money(row['reward_min'])}-{format_money(row['reward_max'])} {CURRENCY_SHORT}\n{row['description']}"
        for row in available
    ]
    embed.add_field(
        name="Доступные шаблоны",
        value="\n\n".join(available_lines) if available_lines else "Заданий сейчас нет.",
        inline=False,
    )
    embed.set_footer(text=f"Используй {BOT_PREFIX}взять <id>, чтобы принять задание.")
    await ctx.send(embed=embed)


@bot.command(name="взять")
async def accept_task_command(ctx: commands.Context, template_id: str) -> None:
    task_id = parse_int(template_id)
    if not task_id:
        await ctx.send("Укажи корректный id задания.")
        return

    with closing(get_connection()) as conn:
        user = ensure_user(conn, ctx.author.id)
        cooldown = get_remaining_cooldown(user["last_task_at"], TASK_COOLDOWN_MINUTES)
        if cooldown:
            await ctx.send(f"Новое задание можно взять через **{format_timedelta(cooldown)}**.")
            return

        template = conn.execute(
            """
            SELECT id, title, description, reward_min, reward_max
            FROM task_templates
            WHERE id = ? AND is_active = 1
            """,
            (task_id,),
        ).fetchone()
        if not template:
            await ctx.send("Такого активного задания нет.")
            return

        reward = random.randint(template["reward_min"], template["reward_max"])
        cursor = conn.execute(
            """
            INSERT INTO task_assignments(user_id, template_id, accepted_at, reward)
            VALUES (?, ?, ?, ?)
            """,
            (ctx.author.id, task_id, now_moscow().isoformat(), reward),
        )
        conn.execute(
            "UPDATE users SET last_task_at = ? WHERE user_id = ?",
            (now_moscow().isoformat(), ctx.author.id),
        )
        conn.commit()

    await ctx.send(
        f"{ctx.author.mention}, задание принято.\n"
        f"Номер задания: **{cursor.lastrowid}**\n"
        f"**{template['title']}**\n{template['description']}\n"
        f"Награда после сдачи: **{format_money(reward)} {CURRENCY_SHORT}**"
    )


@bot.command(name="сдать")
async def finish_task_command(ctx: commands.Context, assignment_id: str) -> None:
    task_id = parse_int(assignment_id)
    if not task_id:
        await ctx.send("Укажи корректный номер задания.")
        return

    with closing(get_connection()) as conn:
        user = ensure_user(conn, ctx.author.id)
        assignment = conn.execute(
            """
            SELECT ta.id, ta.user_id, ta.template_id, ta.accepted_at, ta.reward, ta.status, tt.title, tt.description
            FROM task_assignments ta
            JOIN task_templates tt ON tt.id = ta.template_id
            WHERE ta.id = ? AND ta.user_id = ?
            """,
            (task_id, ctx.author.id),
        ).fetchone()
        if not assignment:
            await ctx.send("Это задание не найдено.")
            return
        if assignment["status"] != "active":
            await ctx.send("Это задание уже закрыто.")
            return

        new_balance = user["balance"] + assignment["reward"]
        completed_at = now_moscow().isoformat()
        conn.execute("UPDATE task_assignments SET status = 'completed' WHERE id = ?", (task_id,))
        archive_completed_task(conn, assignment, completed_at)
        conn.execute(
            "UPDATE users SET balance = ?, total_earned = total_earned + ? WHERE user_id = ?",
            (new_balance, assignment["reward"], ctx.author.id),
        )
        add_transaction(conn, ctx.author.id, "task", assignment["reward"], assignment["title"])
        conn.commit()

    text = random.choice(TASK_FINISH_MESSAGES)
    await ctx.send(
        f"{ctx.author.mention} {text}\n"
        f"Получено: **{format_money(assignment['reward'])} {CURRENCY_SHORT}**\n"
        f"Баланс: **{format_money(new_balance)} {CURRENCY_SHORT}**"
    )


@bot.command(name="магазин")
async def shop_command(ctx: commands.Context) -> None:
    with closing(get_connection()) as conn:
        items = conn.execute(
            "SELECT id, name, description, price FROM shop_items WHERE is_active = 1 ORDER BY price ASC"
        ).fetchall()

    if not items:
        await ctx.send("Магазин пока пуст.")
        return

    embed = discord.Embed(
        title="Магазин города",
        description=f"Тут можно тратить {CURRENCY_NAME} на полезную и не очень шнягу.",
        color=discord.Color.gold(),
    )
    for item in items:
        rub_price = iridium_to_rub(item["price"])
        embed.add_field(
            name=(
                f"{item['id']}. {item['name']} — {format_money(item['price'])} {CURRENCY_SHORT} "
                f"({rub_price:.0f} RUB)"
            ),
            value=item["description"],
            inline=False,
        )
    embed.set_footer(text=f"Покупка: {BOT_PREFIX}купить <id> [кол-во]")
    await ctx.send(embed=embed)


@bot.command(name="купить")
async def buy_command(ctx: commands.Context, item_id: str, quantity: Optional[str] = "1") -> None:
    parsed_item_id = parse_int(item_id)
    parsed_quantity = parse_int(quantity)
    if not parsed_item_id or not parsed_quantity or parsed_quantity <= 0:
        await ctx.send("Укажи корректные id товара и количество.")
        return

    with closing(get_connection()) as conn:
        user = ensure_user(conn, ctx.author.id)
        item = conn.execute(
            "SELECT id, name, price FROM shop_items WHERE id = ? AND is_active = 1",
            (parsed_item_id,),
        ).fetchone()
        if not item:
            await ctx.send("Такого товара нет.")
            return

        total_cost = item["price"] * parsed_quantity
        if user["balance"] < total_cost:
            await ctx.send(
                f"Не хватает средств. Нужно **{format_money(total_cost)} {CURRENCY_SHORT}**, "
                f"а у тебя **{format_money(user['balance'])} {CURRENCY_SHORT}**."
            )
            return

        conn.execute(
            """
            INSERT INTO inventory(user_id, item_id, quantity)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, item_id)
            DO UPDATE SET quantity = quantity + excluded.quantity
            """,
            (ctx.author.id, item["id"], parsed_quantity),
        )
        conn.execute(
            """
            UPDATE users
            SET balance = balance - ?, total_spent = total_spent + ?
            WHERE user_id = ?
            """,
            (total_cost, total_cost, ctx.author.id),
        )
        add_transaction(conn, ctx.author.id, "purchase", -total_cost, item["name"])
        conn.commit()

    await ctx.send(
        f"{ctx.author.mention}, покупка прошла успешно: **{item['name']} x{parsed_quantity}**.\n"
        f"Потрачено: **{format_money(total_cost)} {CURRENCY_SHORT}**"
    )


@bot.command(name="инвентарь")
async def inventory_command(ctx: commands.Context) -> None:
    with closing(get_connection()) as conn:
        ensure_user(conn, ctx.author.id)
        items = conn.execute(
            """
            SELECT si.name, si.description, i.quantity
            FROM inventory i
            JOIN shop_items si ON si.id = i.item_id
            WHERE i.user_id = ? AND i.quantity > 0
            ORDER BY si.name ASC
            """,
            (ctx.author.id,),
        ).fetchall()

    if not items:
        await ctx.send("Инвентарь пока пуст.")
        return

    embed = discord.Embed(title=f"Инвентарь {ctx.author.display_name}", color=discord.Color.purple())
    for item in items:
        embed.add_field(name=f"{item['name']} x{item['quantity']}", value=item["description"], inline=False)
    await ctx.send(embed=embed)


@bot.command(name="вывести")
async def withdraw_command(ctx: commands.Context, amount: str) -> None:
    parsed_amount = parse_int(amount)
    if not parsed_amount or parsed_amount <= 0:
        await ctx.send("Укажи корректную сумму для вывода.")
        return
    if parsed_amount < WITHDRAW_MIN_IRIDIUM:
        await ctx.send(f"Минимальный вывод: **{WITHDRAW_MIN_IRIDIUM} {CURRENCY_SHORT}**.")
        return

    with closing(get_connection()) as conn:
        user = ensure_user(conn, ctx.author.id)
        if user["balance"] < parsed_amount:
            await ctx.send("На балансе недостаточно средств для вывода.")
            return

        rub_before_commission = iridium_to_rub(parsed_amount)
        rub_amount = round(rub_before_commission * (1 - WITHDRAW_COMMISSION_PERCENT / 100), 2)
        conn.execute(
            """
            UPDATE users
            SET balance = balance - ?, total_withdrawn = total_withdrawn + ?
            WHERE user_id = ?
            """,
            (parsed_amount, parsed_amount, ctx.author.id),
        )
        cursor = conn.execute(
            """
            INSERT INTO withdrawals(user_id, currency_amount, rub_amount, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (ctx.author.id, parsed_amount, rub_amount, now_moscow().isoformat()),
        )
        add_transaction(conn, ctx.author.id, "withdraw_request", -parsed_amount, f"Заявка #{cursor.lastrowid}")
        conn.commit()

    await ctx.send(
        f"{ctx.author.mention}, заявка на вывод создана.\n"
        f"Заявка: **#{cursor.lastrowid}**\n"
        f"Списано: **{format_money(parsed_amount)} {CURRENCY_SHORT}**\n"
        f"До комиссии: **{rub_before_commission:.2f} RUB**\n"
        f"Комиссия: **{WITHDRAW_COMMISSION_PERCENT:g}%**\n"
        f"К выплате: **{rub_amount:.2f} RUB**\n"
        "Теперь администрация должна одобрить или отклонить заявку."
    )


@bot.command(name="выводы")
async def withdrawals_command(ctx: commands.Context) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    with closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT id, user_id, currency_amount, rub_amount, status, created_at
            FROM withdrawals
            ORDER BY created_at DESC
            LIMIT 15
            """
        ).fetchall()

    if not rows:
        await ctx.send("Заявок на вывод пока нет.")
        return

    lines = [
        f"#{row['id']} • user `{row['user_id']}` • {format_money(row['currency_amount'])} {CURRENCY_SHORT} "
        f"-> {row['rub_amount']:.2f} RUB • {row['status']}"
        for row in rows
    ]
    await ctx.send("Последние заявки на вывод:\n" + "\n".join(lines))


def _review_withdrawal(withdrawal_id: int, approve: bool) -> tuple[bool, str]:
    with closing(get_connection()) as conn:
        withdrawal = conn.execute(
            "SELECT * FROM withdrawals WHERE id = ?",
            (withdrawal_id,),
        ).fetchone()
        if not withdrawal:
            return False, "Заявка не найдена."
        if withdrawal["status"] != "pending":
            return False, "Эта заявка уже обработана."

        new_status = "approved" if approve else "rejected"
        conn.execute(
            "UPDATE withdrawals SET status = ?, reviewed_at = ? WHERE id = ?",
            (new_status, now_moscow().isoformat(), withdrawal_id),
        )
        if not approve:
            conn.execute(
                """
                UPDATE users
                SET balance = balance + ?, total_withdrawn = total_withdrawn - ?
                WHERE user_id = ?
                """,
                (withdrawal["currency_amount"], withdrawal["currency_amount"], withdrawal["user_id"]),
            )
            add_transaction(
                conn,
                withdrawal["user_id"],
                "withdraw_rejected",
                withdrawal["currency_amount"],
                f"Возврат по заявке #{withdrawal_id}",
            )
        conn.commit()

    status_text = "одобрена" if approve else "отклонена, средства возвращены пользователю"
    return True, f"Заявка #{withdrawal_id} {status_text}."


@bot.command(name="одобритьвывод")
async def approve_withdrawal_command(ctx: commands.Context, withdrawal_id: str) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    parsed_id = parse_int(withdrawal_id)
    if not parsed_id:
        await ctx.send("Укажи корректный id заявки.")
        return

    _, message = _review_withdrawal(parsed_id, True)
    await ctx.send(message)


@bot.command(name="отклонитьвывод")
async def reject_withdrawal_command(ctx: commands.Context, withdrawal_id: str) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    parsed_id = parse_int(withdrawal_id)
    if not parsed_id:
        await ctx.send("Укажи корректный id заявки.")
        return

    _, message = _review_withdrawal(parsed_id, False)
    await ctx.send(message)


@bot.command(name="добавитьтовар")
async def add_item_command(ctx: commands.Context, price: str, *, payload: str) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    parsed_price = parse_int(price)
    if not parsed_price or parsed_price <= 0:
        await ctx.send("Цена должна быть положительным числом.")
        return

    if "|" not in payload:
        await ctx.send("Используй формат: `!добавитьтовар <цена> <название> | <описание>`")
        return

    name, description = [part.strip() for part in payload.split("|", 1)]
    with closing(get_connection()) as conn:
        cursor = conn.execute(
            "INSERT INTO shop_items(name, description, price, is_active) VALUES (?, ?, ?, 1)",
            (name, description, parsed_price),
        )
        conn.commit()
    await ctx.send(f"Товар добавлен. ID: **{cursor.lastrowid}**")


@bot.command(name="удалитьтовар")
async def remove_item_command(ctx: commands.Context, item_id: str) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    parsed_id = parse_int(item_id)
    if not parsed_id:
        await ctx.send("Укажи корректный id товара.")
        return

    with closing(get_connection()) as conn:
        result = conn.execute("UPDATE shop_items SET is_active = 0 WHERE id = ?", (parsed_id,))
        conn.commit()
    if result.rowcount == 0:
        await ctx.send("Товар не найден.")
    else:
        await ctx.send("Товар скрыт из магазина.")


@bot.command(name="добавитьзадание")
async def add_task_command(ctx: commands.Context, reward_min: str, reward_max: str, *, payload: str) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    parsed_min = parse_int(reward_min)
    parsed_max = parse_int(reward_max)
    if not parsed_min or not parsed_max or parsed_min <= 0 or parsed_max < parsed_min:
        await ctx.send("Проверь диапазон награды.")
        return

    if "|" not in payload:
        await ctx.send("Используй формат: `!добавитьзадание <мин> <макс> <название> | <описание>`")
        return

    title, description = [part.strip() for part in payload.split("|", 1)]
    with closing(get_connection()) as conn:
        cursor = conn.execute(
            """
            INSERT INTO task_templates(title, description, reward_min, reward_max, is_active)
            VALUES (?, ?, ?, ?, 1)
            """,
            (title, description, parsed_min, parsed_max),
        )
        conn.commit()
    await ctx.send(f"Задание добавлено. ID: **{cursor.lastrowid}**")


@bot.command(name="удалитьзадание")
async def remove_task_command(ctx: commands.Context, template_id: str) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return

    parsed_id = parse_int(template_id)
    if not parsed_id:
        await ctx.send("Укажи корректный id задания.")
        return

    with closing(get_connection()) as conn:
        result = conn.execute("UPDATE task_templates SET is_active = 0 WHERE id = ?", (parsed_id,))
        conn.commit()
    if result.rowcount == 0:
        await ctx.send("Задание не найдено.")
    else:
        await ctx.send("Задание отключено.")


@bot.command(name="историязаданий")
async def completed_tasks_command(ctx: commands.Context) -> None:
    if not isinstance(ctx.author, discord.Member) or not is_admin(ctx.author):
        await ctx.send("Эта команда доступна только администрации.")
        return
    await ctx.send(build_completed_tasks_message())


def load_token() -> str:
    env_token = os.getenv("DISCORD_TOKEN")
    if env_token:
        return env_token.strip().removeprefix("Bot ").strip()
    if TOKEN_PATH.exists():
        return TOKEN_PATH.read_text(encoding="utf-8").strip().removeprefix("Bot ").strip()
    raise RuntimeError("Токен не найден. Добавь DISCORD_TOKEN в окружение или в файл discord token.txt")


def main() -> None:
    init_db()
    token = load_token()
    try:
        bot.run(token)
    except PrivilegedIntentsRequired:
        print(
            "Бот запрашивает Message Content Intent. "
            "Включи его в Discord Developer Portal: "
            "Application -> Bot -> Privileged Gateway Intents -> Message Content Intent."
        )
        raise
    except LoginFailure:
        print(
            "Discord отклонил токен бота: 401 Unauthorized. "
            "Сгенерируй новый токен в Discord Developer Portal -> Bot -> Reset Token "
            "и вставь его в DISCORD_TOKEN в файле .env или в файл discord token.txt."
        )
        raise


if __name__ == "__main__":
    main()
