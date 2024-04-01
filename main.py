from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.memory import MemoryJobStore
from dotenv import find_dotenv, load_dotenv
from pyrogram.errors import FloodWait
from pyrogram import Client, filters
from pyrogram.errors import *
import asyncpg
import asyncio
import signal
import random
import time
import sys
import os

#СЛУЖЕБНЫЕ ПЕРЕМЕННЫЕ
allowed_usernames = ["TimurLebedev213"]
dsn = "postgres://postgres@localhost/test"
#proxy = {}
#app = Client()
prefixes = "!"
proxy = {
     "scheme": "http",
     "hostname": "147.45.89.137",
     "port": 8000,
     "username": "fQELra",
     "password": "kssNeb"
}

app = Client(
    "Android",
    app_version="1.0.0",
    device_model="PC",
    system_version="Linux",
    api_id=26884942,
    api_hash="d24e70487908ccc63aa4125062bf3703"
)

#СЛУЖЕБНЫЕ ФУНКЦИИ
async def chat_exist(identifier):
    async with asyncpg.create_pool(dsn=dsn) as pool:
        async with pool.acquire() as connection:
            if isinstance(identifier, int):
                exists = await connection.fetchval("SELECT EXISTS(SELECT username FROM groups WHERE id = $1)", identifier)
            elif isinstance(identifier, str):
                exists = await connection.fetchval("SELECT EXISTS(SELECT username FROM groups WHERE username = $1)", identifier)
            return exists

async def get_all_chat_ids():
    async with asyncpg.create_pool(dsn=dsn) as pool:
        async with pool.acquire() as connection:
            ids = await connection.fetch("SELECT id FROM groups")
            return [row['id'] for row in ids]

async def get_all_message_ids():
    async with asyncpg.create_pool(dsn=dsn) as pool:
        async with pool.acquire() as connection:
            ids = await connection.fetch("SELECT id FROM messages")
            return [row['id'] for row in ids]

def time_to_seconds(time_str):
    hours, minutes, seconds = map(int, time_str.split(':'))
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds

def seconds_to_time(seconds):
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def check_username(func):
    allowed_username = "TimurLebedev213"
    async def wrapper(client, message):
        if message.from_user.username in allowed_username:
            await func(client, message)
        else:
            return
    return wrapper

try:
    async def setup_database():
        async with asyncpg.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute('''
                        CREATE TABLE IF NOT EXISTS groups (
                            id SERIAL PRIMARY KEY,
                            title TEXT,
                            link TEXT,
                            username TEXT,
                            interval_seconds INTEGER DEFAULT 40,
                            messages_count INTEGER DEFAULT 0,
                            send_status INTEGER DEFAULT 0
                        );
                    ''')

                    await connection.execute('''
                        CREATE TABLE IF NOT EXISTS messages (
                            id SERIAL PRIMARY KEY,
                            id_chat INTEGER NOT NULL,
                            message_id INTEGER,
                            message_text TEXT,
                            tg_chat_id BIGINT,
                            FOREIGN KEY (id_chat) REFERENCES groups(id) ON DELETE CASCADE
                        );
                    ''')
        
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup_database())
    print("Connected to PostgreSQL")

except asyncpg.exceptions.PostgresError as e:
    print(f'Ошибка подключения к PostgreSQL: {e}')

@app.on_message(filters.command("menu", prefixes=prefixes))
@check_username
async def menu(client, message):
    await client.send_message(
        chat_id=message.chat.id,
        text="Список команд:\n\
1. `!menu` - Показать меню\n\
2. `!new {link}` - Добавить чат по юзернейму\n\
3. `!del {id_chat,id_chat...}` - Удалить чат/несколько чатов(1,2,3)/все чаты(all)\n\
4. `!chats` - Показать список чатов\n\
5. `!addmes {id_chat,id_chat...}` - Добавить сообщение для рассылки в чат/несколько чатов(1,2,3)/все чаты(all)\n\
6. `!delmes {message_id,message_id...}` - Удалить сообщение/несколько сообщений(1,2,3)\n\
7. `!timer {id_chat,id_chat...} {time}` - Установить таймер рассылки в чат/несколько чатов(1,2,3)/все чаты(all) в формате 06:12:34\n\
8. `!off {id_chat,id_chat...}` - Отключить рассылку, если без аргументов, то выключится во всех сразу\n\
9. `!on {id_chat,id_chat...}` - Включить рассылку, если без аргументов, то включится во всех сразу\n\
10.`!messages {id_chat}` - Показать все сообщения в чате\n\
11.`!allmessages` - Показать все сообщения\n\
12.`!delallin {id_chat,id_chat...}` - Удалить все сообщения из указанных чатов\n\
13.`!clearmess` - Удалить ВСЕ сообщения из базы данных\n\
14.`!clearall` - Отчистить ВСЮ базу данных. ВНИМАНИЕ! Эта функция не выходит из чатов!",
    )

@app.on_message(filters.command("docs", prefixes=prefixes))
@check_username
async def view_docs(client, message):
    await client.send_message(
        chat_id=message.chat.id,
        text="Пока что так",
    )

@app.on_message(filters.command("new", prefixes=prefixes))
@check_username
async def new_chat(client, message):
    global index
    global last_chat_time
    global current_time
    chat_exists = False

    command_parts = message.text.split(maxsplit=1)

    if command_parts is None or len(command_parts) != 2:
        await message.reply("Неверное количество аргументов. Используйте `!new {username}`")
        return

    username = str(command_parts[1])

    try:
        async with asyncpg.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as connection:
                chat_exists = await chat_exist(username)
                if chat_exists:
                    await message.reply("Чат с этим пользователем уже добавлен.")
                    return

                try:
                    await app.join_chat(username)

                    chat = await client.get_chat(username)
                    title = chat.title
                    link = f"https://t.me/{username}"

                    async with connection.transaction():
                        await connection.execute("INSERT INTO groups (title, link, username) VALUES ($1, $2, $3)", title, link, username)

                    await message.reply("Успешно присоединился в группу, после настройки не забудьте включить рассылку")
                    
                except UsernameInvalid:
                    await message.reply("Не могу присоединиться, проверьте юзернейм")
                except UserAlreadyParticipant:
                    await message.reply("Я уже присоединился к этой группе")
                except InviteHashExpired:
                    await message.reply("Эта ссылка для приглашения больше не работает")

    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: {e}")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{str(e)}`")

@app.on_message(filters.command("del", prefixes=prefixes))
@check_username
async def del_chat(client, message):
    try:
        command_parts = message.text.split(maxsplit=1)

        if len(command_parts) != 2:
            await message.reply("Неверное количество аргументов. Используйте `!del {id_chat}`")
            return

        async with asyncpg.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    if command_parts[1] == 'all':
                        id_chat = await get_all_chat_ids()
                    else:
                        id_chat = [int(id.strip()) for id in command_parts[1].split(",")]

                    del_flag = False
                    for id in id_chat:
                        chat_exists = await chat_exists(id)
                        if chat_exists:
                            try:
                                username = await connection.fetchval("SELECT username FROM groups WHERE id=$1", id)
                                await app.leave_chat(username)
                                await connection.execute("DELETE FROM groups WHERE id=$1", id)
                                await connection.execute("DELETE FROM messages WHERE id_chat=$1", id)
                                del_flag = True

                            except ValueError:
                                await message.reply("Индекс группы должен быть целым числом")
                        else:
                            await message.reply(f"Чата с id {id} не существует в базе")
                            del_flag = False

                    if del_flag:
                        await message.reply("Успешно вышел из групп и удалил сообщения")

    except IndexError:
        await message.reply("Неверное количество аргументов. Используйте `!del {id_chat}`")
    except ValueError:
        await message.reply("Индекс группы должен быть целым числом")
    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: {e}")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{e}`")

@app.on_message(filters.command("chats", prefixes=prefixes))
@check_username
async def view_chats(client, message):
    try:
        async with asyncpg.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as connection:
                groups = await connection.fetch("SELECT * FROM groups")
                chat_list_text = "Ваши чаты:\n"

                for chat in groups:
                    time = seconds_to_time(chat[4])
                    chat_list_text += f"{chat[0]}. `{chat[1]}`, интервал: `{time}`, кол-во сообщений: `{chat[5]}`\n"

                if chat_list_text == "Ваши чаты:\n":
                    await message.reply("Список пустой")
                else:
                    await message.reply(chat_list_text)

    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: `{e}`")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{e}`")

addmes_command_triggered = {}
id_chats = []

@app.on_message(filters.command("addmes", prefixes=prefixes))
@check_username
async def add_messages(client, message):
    try:
        command_parts = message.text.split(maxsplit=1)
        if len(command_parts) != 2:
            await message.reply("Неверное количество аргументов. Используйте `!addmes {id_chat}`")
            return

        global id_chats

        await message.reply("Введите ваше сообщение")

        chat_arg = command_parts[1]
        if chat_arg.lower() == 'all':
            id_chats = await get_all_chat_ids()
            id_chats = [int(chat_id) for chat_id in id_chats]
        else:
            id_chats = [int(chat_id) for chat_id in chat_arg.split(',')]

        for id_chat in id_chats:
            if not await chat_exist(id_chat):
                await message.reply(f"Чат с id {id_chat} не найден.")
                return
            addmes_command_triggered[message.chat.id] = id_chat

    except ValueError:
        await message.reply("Индекс группы должен быть целым числом")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.command("delmes", prefixes=prefixes))
@check_username
async def del_messages(client, message):
    try:
        command_parts = message.text.split(maxsplit=2)

        if len(command_parts) < 2 or len(command_parts) > 3:
            await message.reply("Неверное количество аргументов. Используйте `!delmes {message_id1,message_id2,...}` или `!delmes all`")
            return

        if command_parts[1].lower() == 'all':
            async with asyncpg.create_pool(dsn) as pool:
                async with pool.acquire() as connection:
                    async with connection.transaction():
                        await connection.execute("DELETE FROM messages")
                        await connection.execute("UPDATE groups SET messages_count = 0")
            await message.reply("Все сообщения успешно удалены из всех чатов")
            return

        message_ids = command_parts[1].split(',')
        message_ids = [int(id.strip()) for id in message_ids]

        if len(command_parts) == 2:
            async with asyncpg.create_pool(dsn) as pool:
                async with pool.acquire() as connection:
                    async with connection.transaction():
                        for message_id in message_ids:
                            id_chats = await connection.fetch("SELECT id_chat FROM messages WHERE message_id=$1", message_id)
                            for id_chat in id_chats:
                                await connection.execute("UPDATE groups SET messages_count = messages_count - 1 WHERE id=$1", id_chat['id_chat'])
                            await connection.execute("DELETE FROM messages WHERE message_id=$1", message_id)

            if len(message_ids) == 1:
                await message.reply("Сообщение успешно удалено из всех чатов")
            else:
                await message.reply("Сообщения успешно удалены из всех чатов")
        else:
            chat_ids = command_parts[2].split(',')
            chat_ids = [int(id.strip()) for id in chat_ids]

            async with asyncpg.create_pool(dsn) as pool:
                async with pool.acquire() as connection:
                    async with connection.transaction():
                        for message_id in message_ids:
                            id_chats = await connection.fetch("SELECT id_chat FROM messages WHERE message_id=$1", message_id)
                            for id_chat in id_chats:
                                if id_chat['id_chat'] in chat_ids:
                                    await connection.execute("UPDATE groups SET messages_count = messages_count - 1 WHERE id=$1", id_chat['id_chat'])
                            await connection.execute("DELETE FROM messages WHERE message_id=$1 AND id_chat = ANY($2::int[])", message_id, chat_ids)

            if len(message_ids) == 1:
                await message.reply("Сообщение успешно удалено из указанных чатов")
            else:
                await message.reply("Сообщения успешно удалены из указанных чатов")

    except ValueError:
        await message.reply("Индексы сообщений и идентификаторы чатов должны быть целыми числами")
    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: {str(e)}")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: {str(e)}")
        
@app.on_message(filters.command("timer", prefixes=prefixes))
@check_username
async def timer(client, message):
    try:
        command_parts = message.text.split(maxsplit=2)
        if len(command_parts) != 3:
            await message.reply("Неверное количество аргументов. Используйте `!timer {id_chat} {time}` или `!timer all {time}`")
            return

        id_chat_arg = command_parts[1].strip().lower()

        if id_chat_arg == "all":
            time = time_to_seconds(command_parts[2])

            await message.reply("Таймер установлен успешно для всех чатов")

            async with asyncpg.create_pool(dsn) as pool:
                async with pool.acquire() as connection:
                    async with connection.transaction():
                        await connection.execute("UPDATE groups SET interval_seconds = $1", time)

        else:
            id_chats = [int(chat_id) for chat_id in id_chat_arg.split(',')]
            time = time_to_seconds(command_parts[2])

            await message.reply("Таймер установлен успешно")

            async with asyncpg.create_pool(dsn) as pool:
                async with pool.acquire() as connection:
                    async with connection.transaction():
                        for id_chat in id_chats:
                            await connection.execute("UPDATE groups SET interval_seconds = $1 WHERE id = $2", time, id_chat)

    except IndexError:
        await message.reply("Неверное количество аргументов. Используйте `!timer {id_chat} {time}` или `!timer all {time}`")
    except ValueError:
        await message.reply("Индекс группы должен быть целым числом, время должно быть в формате `06:12:34`")
    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: {str(e)}")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.command("messages", prefixes=prefixes))
@check_username
async def view_messages(client, message):
    try:
        command_parts = message.text.split(maxsplit=1)

        if len(command_parts) != 2:
            await message.reply("Неверное количество аргументов. Используйте `!messages {id_chat}`")
            return

        id_chat = int(command_parts[1])

        async with asyncpg.create_pool(dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    messages = await connection.fetch("SELECT id, message_id, tg_chat_id FROM messages WHERE id_chat = $1 ORDER BY id_chat ASC", id_chat)
                    title = await connection.fetchval("SELECT title FROM groups WHERE id=$1", id_chat)

        messages_list_text = f"Ваши сообщения в чате `{title}`:\n"

        if not messages:
            await message.reply("Список пустой")
            return

        for msg in messages:
            message_text = await app.get_messages(msg['tg_chat_id'], msg['message_id'])
            if message_text is not None:
                if message_text.caption is not None:
                    content = message_text.caption
                elif message_text.text is not None:
                    content = message_text.text
                else:
                    content = "Сообщение без текста"
                words = content.split()[:15]
                truncated_message = ' '.join(words)
                messages_list_text += f"{msg['message_id']}. {truncated_message}\n"

        await message.reply(messages_list_text)

    except ValueError:
        await message.reply("Индекс чата должен быть целым числом")
    except IndexError:
        await message.reply("Неверное количество аргументов. Используйте `!messages {id_chat}`")
    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: {str(e)}")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.command("allmessages", prefixes=prefixes))
@check_username
async def view_all_messages(client, message):
    try:
        async with asyncpg.create_pool(dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    messages = await connection.fetch("SELECT m.message_id, m.message_text, m.id_chat, g.title FROM messages m INNER JOIN groups g ON m.id_chat = g.id ORDER BY m.message_id ASC")
    
        if not messages:
            await message.reply("Список пустой")
            return
    
        chats_messages = {}
    
        all_messages_text = "__Все ваши сообщения:__"
    
        await message.reply(all_messages_text)
    
        for msg in messages:
            message_id = msg['message_id']
            message_text = msg['message_text'][:50]
            id_chat = msg['id_chat']
            chat_title = msg['title']
    
            if message_id not in chats_messages:
                chats_messages[message_id] = {'message_text': message_text, 'chats': []}
    
            chats_messages[message_id]['chats'].append(f"{id_chat}. {chat_title}\n")
    
        for message_id, data in chats_messages.items():
            await message.reply(f"ID:`{message_id}`\n {data['message_text']}\nГруппы:\n {' '.join(data['chats'])}")
            await asyncio.sleep(0.7)
    
    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: `{str(e)}`")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{str(e)}`")

job_store = MemoryJobStore()
ascheduler = AsyncIOScheduler(jobstores={
    'default': job_store
})

async def send_messages(client, id, username, interval, messages):
    try:
        while True:
            for message in messages:
                tg_chat_id = message['tg_chat_id']
                message_id = message['message_id']
                await client.copy_message(username, tg_chat_id, message_id)
    except asyncio.exceptions.CancelledError:
        pass
    except Exception as e:
        print(f"Внутренняя ошибка: {e}")

async def create_tasks(client, message):
    global ascheduler
    try:
        async with asyncpg.create_pool(dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    groups = await connection.fetch("SELECT id, username, interval_seconds FROM groups")

                    for group in groups:
                        if group:
                            id = group['id']
                            username = group['username']
                            interval = group['interval_seconds']
                            messages = await connection.fetch("SELECT message_id, message_text, tg_chat_id FROM messages WHERE id_chat = $1", id)
                            ascheduler.add_job(send_messages, args=[client, id, username, interval, messages], id=str(id), max_instances=1)
                    
    except ValueError as e:
        await message.reply(f"Внутренняя ошибка: `{str(e)}`")

@app.on_message(filters.command("on", prefixes=prefixes))
@check_username
async def on(client, message):
    global ascheduler
    try:

        await create_tasks(client, message)
        ascheduler.start()
        await message.reply("Рассылка запущена")

    except ValueError as e:
        await message.reply(f"Внутренняя ошибка `{str(e)}`")

@app.on_message(filters.command("off", prefixes=prefixes))
@check_username
async def off(client, message):
    global ascheduler
    try:
        
        ascheduler.shutdown(wait=False)
        await message.reply("Рассылка отключена")

    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{str(e)}`")
    
@app.on_message(filters.command("clearall", prefixes=prefixes))
@check_username
async def clear_all(client, message):
    try:
        async with asyncpg.create_pool(dsn=dsn) as pool:
            async with pool.acquire() as connection:
                async with connection.transaction():
                    await connection.execute("DELETE FROM groups")
                    await connection.execute("DELETE FROM messages")
        
        await message.reply("Все данные успешно удалены из базы данных.")
    except asyncpg.exceptions.PostgresError as e:
        await message.reply(f"Ошибка PostgreSQL: `{e}`")
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{str(e)}`")
        
@app.on_message()
async def handle_message(client, message):
    try:
        if message.chat.id in addmes_command_triggered and addmes_command_triggered[message.chat.id]:
            id_chat = addmes_command_triggered.pop(message.chat.id)
            message_id = message.id
            tg_chat_id = message.chat.id
            message_text = message.text or message.caption

            await message.reply("Сообщение добавлено успешно")

            global id_chats
            async with asyncpg.create_pool(dsn=dsn) as pool:
                async with pool.acquire() as connection:
                    async with connection.transaction():
                        for id_chat in id_chats:
                            try:
                                await connection.execute("INSERT INTO messages (id_chat, message_id, message_text, tg_chat_id) VALUES ($1, $2, $3, $4)", id_chat, message_id, message_text, tg_chat_id)
                                await connection.execute("UPDATE groups SET messages_count = messages_count + 1 WHERE id = $1", id_chat)
                            except asyncpg.exceptions.PostgresError as e:
                                print(f"Ошибка PostgreSQL: `{str(e)}`")

        else:
            return
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: `{str(e)}`")

async def handler(signal, frame):
    async with asyncpg.create_pool(dsn=dsn) as pool:
        async with pool.acquire() as connection:
            async with connection.transaction():
                connection.close()
                print("Disconnected from PostgreSQL")
    sys.exit()

signal.signal(signal.SIGINT, handler)

app.run()
