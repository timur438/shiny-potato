from telethon import TelegramClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import random

chats_list = [
    (1, "Название чата 1"),
    (2, "Название чата 2"),
    (3, "Название чата 3"),
    (4, "Название чата 4")
]

messages_list = {
    1: ["Сообщение 1 для чата 1", "Сообщение 2 для чата 1", "Сообщение 3 для чата 1"],
    2: ["Сообщение 1 для чата 2", "Сообщение 2 для чата 2", "Сообщение 3 для чата 2"],
    3: ["Сообщение 1 для чата 3", "Сообщение 2 для чата 3", "Сообщение 3 для чата 3"],
    4: ["Сообщение 1 для чата 4", "Сообщение 2 для чата 4", "Сообщение 3 для чата 4"]
}

api_id = 'your_api_id'
api_hash = 'your_api_hash'

app = TelegramClient('telethon_test', api_id, api_hash)

scheduler = AsyncIOScheduler()

async def send_messages(client, chat_id, message_text, interval):
    try:
        for chat_id, chat_name in chats_list:
            messages = messages_list.get(chat_id, [])
            for message_text in messages:
                await client.send_message(chat_id, message_text)
                await asyncio.sleep(interval)
    except Exception as e:
        print(f"Внутренняя ошибка: {e}")

async def create_tasks(client, message):
    try:
        for chat_id, chat_name in chats_list:
            messages = messages_list.get(chat_id, [])
            interval = 20
            for message_text in messages:
                scheduler.add_job(send_messages, IntervalTrigger(seconds=1), args=[client, chat_id, message_text, interval], id=f"{chat_id}_{message_text}", misfire_grace_time=10)
    except Exception as e:
        await message.reply(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.command("on", prefixes=prefixes))
@check_username
async def on(client, message):
    global send_active
    send_active = True

    await message.reply("Рассылка запущена")

    await create_tasks(client, message)
    scheduler.start()

@app.on_message(filters.command("off", prefixes=prefixes))
@check_username
async def off(client, message):
    global send_active
    send_active = False
    scheduler.shutdown(wait=False)
    await message.reply("Рассылка остановлена")
