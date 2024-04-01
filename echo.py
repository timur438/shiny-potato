from pyrogram.raw.functions.messages import DeleteChatUser
from pyrogram.raw.types import InputPeerChat
from dotenv import find_dotenv, load_dotenv
from pyrogram.errors import FloodWait
from pyrogram import Client, filters
from pyrogram.types import Chat
from pyrogram.errors import *
import sqlite3
import asyncio
import random
import time
import os

proxy = {
     "scheme": "http",
     "hostname": "38.170.100.135",
     "port": 8000,
     "username": "dSehM4",
     "password": "VbDk8z"
}

app = Client(
    "echoBot",
    app_version="1.0.0",
    device_model="PC",
    system_version="Linux",
    api_id=13889057,
    api_hash="a7e6b904d00b738f3602d8903eb69bb8",
    proxy=proxy
)

messsage_id = None
tg_chat_id = None
admin_text = "edittext"
is_admin = False

@app.on_message(filters.private)
async def echo(client, message):
    global message_id
    global tg_chat_id
    global is_admin
    print(message)
    try:
        count = await app.get_chat_history_count(message.chat.id)
        if count == 1:
            await client.copy_message(message.from_user.id, tg_chat_id, message_id)
            
        elif is_admin:
            message_id = message.id
            tg_chat_id = message.chat.id
            await message.reply("Сообщение добавлено")
            is_admin = False
        elif message.text and message.text.lower() == admin_text:
            is_admin = True
            await message.reply("Отправьте ваше сообщение")
    except Exception as e:
        print(f"Внутренняя ошибка: {str(e)}")

app.run()
