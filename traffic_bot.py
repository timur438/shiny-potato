from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import find_dotenv, load_dotenv
from pyrogram.errors import FloodWait
from pyrogram import Client, filters, types, enums
from datetime import datetime
from pyrogram.errors import *
import sqlite3
import asyncio
import random
import time
import os
import re


def check_username(func):
    allowed_username = ["TimurStrokach","cherviachello", "I_LOVEYOU1921"]
    async def wrapper(client, message):
        if message.from_user.username in allowed_username:
            await func(client, message)
        else:
            return
    return wrapper

connection = sqlite3.connect('traffic_bot1.db')
cursor = connection.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS main (
                    id INTEGER PRIMARY KEY,
                    username TEXT,
                    link TEXT,
                    add_time INTEGER,
                    send_status INTEGER DEFAULT 0
                )''')

proxy = {
     "scheme": "http",
     "hostname": "147.45.89.137",
     "port": 8000,
     "username": "fQELra",
     "password": "kssNeb"
}

app = Client(
    "Traffic Bot",
    app_version="1.0.0",
    device_model="PC",
    system_version="Linux",
    api_id=26884942,
    api_hash="d24e70487908ccc63aa4125062bf3703",
    proxy=proxy
)

prefixes = "!"
send_active = False

scheduler = AsyncIOScheduler()

@app.on_message(filters.command("on", prefixes=prefixes))
@check_username
async def on(client, message):
  global send_active
  try:
    await message.reply("Бот включен")

    send_active = True

    while send_active == True:
      await client.send_message("leomatchbot", "❤️")
      await asyncio.sleep(3, 10)
    
  except Exception as e:
        print(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.command("off", prefixes=prefixes))
@check_username
async def off(client, message):
  global send_active
  try:
    await message.reply("Бот выключен")
    
    send_active = False
    scheduler.shutdown(wait=False)
    
  except Exception as e:
        print(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.command("check", prefixes=prefixes))
@check_username
async def check(client, message):
  print("1")
  try:
    cursor.execute("SELECT * FROM main")
    main = cursor.fetchall()

    if main == None:
      await message.reply("Список пустой")
      return
  
    for user in main:
      datetime_obj = datetime.fromtimestamp(user[3])
      formatted_time = datetime_obj.strftime('%H:%M:%S')

      mes = f"`{user[1]}`, добавлен в `{formatted_time}`, "
      if user[4] == 0:
        mes += "сообщение не отправлено."
      if user[4] == 1:
        mes += "сообщение отправлено."

      await message.reply(mes)
      await asyncio.sleep(1)

  except Exception as e:
        print(f"Внутренняя ошибка: {str(e)}")

@app.on_message(filters.chat("leomatchbot"))
async def handle_message(client, message):
  global send_active
  message_text = message.text or message.caption
    
  if message_text != None and message_text.startswith("Нет такого варианта ответа"):
    await client.send_message("leomatchbot", "1")
    await asyncio.sleep(5, 15)
      
  elif message_text != None and message_text.startswith("Пришли свое расположение и увидишь анкеты рядом с тобой."):
    await client.send_message("leomatchbot", "Продолжить смотреть анкеты")
    await asyncio.sleep(5, 15)

  elif message_text != None and message_text.startswith("❗️ Помни, что в интернете люди могут выдавать себя за других."):
    await client.send_message("leomatchbot", "Продолжить просмотр анкет")
    await asyncio.sleep(5, 15)

  elif message_text != None and message_text.startswith("Ты понравился"):
    await client.send_message("leomatchbot", "1 👍")
    await asyncio.sleep(5, 15)
    
  elif message_text != None and message_text.startswith("Это все, идем дальше?"):
    await client.send_message("leomatchbot", "🚀 Продолжить")
    await asyncio.sleep(5, 15)
  
  elif message_text != None and message_text.startswith("Кому-то понравилась твоя анкета"):
    await client.send_message("leomatchbot", "❤️")
    await asyncio.sleep(5, 15)
      
  elif message_text != None and message_text.startswith("Есть взаимная симпатия! Начинай общаться"):
      
    for entity in message.entities:
      print(entity)
      link = ""
      if entity.type == enums.MessageEntityType.TEXT_LINK:
        link = entity.url
        print(link)
    pattern = r'https://t.me/(\S+)'
    username = re.findall(pattern, link)
    add_time = time.time()

    cursor.execute("INSERT INTO main (link, username, add_time) VALUES (?, ?, ?)", (link, username[0], add_time))
    connection.commit()
      
    await client.send_message("leomatchbot", "1 🚀")
    await asyncio.sleep(5, 15)

    await answer(client)
    
  elif message_text != None and message_text.startswith("Слишком много ❤️ за сегодня."):
    send_active = False
    await client.send_message("me", "Рассылка остановлена.")
    
@app.on_message(filters.private)
async def echo(client, message):
  
  count = await app.get_chat_history_count(message.chat.id)
  cursor.execute("SELECT username FROM main")
  usernames = cursor.fetchall()
  
  if count == 1 and (message.from_user.username,) in usernames:
    await message.reply("Привет")
    await asyncio.sleep(3, 6)
    await message.reply("Подпишись на мой канал, пожалуйста, там очень интересно) https://t.me/timur_strokach")

    cursor.execute("UPDATE main SET send_status = 1 WHERE username = ?", (username,))
    connection.commit()

async def answer(client):
  cursor.execute("SELECT * FROM main")
  main = cursor.fetchall()
  
  for user in main:
    username = user[1]
    link = user[2]
    add_time = user[3]
    send_status = user[4]
    
    current_time = time.time()
    
    if current_time - add_time >= 10*60 and send_status == 0:
      await client.send_message(username, "Привет")
      await asyncio.sleep(3, 6)
      await client.send_message(username, "Подпишись на мой канал, пожалуйста, там очень интересно) https://t.me/timur_strokach")
      
      cursor.execute("UPDATE main SET send_status = 1 WHERE username = ?", (username,))
      connection.commit()

async def answer_loop():
    while True:
        await asyncio.sleep(60)
        await answer(app)

async def send_loop(client):
    global send_active

    await client.send_message("leomatchbot", "❤️")
    messages = await client.get_history("leomatchbot", limit=3)
        
    if any("Слишком много ❤️ за сегодня" in message.text or message.caption for message in messages):
        send_active = False
    if not any("Слишком много ❤️ за сегодня" in message.text or message.caption for message in messages):
        send_active = True
        await client.send_message("me", "Рассылка возобновлена.")

scheduler.add_job(send_loop, IntervalTrigger(seconds=1200), args=[app])
scheduler.add_job(answer_loop, IntervalTrigger(seconds=2))
scheduler.start()
    
app.run()
