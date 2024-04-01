import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from aiogram.utils.keyboard import ReplyKeyboardBuilder

logging.basicConfig(level=logging.INFO)
bot = Bot(token="6883695582:AAGc1nNOVj2okUWOhGbLVr6XixQxjs6MfWw")
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    builder = ReplyKeyboardBuilder()
    
    builder.add(types.InlineKeyboardButton(text='Купить рассылку', callback_data="buy_echo"))
    builder.add(types.InlineKeyboardButton(text='Мой профиль', callback_data="my_account"))
    builder.add(types.InlineKeyboardButton(text='Ввести промокод', callback_data="promocode"))
    
    await message.answer("Привет! Здесь ты можешь купить рассылку", reply_markup=builder.as_markup(),)

@dp.callback_query(F.data == "promocode")
async def promocode(callback: types.CallbackQuery):
    await callback.message.answer("Это меню промокода, скоро здесь что-то будет")




async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
