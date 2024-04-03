import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters.command import Command
from aiogram.utils.keyboard import ReplyKeyboardBuilder

logging.basicConfig(level=logging.INFO)
bot = Bot(token="5305020634:AAHkuIW6YT1F64idr4zWKfLetJ8-5b-GT14")
dp = Dispatcher()

promo_codes = {
    "HELLO" : 10
    }

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    builder = ReplyKeyboardBuilder()
    
    builder.add(types.KeyboardButton(text='Купить рассылку', callback_data="buy_echo"))
    builder.add(types.KeyboardButton(text='Мой профиль', callback_data="my_account"))
    builder.add(types.KeyboardButton(text='Ввести промокод', callback_data="promocode"))
    
    await message.answer("Привет! Здесь ты можешь купить рассылку", reply_markup=builder.as_markup(),)

@dp.message()
async def process_enter_promo_code(message: types.Message):
    if message.text == "Ввести промокод":
        await message.answer("Введите промокод:")
    elif message.text.startswith("ПРОМОКОД:"):
        promo_code = message.text.split(":")[1].strip().upper()
        discount = promo_codes.get(promo_code, 0)
        if discount:
            await message.answer(f"Промокод '{promo_code}' принят. Ваша скидка: {discount}%")
        else:
            await message.answer("Неверный промокод. Попробуйте еще раз.")
    else:
        await message.answer(message.text)



async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
