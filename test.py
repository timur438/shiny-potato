from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio

async def send_messages():
    print("Sending messages...")

async def start_scheduler():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(send_messages, 'date')
    scheduler.start()
    scheduler.print_jobs()
    return scheduler

async def stop_scheduler(scheduler):
    scheduler.print_jobs()  # Здесь должны быть видны задачи
    scheduler.shutdown()

async def main():
    scheduler = await start_scheduler()
    await asyncio.sleep(10)  # Подождите некоторое время для наблюдения за результатами
    await stop_scheduler(scheduler)

asyncio.run(main())

