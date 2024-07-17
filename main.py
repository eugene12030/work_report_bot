import logging
import psycopg2
import os
import datetime
import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, ReplyKeyboardMarkup, \
    KeyboardButton, ReplyKeyboardRemove, FSInputFile
from aiogram.fsm.state import State, StatesGroup

API_TOKEN = '7483654306:AAGg8aJh0fpJp38dejeD-2LjnsfJIY4B2w8'

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize bot and dispatcher
storage = MemoryStorage()
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot=bot, storage=storage)

connection = psycopg2.connect(
    database="railway",
    user='postgres',
    password="PmULTRpTosfVfJTNzOyPfIWhPwyHRDqo",
    host="monorail.proxy.rlwy.net",
    port="42320",
)
connection.autocommit = True
cursor = connection.cursor()


class InitForm(StatesGroup):
    name = State()


class WorkPlace(StatesGroup):
    work_place = State()


class Koef_change(StatesGroup):
    name = State()
    new_koef = State()


admin_ids = [911018424, 478580891, 273205509]
std_koef = (445, 668, 500, 500)


@dp.message(Command('admin'))
async def admin(message: Message):
    if message.from_user.id in admin_ids:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Выгрузить трудовую ведомость", callback_data="get_work_report")],
            [InlineKeyboardButton(text="Выгрузить журнал", callback_data="get_journal")],
            [InlineKeyboardButton(text="Изменить ставку", callback_data="change_salary")]
        ])
        await message.answer("Доступные действия", reply_markup=keyboard)
    else:
        await message.answer("Вы не имеете доступа к этой команде")


@dp.message(Command('start'))
async def start(message: Message, state: FSMContext):
    cursor.execute("SELECT id FROM workers")
    users_id = cursor.fetchall()
    users_id = [int(x[0]) for x in users_id]
    if message.from_user.id not in users_id:
        await message.answer("Введите свое ФИО")
        await state.set_state(InitForm.name)
    else:
        await message.answer("Ваш id уже зарегистрирован")


@dp.message(InitForm.name)
async def process_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    user_data = await state.get_data()
    name = user_data['name']
    await state.clear()
    cursor.execute('SELECT name FROM workers')
    users_name = cursor.fetchall()
    users_name = [x[0] for x in users_name]
    if name not in users_name:
        query = f"INSERT INTO workers (id, name, worktime_storage, worktime_storage_overtime, worktime_montage, worktime_montage_overtime, status, koef_storage, koef_storage_overtime, koef_montage, koef_montage_overtime) VALUES ('{message.from_user.id}', '{name}', 0, 0, 0, 0, 0, 445, 668, 500, 500);"
        cursor.execute(query)
        # cursor.execute(f'INSERT INTO koef (id , storage, storage_overtime, montage, montage_overtime) VALUES ("{message.from_user.id}", 445, 668, 500, 500)')
        await message.answer("Вы успешно зарегистрированы")
    else:
        await message.answer("Ваше имя уже используется")


@dp.message(Command('help'))
async def help(message: Message):
    cursor.execute("SELECT id FROM workers")
    users_id = cursor.fetchall()
    users_id = [int(x[0]) for x in users_id]
    if message.from_user.id in users_id:
        # Create an inline keyboard
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Начать рабочий день", callback_data="start_day"),
             InlineKeyboardButton(text="Закончить рабочий день", callback_data="finish_day")],
            [InlineKeyboardButton(text="Отработка в этом месяце", callback_data="see_results")]
        ])

        await message.answer("Доступные действия", reply_markup=keyboard)
    else:
        await message.answer("Вы не зарегестрированы")


# Handle button presses
@dp.callback_query()
async def handle_callback_query(callback_query: CallbackQuery, state: FSMContext):
    worker_id = callback_query.from_user.id
    cursor.execute(f"SELECT status FROM workers WHERE id = '{worker_id}'")
    if callback_query.data == "start_day":
        status = cursor.fetchall()[0][0]
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Закончить рабочий день", callback_data="finish_day")]
        ])
        if status == 0:
            start_time = datetime.datetime.now()
            cursor.execute(
                f"UPDATE workers SET status = 1 , last_start_of_day = TIMESTAMP '{start_time}' WHERE id = '{worker_id}'")

            await callback_query.message.answer("Рабочий день начат", reply_markup=keyboard)
        else:
            await callback_query.message.answer("Рабочий день уже начат", reply_markup=keyboard)

    elif callback_query.data == "finish_day":
        status = cursor.fetchall()[0][0]
        if status == 1:
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="Монтаж"), KeyboardButton(text="Склад")]
                ],
                resize_keyboard=True,
                one_time_keyboard=True
            )
            await callback_query.message.answer("Укажите место работы", reply_markup=keyboard)
            await state.set_state(WorkPlace.work_place)

        else:
            await callback_query.message.answer("Рабочий день ещё не начат")

    elif callback_query.data == "see_results":
        cursor.execute(
            f"SELECT worktime_storage, worktime_storage_overtime, worktime_montage, worktime_montage_overtime FROM workers WHERE id = '{worker_id}'")
        cur_stats = cursor.fetchall()[0]
        cursor.execute(
            f"SELECT koef_storage, koef_storage_overtime, koef_montage, koef_montage_overtime FROM workers WHERE id = '{worker_id}'")
        koef = cursor.fetchall()[0]
        await callback_query.message.answer(f"В этом месяце вы проработали:\n"
                                            f"Склад : {cur_stats[0]} часов (+ {cur_stats[1]} часов сверхурочными)\n"
                                            f"Монтаж : {cur_stats[2]} часов (+ {cur_stats[3]} часов сверхурочными)\n"
                                            f"Вы заработали : {cur_stats[0] * koef[0] + cur_stats[1] * koef[1] + cur_stats[2] * koef[2] + cur_stats[3] * koef[3]} рублей")

    elif callback_query.data == "get_work_report":
        df = pd.read_sql('SELECT * FROM workers', connection)
        df = df.drop(columns=['status', 'last_start_of_day'])
        df = df.rename(columns=dict(zip(['name', 'worktime_storage', 'worktime_storage_overtime', 'worktime_montage',
                                         'worktime_montage_overtime'],
                                        ['Имя', "ВрС, ч", 'ВрС п/раб', 'ВрМ, ч', 'ВрМ п/раб'])))
        df['ЗпС'] = df['ВрС, ч'] * df['koef_storage']
        df['ЗпС п/раб'] = df['ВрС п/раб'] * df['koef_storage_overtime']
        df['ЗпМ'] = df['ВрМ, ч'] * df['koef_montage']
        df['ЗпМ п/раб'] = df['ВрМ п/раб'] * df['koef_montage_overtime']
        df['Итого'] = df['ЗпС'] + df['ЗпС п/раб'] + df['ЗпМ'] + df['ЗпМ п/раб']
        df = df.drop(columns=['koef_storage', 'koef_montage_overtime', 'koef_montage', 'koef_storage_overtime'])
        df.to_excel("Трудовая_ведомость.xlsx", index=False)
        file = FSInputFile("Трудовая_ведомость.xlsx")
        await bot.send_document(chat_id=worker_id, document=file)
        os.remove('Трудовая_ведомость.xlsx')

    elif callback_query.data == "get_journal":
        df = pd.read_sql('SELECT * FROM journal', connection)
        df = df.iloc[::-1].reset_index(drop=True)
        cursor.execute(f"SELECT id, name FROM workers")
        id_name_dict = {}
        for i in cursor.fetchall():
            id_name_dict[i[0]] = i[1]
        df = df.rename(columns=dict(zip(["worker_id", "start_of_day", 'end_of_day', 'work_hours', 'work_type'],
                                        ['Имя', "Начало дня", 'Конец дня', 'Часов отработано', 'Место работы'])))
        df = df.drop(columns=['id'])
        df['Имя'] = df['Имя'].replace(id_name_dict)
        df['Место работы'] = df["Место работы"].replace({0: "Монтаж", 1: "Склад"})
        df.to_excel("journal.xlsx", index=False)
        file = FSInputFile("journal.xlsx")
        await bot.send_document(chat_id=worker_id, document=file)
        os.remove('journal.xlsx')

    elif callback_query.data == "change_salary":
        await callback_query.message.answer("Введите ФИО рабочего")
        await state.set_state(Koef_change.name)


@dp.message(Koef_change.name)
async def change_koef1(message: Message, state: FSMContext):
    await state.update_data(name=message.text)
    await message.answer("Введите новую ставку в формате:\n"
                         "Склад, Склад п/раб, Монтаж, Монтаж п/раб\n"
                         "(руб/час)")
    await state.set_state(Koef_change.new_koef)


@dp.message(Koef_change.new_koef)
async def change_koef2(message: Message, state: FSMContext):
    await state.update_data(new_koef=message.text)
    data = await state.get_data()
    worker_name = data['name']
    new_koef = data['new_koef']
    try:
        new_koef = tuple(map(int, new_koef.split(",")))
        cursor.execute(
            f"update workers set koef_storage = {new_koef[0]}, koef_storage_overtime = {new_koef[1]}, koef_montage = {new_koef[2]}, koef_montage_overtime = {new_koef[3]} WHERE name = '{worker_name}'")
        await message.answer(f"Cтавка изменена")
    except:
        await message.answer("Неверный формат данных")


@dp.message(WorkPlace.work_place)
async def get_work_place(message: Message, state: FSMContext):
    worker_id = message.from_user.id
    await state.update_data(work_place=message.text)
    data = await state.get_data()
    work_place = data['work_place']
    await state.clear()
    if work_place not in ['Монтаж', 'Склад']:
        await message.answer(f"Место работы указано неверно {work_place}")
    else:
        end_time = datetime.datetime.now()
        cursor.execute(f"SELECT last_start_of_day FROM workers WHERE id = '{worker_id}'")
        start_time = cursor.fetchall()[0][0]
        work_hours = (end_time - start_time).seconds // 3600
        overtime = 0
        work_hours -= 1
        work_hours = max(work_hours, 0)
        cursor.execute(
            f"SELECT worktime_storage, worktime_storage_overtime, worktime_montage, worktime_montage_overtime FROM workers WHERE id = '{worker_id}'")
        cur_stats = cursor.fetchall()[0]
        if work_place == "Склад":
            if work_hours > 8:
                overtime = work_hours - 8
                work_hours = 8
            cursor.execute(
                f"UPDATE workers SET worktime_storage = {cur_stats[0] + work_hours}, worktime_storage_overtime = {cur_stats[1] + overtime} WHERE id = '{worker_id}'")
        if work_place == "Монтаж":
            if work_hours > 12:
                overtime = work_hours - 12
                work_hours = 12
            cursor.execute(
                f"UPDATE workers SET worktime_montage = {cur_stats[2] + work_hours}, worktime_montage_overtime = {cur_stats[3] + overtime} WHERE id = '{worker_id}'")
        cursor.execute(f"UPDATE workers SET status = 0 WHERE id = '{worker_id}'")

        cursor.execute(
            f"INSERT INTO journal (worker_id, start_of_day, end_of_day, work_hours, work_type) VALUES ('{worker_id}', TIMESTAMP'{start_time}', TIMESTAMP'{end_time}', {work_hours+overtime}, {int(work_place == 'Склад')})")
        await message.answer("Рабочий день закончен", reply_markup=ReplyKeyboardRemove())


async def month_change():
    df = pd.read_sql('SELECT * FROM workers', connection)
    df = df.drop(columns=['status', 'last_start_of_day'])
    df = df.rename(columns=dict(zip(['name', 'worktime_storage', 'worktime_storage_overtime', 'worktime_montage',
                                     'worktime_montage_overtime'],
                                    ['Имя', "ВрС, ч", 'ВрС п/раб', 'ВрМ, ч', 'ВрМ п/раб'])))
    df['ЗпС'] = df['ВрС, ч'] * df['koef_storage']
    df['ЗпС п/раб'] = df['ВрС п/раб'] * df['koef_storage_overtime']
    df['ЗпМ'] = df['ВрМ, ч'] * df['koef_montage']
    df['ЗпМ п/раб'] = df['ВрМ п/раб'] * df['koef_montage_overtime']
    df['Итого'] = df['ЗпС'] + df['ЗпС п/раб'] + df['ЗпМ'] + df['ЗпМ п/раб']
    df = df.drop(columns=['koef_storage', 'koef_montage_overtime', 'koef_montage', 'koef_storage_overtime'])
    df.to_excel("Трудовая_ведомость.xlsx", index=False)
    file = FSInputFile("Трудовая_ведомость.xlsx")

    for admin_id in admin_ids:
        await bot.send_document(chat_id=admin_id, document=file)
    os.remove('Трудовая_ведомость.xlsx')

    cursor.execute(
        f"UPDATE workers SET worktime_storage = 0, worktime_storage_overtime = 0, worktime_montage = 0, worktime_montage_overtime = 0")


async def loop():
    while True:
        now = datetime.datetime.now()
        if now.day == 1 and now.hour == 0 and now.minute == 0 and now.second == 0:
            await month_change()
        await asyncio.sleep(1)


# Start polling
async def main():
    asyncio.create_task(loop())
    await dp.start_polling(bot)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())
