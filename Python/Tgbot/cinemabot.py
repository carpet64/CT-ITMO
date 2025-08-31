import os
import sqlite3
from datetime import datetime
from aiogram import Bot, types, Dispatcher
from aiogram.utils import executor
import aiohttp
from typing import Optional
import logging
from googlesearch import search

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot_token = os.getenv('BOT_TOKEN')
API_KEY = os.getenv('KINOPOISK_API_KEY')
bot = Bot(token=str(bot_token))
dp = Dispatcher(bot)

timeout = aiohttp.ClientTimeout(total=60)

def init_db():
    conn = sqlite3.connect('cinema_bot.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS history
                 (user_id INTEGER, query_text TEXT, timestamp DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS stats
                 (user_id INTEGER, film_name TEXT, search_count INTEGER,
                  PRIMARY KEY (user_id, film_name))''')
    c.execute('''CREATE TEMPORARY TABLE temp_stats AS
                 SELECT user_id, film_name, SUM(search_count) as search_count
                 FROM stats
                 GROUP BY user_id, film_name''')
    c.execute('DELETE FROM stats')
    c.execute('''INSERT INTO stats (user_id, film_name, search_count)
                 SELECT user_id, film_name, search_count FROM temp_stats''')
    c.execute('DROP TABLE IF EXISTS temp_stats')
    conn.commit()
    conn.close()

init_db()

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message) -> None:
    await message.reply("🎬 Привет, кинопутешественник! \n"
                       "Я — твой проводник в мире кинематографа, где каждый кадр дышит магией, а сюжеты оставляют след в душе.\n"
                       "Что я умею?\n 🔍 Найти любой фильм или сериал — от культовой классики до свежего релиза.\n"
                       "🎯 Подскажу, где его можно посмотреть онлайн\n"
                       "📜 /history — посмотреть историю запросов\n"
                       "📊 /stats — узнать статистику по фильмам\n"
                       "Просто напиши название — и погружайся в кино!")

@dp.message_handler(commands=['history'])
async def show_history(message: types.Message):
    conn = sqlite3.connect('cinema_bot.db')
    c = conn.cursor()
    c.execute("SELECT query_text, timestamp FROM history WHERE user_id=? ORDER BY timestamp DESC LIMIT 10",
              (message.from_user.id,))
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        await message.answer("История запросов пуста.")
        return
    
    response = "📜 История запросов:\n"
    for query, time in rows:
        response += f"Запрос: {query}, Время: {time}\n"
    await message.answer(response)

@dp.message_handler(commands=['stats'])
async def show_stats(message: types.Message):
    conn = sqlite3.connect('cinema_bot.db')
    c = conn.cursor()
    c.execute("SELECT film_name, search_count FROM stats WHERE user_id=? ORDER BY search_count DESC",
              (message.from_user.id,))
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        await message.answer("Статистика пуста.")
        return
    
    response = "📊 Статистика:\n"
    for film, count in rows:
        response += f"Фильм: {film}, Показов: {count}\n"
    await message.answer(response)

async def search_kp(title: str):
    url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword?keyword={title}"
    headers = {"X-API-KEY": str(API_KEY)}
    
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            return await response.json()

async def get_film_details(film_id: int):
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{film_id}"
    headers = {"X-API-KEY": str(API_KEY)}
    
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url) as response:
            return await response.json()

async def find_movie_link(movie_title: str) -> Optional[str]:
    query = f"смотреть {movie_title} онлайн вк видео рутуб лордфильм"
    results = search(query, num_results=1, lang="ru")
    for r in results:
        logger.info(str(r))
        return str(r)
    return None

@dp.message_handler()
async def search_film(query: types.Message):
    if query.text.startswith('/'):
        return
    
    search_result = await search_kp(query.text)
    films = search_result.get("films", [])
    
    if not films:
        await query.answer("Фильм не найден 😢 Проверь название и попробуй ещё раз.")
        return
    film = films[0]
    film_id = film["filmId"]
    
    details = await get_film_details(film_id)
    film_name = details.get('nameRu', 'Без названия')
    if details.get('nameOriginal') is None:
        details['nameOriginal'] = film_name
    film_year = details.get('year', '—')
    if details.get('year') is None:
        details['nameOriginal'] = film_year
    conn = sqlite3.connect('cinema_bot.db')
    c = conn.cursor()
    c.execute("INSERT INTO history (user_id, query_text, timestamp) VALUES (?, ?, ?)",
              (query.from_user.id, query.text, datetime.now()))
    c.execute('''INSERT OR REPLACE INTO stats (user_id, film_name, search_count)
                 VALUES (?, ?, COALESCE((SELECT search_count + 1 FROM stats WHERE user_id=? AND film_name=?), 1))''',
              (query.from_user.id, film_name, query.from_user.id, film_name))
    conn.commit()
    conn.close()

    countries = [c['country'] for c in details.get('countries', [])]
    countries_str = ", ".join(countries) if countries else "—"
    genres = [g['genre'] for g in details.get('genres', [])]
    genres_str = ", ".join(genres) if genres else "—"
    description = details.get('description', 'нет описания')
    description = description.split("\n")[0]
    if len(description) > 500:
        description = description[:497] + "..."
    response = (
        f"🎥 <b>{film_name}</b>\n"
        f"🌍 Оригинальное название: {details.get('nameOriginal', '—')}\n"
        f"📅 Год: {film_year}\n"
        f"🏳️ Страна: {countries_str}\n"
        f"🎬 Жанры: {genres_str}\n"
        f"⭐ Рейтинг Кинопоиск: {details.get('ratingKinopoisk', '—')}\n"
        f"⭐ Рейтинг IMDb: {details.get('ratingImdb', '—')}\n"
        f"📖 Описание: {description}"
    )
    poster_url = details.get('posterUrl')

    link = await find_movie_link(f'{film_name} ({film_year})')
    
    if link:
        response += f"\nПосмотреть можно здесь: {link}"
    logger.info(f"Готово")
    if poster_url:
        await query.answer_photo(poster_url, caption=response, parse_mode="HTML")
    else:
        await query.answer(response, parse_mode="HTML")

if __name__ == '__main__':
    executor.start_polling(dp)