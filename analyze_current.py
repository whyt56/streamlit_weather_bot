import aiohttp


async def get_current_temperature_async(city, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            if response.status == 200:
                current_temp = data['main']['temp']
                return current_temp, None
            else:
                return None, data.get("message", "Unknown Error")


def is_temperature_normal(current_temp, city, season, season_stats):
    stats = season_stats[(season_stats["city"] == city) & (season_stats["season"] == season)]
    if not stats.empty:
        mean_temp = stats["mean"].values[0]
        std_temp = stats["std"].values[0]
        if mean_temp - 2 * std_temp <= current_temp <= mean_temp + 2 * std_temp:
            return True
        else:
            return False
    return None
