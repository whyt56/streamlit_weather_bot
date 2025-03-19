import asyncio
import streamlit as st
from analyze_dataset import load_and_process_data, plot_temperature_anomalies_interactive
from analyze_current import get_current_temperature_async, is_temperature_normal

def main():
    st.title("Анализ температуры и мониторинг текущей погоды")

    uploaded_file = st.file_uploader("Загрузите CSV файл с историческими данными", type=["csv"])

    if uploaded_file is not None:
        df_pandas, season_stats_seq, season_stats_parallel = load_and_process_data(uploaded_file)

        cities = df_pandas["city"].unique()
        selected_city = st.selectbox("Выберите город", cities)

        api_key = st.text_input("Введите API-ключ для OpenWeatherMap")

        if api_key:
            current_temp, error_message = asyncio.run(get_current_temperature_async(selected_city, api_key))

            if current_temp:
                st.write(f"Текущая температура в {selected_city}: {current_temp}°C")

                season = st.selectbox("Выберите сезон", ["winter", "spring", "summer", "autumn"])
                if is_temperature_normal(current_temp, selected_city, season, season_stats_seq):
                    st.write(f"Температура в {selected_city} является нормальной для сезона {season}.")
                else:
                    st.write(f"Температура в {selected_city} является аномальной для сезона {season}.")
            else:
                st.error(f"Ошибка получения данных: {error_message}")

        st.subheader(f"Статистика для {selected_city}")
        city_data = df_pandas[df_pandas["city"] == selected_city]
        st.write(city_data.describe())

        st.subheader(f"Временной ряд температур для {selected_city}")
        plot_temperature_anomalies_interactive(df_pandas, season_stats_parallel, selected_city)

if __name__ == "__main__":
    main()
