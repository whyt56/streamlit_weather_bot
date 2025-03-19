import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import polars as pl
import time


"""Вычисление скользящего среднего Pandas"""
def compute_moving_average_pandas(df, window=30):
    df["moving_avg"] = df.groupby("city")["temperature"].transform(
        lambda x: x.rolling(window=window, min_periods=1).mean())
    return df


"""Вычисление скользящего среднего для Polars"""
def compute_moving_average_polars(df, window=30):
    return df.with_columns([
        pl.col("temperature")
        .rolling_mean(window)
        .alias("moving_avg")
    ])


"""Вычисление средней температуры и стандартного отклонения для Polars"""
def compute_season_stats_polars(df):
    df = compute_moving_average_polars(df)
    return df.group_by(["city", "season"]).agg([
        pl.col("temperature").mean().alias("mean"),
        pl.col("temperature").std().alias("std")
    ])


"""У меня получилось что в среднем параллельные вычисления сделанные с помощью Polars ускоряют вычисления примерно в 5 раз"""
def load_and_process_data(uploaded_file):
    df_pandas = pd.read_csv(uploaded_file, parse_dates=["timestamp"])

    df_pandas["timestamp"] = df_pandas["timestamp"].astype(str)

    df = pl.from_pandas(df_pandas)

    start_time = time.time()
    df_pandas = compute_moving_average_pandas(df_pandas)
    season_stats_seq = df_pandas.groupby(["city", "season"])["temperature"].agg(['mean', 'std']).reset_index()
    end_time = time.time()
    print(f"Последовательное выполнение (Pandas): {end_time - start_time:.5f} сек")

    start_time = time.time()
    season_stats_parallel = compute_season_stats_polars(df)
    end_time = time.time()
    print(f"Параллельное выполнение (Polars): {end_time - start_time:.5f} сек")


    season_stats_parallel = season_stats_parallel.to_pandas()

    return df_pandas, season_stats_seq, season_stats_parallel


def plot_temperature_anomalies_interactive(df_pandas, season_stats_parallel, selected_city):
    df_pandas["timestamp"] = pd.to_datetime(df_pandas["timestamp"])

    merged = df_pandas.merge(season_stats_parallel, on=["city", "season"], how="left")
    merged["is_anomaly"] = (merged["temperature"] < (merged["mean"] - 2 * merged["std"])) | (
            merged["temperature"] > (merged["mean"] + 2 * merged["std"]))

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=merged[merged["city"] == selected_city]["timestamp"],
                             y=merged[merged["city"] == selected_city]["temperature"],
                             mode='lines', name='Температура', line=dict(color='skyblue'),
                             hovertext=merged[merged["city"] == selected_city]["timestamp"].dt.strftime('%d-%m-%Y'),
                             hoverinfo='text+y'))

    fig.add_trace(go.Scatter(x=merged[merged["city"] == selected_city]["timestamp"],
                             y=merged[merged["city"] == selected_city]["moving_avg"],
                             mode='lines', name='Скользящее среднее', line=dict(dash='dash', color='orange'),
                             hoverinfo='skip'))

    anomalies = merged[(merged["city"] == selected_city) & merged["is_anomaly"]]
    fig.add_trace(go.Scatter(x=anomalies["timestamp"],
                             y=anomalies["temperature"],
                             mode='markers', name='Аномалии', marker=dict(color='red', size=5),
                             hovertext=anomalies["timestamp"].dt.strftime('%d-%m-%Y'),
                             hoverinfo='text+y'))

    fig.update_layout(
        title=f"Температурные аномалии в {selected_city}",
        xaxis_title='Дата',
        yaxis_title='Температура (°C)',
        xaxis_tickangle=45,
        xaxis=dict(
            dtick="M12",
            tickformat="%Y",
            tickmode="linear"
        ),
        template='plotly_dark'
    )

    st.plotly_chart(fig)

