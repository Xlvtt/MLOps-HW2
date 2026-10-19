import streamlit as st
import pandas as pd
import altair as alt
from kafka import KafkaProducer
import json
import time
import os
import uuid
import psycopg2

# Конфигурация Kafka
KAFKA_CONFIG = {
    "bootstrap_servers": os.getenv("KAFKA_BROKERS", "kafka:9092"),
    "topic": os.getenv("KAFKA_TOPIC", "transactions")
}

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
}

MODEL_COLUMN_ALIASES = {
    "transaction_time": ("transaction_time", "trans_date_trans_time"),
    "gender": ("gender",),
    "merch": ("merch", "merchant"),
    "cat_id": ("cat_id", "category"),
    "one_city": ("one_city", "city"),
    "us_state": ("us_state", "state"),
    "jobs": ("jobs", "job"),
    "amount": ("amount", "amt"),
    "population_city": ("population_city", "city_pop"),
    "lat": ("lat",),
    "lon": ("lon", "long"),
    "merchant_lat": ("merchant_lat", "merch_lat"),
    "merchant_lon": ("merchant_lon", "merch_long"),
}


def add_model_column_aliases(transaction):
    """Keep CSV fields and add the names expected by the online model."""
    transaction = dict(transaction)
    for model_column, candidates in MODEL_COLUMN_ALIASES.items():
        if model_column not in transaction:
            for candidate in candidates:
                if candidate in transaction:
                    transaction[model_column] = transaction[candidate]
                    break
    return transaction


def load_results():
    """Загрузка последних fraud-транзакций и скоров из PostgreSQL."""
    with psycopg2.connect(**POSTGRES_CONFIG) as connection:
        fraud = pd.read_sql_query(
            """
            SELECT transaction_id, score, fraud_flag, created_at
            FROM transaction_scores
            WHERE fraud_flag = 1
            ORDER BY created_at DESC
            LIMIT 10
            """,
            connection,
        )
        scores = pd.read_sql_query(
            """
            SELECT transaction_id, score, fraud_flag, created_at
            FROM transaction_scores
            ORDER BY created_at DESC
            LIMIT 100
            """,
            connection,
        )
    return fraud, scores

def load_file(uploaded_file):
    """Загрузка CSV файла в DataFrame"""
    try:
        return pd.read_csv(uploaded_file)
    except Exception as e:
        st.error(f"Ошибка загрузки файла: {str(e)}")
        return None

def send_to_kafka(df, topic, bootstrap_servers):
    """Отправка данных в Kafka с уникальным ID транзакции"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="PLAINTEXT"
        )
        
        # Генерация уникальных ID для всех транзакций
        df['transaction_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        progress_bar = st.progress(0)
        total_rows = len(df)
        
        for idx, row in df.iterrows():
            transaction = add_model_column_aliases(
                row.drop('transaction_id').to_dict()
            )
            # Отправляем данные вместе с ID
            producer.send(
                topic, 
                value={
                    "transaction_id": row['transaction_id'],
                    "data": transaction
                }
            )
            progress_bar.progress((idx + 1) / total_rows)
            time.sleep(0.01)
            
        producer.flush()
     
        return True
    except Exception as e:
        st.error(f"Ошибка отправки данных: {str(e)}")
        return False

# Инициализация состояния
if "uploaded_files" not in st.session_state:
    st.session_state.uploaded_files = {}

# Интерфейс
st.title("📤 Отправка данных в Kafka")

# Блок загрузки файлов
uploaded_file = st.file_uploader(
    "Загрузите CSV файл с транзакциями",
    type=["csv"]
)

if uploaded_file and uploaded_file.name not in st.session_state.uploaded_files:
    # Добавляем файл в состояние
    st.session_state.uploaded_files[uploaded_file.name] = {
        "status": "Загружен",
        "df": load_file(uploaded_file)
    }
    st.success(f"Файл {uploaded_file.name} успешно загружен!")

# Список загруженных файлов
if st.session_state.uploaded_files:
    st.subheader("🗂 Список загруженных файлов")
    
    for file_name, file_data in st.session_state.uploaded_files.items():
        cols = st.columns([4, 2, 2])
        
        with cols[0]:
            st.markdown(f"**Файл:** `{file_name}`")
            st.markdown(f"**Статус:** `{file_data['status']}`")
        
        with cols[2]:
            if st.button(f"Отправить {file_name}", key=f"send_{file_name}"):
                if file_data["df"] is not None:
                    with st.spinner("Отправка..."):
                        success = send_to_kafka(
                            file_data["df"],
                            KAFKA_CONFIG["topic"],
                            KAFKA_CONFIG["bootstrap_servers"]
                        )
                        if success:
                            st.session_state.uploaded_files[file_name]["status"] = "Отправлен"
                            st.rerun()
                else:
                    st.error("Файл не содержит данных")

st.divider()
st.header("📊 Результаты скоринга")

if st.button("Посмотреть результаты"):
    try:
        fraud_results, score_results = load_results()

        st.subheader("Последние 10 транзакций с fraud_flag = 1")
        if fraud_results.empty:
            st.info("Транзакций с признаком мошенничества пока нет.")
        else:
            st.dataframe(fraud_results, use_container_width=True, hide_index=True)

        st.subheader("Распределение скоров последних 100 транзакций")
        if score_results.empty:
            st.info("В базе пока нет результатов скоринга.")
        else:
            histogram = (
                alt.Chart(score_results)
                .mark_bar()
                .encode(
                    x=alt.X("score:Q", bin=alt.Bin(maxbins=10), title="Score"),
                    y=alt.Y("count():Q", title="Число транзакций"),
                    tooltip=[alt.Tooltip("count():Q", title="Транзакций")],
                )
            )
            st.altair_chart(histogram, use_container_width=True)
    except Exception as error:
        st.error(f"Не удалось загрузить результаты: {error}")
