# Импорты стандартных библиотек и компонентов Airflow
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# Параметры подключения к PostgreSQL
PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "airflow"),
    "user": os.environ.get("POSTGRES_USER", "airflow"),
    "password": os.environ.get("POSTGRES_PASSWORD", "airflow"),
}

# Настройки DAG по умолчанию
default_args = {
    "owner": "data-eng",                    # Владелец DAG
    "email": ["owner@example.com"],        # Email для уведомлений
    "email_on_failure": True,              # Отправлять email при ошибке
    "email_on_success": False,             # Не отправлять email при успехе
    "retries": 2,                          # Количество попыток при ошибке
    "retry_delay": timedelta(seconds=20),  # Задержка между попытками
}

# Создание DAG
with DAG(
    dag_id="example_dag",                  # Уникальный ID DAG
    start_date=datetime(2025, 9, 1),      # Дата начала (условная)
    schedule_interval=None,                # Ручной запуск
    catchup=False,                         # Не догонять прошедшие запуски
    default_args=default_args,             # Применить настройки по умолчанию
    tags=["poc", "batch"],                 # Теги для поиска/фильтрации
) as dag:

    # Начальная точка DAG
    start = EmptyOperator(task_id="start")

    # Задача чтения CSV-файла
    @task(task_id="read_csv")
    def read_csv():
        # Чтение файла deliveries.csv
        df = pd.read_csv("/opt/airflow/data/deliveries.csv")
        # Подсчет количества неудачных доставок
        failed = int((df["delivery_status"] == "failed").sum())
        # Общее количество доставок
        total = int(len(df))
        # Возврат словаря с результатами
        return {"failed": failed, "total": total}

    # Задача чтения данных из PostgreSQL
    @task(task_id="read_postgres")
    def read_postgres():
        # SQL-запрос для подсчета заказов по статусам
        sql = "SELECT status, COUNT(*) FROM orders GROUP BY status;"
        # Подключение к БД и выполнение запроса
        with psycopg2.connect(**PG_CONN) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        # Возврат результата в виде словаря
        return {row[0]: row[1] for row in rows}

    # Задача ветвления на основе анализа данных
    @task.branch(task_id="branch")
    def decide_branch(csv_stats: dict):
        failed = csv_stats["failed"]
        total = max(csv_stats["total"], 1)  # Избегаем деления на 0
        ratio = failed / total              # Рассчет соотношения
        # Возвращаем ID следующей задачи в зависимости от результата
        return "alert_path" if ratio > 0.3 else "normal_path"

    # Пустые задачи для ветвления
    normal_path = EmptyOperator(task_id="normal_path")
    alert_path  = EmptyOperator(task_id="alert_path")

    # Задача обработки данных с retry-политикой
    @task(task_id="process_data", retries=1, retry_delay=timedelta(seconds=10))
    def process_data():
        # Эмуляция вызова внешнего сервиса (BigQuery)
        return "ok"

    # Fallback-обработчик при ошибках
    fallback = EmptyOperator(
        task_id="fallback_handler",
        trigger_rule=TriggerRule.ONE_FAILED  # Запускается, если хотя бы одна задача упала
    )

    # Задача отправки email при успехе
    notify_success = EmailOperator(
        task_id="notify_success",
        to="owner@example.com",
        subject="[POC] DAG succeeded",
        html_content="POC batch pipeline completed successfully.",
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Запускается, если все задачи успешны
    )

    # Задача отправки email при ошибке
    notify_failure = EmailOperator(
        task_id="notify_failure",
        to="owner@example.com",
        subject="[POC] DAG failed",
        html_content="POC batch pipeline failed. Check logs.",
        trigger_rule=TriggerRule.ONE_FAILED,  # Запускается, если хотя бы одна задача упала
    )

    # Конечная точка DAG
    end = EmptyOperator(task_id="end")

    # Вызов задач
    csv_stats = read_csv()
    orders = read_postgres()
    branch = decide_branch(csv_stats)

    # Определение зависимостей
    # start -> csv_stats, orders
    start >> [csv_stats, orders] >> branch
    # branch -> normal_path -> process_data
    branch >> normal_path >> process_data()
    # branch -> alert_path -> process_data
    branch >> alert_path  >> process_data()

    # process_data -> end
    process_data() >> end
    # process_data -> notify_success, notify_failure, fallback
    process_data() >> [notify_success, notify_failure, fallback]