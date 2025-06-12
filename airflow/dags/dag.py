from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def get_or_create_time_var():
    try:
        time_str = Variable.get("processing_time")
        time_dict = json.loads(time_str)
        time_dict.setdefault("year", 2020)
        time_dict.setdefault("month", 1)
        return time_dict
    except KeyError:
        initial_time = {"year": 2020, "month": 1}
        Variable.set("processing_time", json.dumps(initial_time))
        return initial_time

@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    catchup=False,
    tags=["monthly_batch_processing"]
)
def batch_processing_dag():

    @task
    def get_or_create_time():
        current_time = get_or_create_time_var()
        print(f"⏳ Current batch processing time: {current_time}")
        return {
            "year": current_time["year"],
            "month": current_time["month"]
        }

    @task
    def increase_time_var():
        current_time = get_or_create_time_var()
        dt = datetime(current_time["year"], current_time["month"], 1) + relativedelta(months=1)
        updated = {"year": dt.year, "month": dt.month}
        Variable.set("processing_time", json.dumps(updated))
        print(f"✅ Updated batch processing time to: {updated}")
        return updated


    time_params = get_or_create_time()

    batch_processing = SparkSubmitOperator(
        task_id="batch_processing",
        application="/opt/airflow/code/extract.py",
        name="ExtractData",
        application_args=[
            "{{ ti.xcom_pull(task_ids='get_or_create_time')['year'] }}",
            "{{ ti.xcom_pull(task_ids='get_or_create_time')['month'] }}"
        ],
        conn_id="spark_default",
        verbose=True,
        conf={
            "spark.executor.memory": "1536m",
            "spark.driver.memory": "1024m",
            "spark.executor.cores": "3",
            "spark.num.executors": "3",
            "spark.sql.shuffle.partitions": "50",
            "spark.default.parallelism": "9"
        }
    )


    next_time = increase_time_var()

    time_params >> batch_processing >> next_time

batch_processing_dag = batch_processing_dag()
