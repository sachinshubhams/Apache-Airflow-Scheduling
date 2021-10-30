try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def product_fn_execute(**context):
    print("product_fn_execute execute")
    context['ti'].xcom_push(key='mykey', value="product_fn_execute Hello ")


def product_two_fn_execute(**context):
    instance = context.get("ti").xcom_pull(key="mykey")
    data = [{"name":"Sachin Shubham","title":"Data Scientist"}, { "name":"Shalini","title":"Data Scientist"},]
    df = pd.DataFrame(data=data)
    print('@'*66)
    print(df.head())
    print('@'*66)
    print("product_two_fn_execute got value :{} from Function 1  ".format(instance))


with DAG(
        dag_id="product_dags",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 10, 30),
        },
        catchup=False) as f:

    product_fn_execute = PythonOperator(
        task_id="product_fn_execute",
        python_callable=product_fn_execute,
        provide_context=True,
        op_kwargs={"name":"Sachin Shubham"}
    )
    product_two_fn_execute = PythonOperator(
        task_id="product_two_fn_execute",
        python_callable=product_two_fn_execute,
        provide_context=True,
    )

product_fn_execute >> product_two_fn_execute  