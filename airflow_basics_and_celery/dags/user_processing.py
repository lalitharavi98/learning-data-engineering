from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# def __extract_user(ti):
#     fake_user = ti.xcom_pull(task_ids="is_api_available")
#     return {
#         "id": fake_user["id"],
#         "firstname": fake_user["personalInfo"]["firstName"],
#         "lastname": fake_user["personalInfo"]["lastName"],
#         "email": fake_user["personalInfo"]["email"],
#     }

@dag
def user_processing():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres", # airflow connection
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        # for downstream tasks to use the data pass it to xcom
        
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    @task
    def extract_user(fake_user: dict) -> dict:
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }
    
    @task
    def process_user(user_info: dict):
        import csv
        from datetime import datetime
        # user_info =  {
        #     "id": "123",
        #     "firstname": "John",
        #     "lastname": "doe",
        #     "email": "example.com",
        # }
        user_info["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)

    @task
    def store_user():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        pg_hook.copy_expert(
            sql="COPY users FROM STDIN WITH CSV HEADER",
            filename="/tmp/user_info.csv"
        )
    

    # extract_user = PythonOperator(
    #     task_id="extract_user",
    #     python_callable = __extract_user,
    # )
    process_user(extract_user(create_table >> is_api_available())) >> store_user()
    

user_processing()  # need to call at the end of the file otherwise wont show up on the airflow UI
