from airflow.sdk import dag, task

@dag
def celery_dag():
    @task
    def a():
        from time import sleep
        sleep(5)

    @task(
        queue="high_cpu"  # specify the queue name to route this task to a specific worker
    )
    def b():
        from time import sleep
        sleep(5)
    
    @task(
            queue="high_cpu"  # specify the queue name to route this task to a specific worker
    )
    def c():
        from time import sleep
        sleep(5)

    @task
    def d():
        from time import sleep
        sleep(5)

    a() >> [ b(), c()] >> d()


celery_dag()