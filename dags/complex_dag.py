from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, dag

@dag(schedule_interval='@daily', start_date=datetime(2022, 1, 1), catchup=False)
def complex_taskflow():

    @task()
    def extract():
        return {'data': 42}

    @task()
    def transform(order_data_dict: dict):
        return {'data': order_data_dict['data'] * 2}

    @task()
    def load(total_order_value: dict):
        print(f"Total order value is: {total_order_value['data']}")

    @task(multiple_outputs=True)
    def branch():
        return {
            'branch_a': 'Branch A data',
            'branch_b': 'Branch B data',
        }

    @task()
    def end():
        print('End task')

    order_data = extract()
    order_summary = transform(order_data)

    branch_data = branch()
    load(order_summary)

    end1 = end()
    end2 = end()

    order_summary >> [end1, end2]
    branch_data['branch_a'] >> end1
    branch_data['branch_b'] >> end2

example_dag = complex_taskflow()
