"""
airflow를 활용한 머신러닝 예제
"""


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


import sys, os
sys.path.append(os.getcwd())

#정의된 타이타닉 머신러닝 코드를 가져와 객체 생성
from MLproject.titanic import *
titanic = TitanicMain()

def print_result(**kwargs):
    r = kwargs["task_instance"].xcom_pull(key='result_msg')
    print("message : ", r)

default_args = {
    'owner': 'owner-name',
    'depends_on_past': False,
    'email': ['your-email@g.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
}

dag_args = dict(
    dag_id="tutorial-ml-op",
    default_args=default_args,
    description='tutorial DAG ml',
    schedule_interval=timedelta(minutes=50),
    start_date=datetime(2022, 2, 1),
    tags=['example-sj'],
)

#operator를 사용해 각 task 정의
with DAG( **dag_args ) as dag:
    start = BashOperator(
        task_id='start',
        bash_command='echo "start!"',
    )

    #타이타닉 전처리를 수행하는 DAG task, prepro_data함수를 python_callable에 사용
    #함수에서 사용하는 parameter f_name을 op_kwargs로 보내줌
    prepro_task = PythonOperator(
        task_id='preprocessing',
        python_callable=titanic.prepro_data,
        op_kwargs={'f_name': "train"}
    )
    
    #전처리 된 타이타닉 데이터를 활용해 머신러닝 모델링 수행, run_modeling함수를 python_callable에 사용
    #함수에서 사용하는 n_estimator와 flag를 op_kwargs로 보내줌
    modeling_task = PythonOperator(
        task_id='modeling',
        python_callable=titanic.run_modeling,
        op_kwargs={'n_estimator': 100, 'flag' : True}
    )

    #앞서 받은 결과 값을 XCom에서 가져와서 출력
    msg = PythonOperator(
        task_id='msg',
        python_callable=print_result
    )

    complete = BashOperator(
        task_id='complete_bash',
        bash_command='echo "complete~!"',
    )

    #task 관계 설정 (시작 -> 전처리 task실행 -> 모델링 수행 -> 메세지 출력 -> 완료)
    start >> prepro_task >> modeling_task >> msg >> complete