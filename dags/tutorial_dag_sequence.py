from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from random import randint

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


#branchpythonoperator 구성 -> task형태로
#적용될 함수 : 1,2 중 하나의 숫자를 랜덤으로 뽑아 1이면 path1, 2이면 path2로 이동
def random_branch_path():
    # 필요 라이브러리는 여기서 import
    # https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag 참고

    return "path1" if randint(1, 2) == 1 else "my_name_en"

with DAG( **dag_args ) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BranchPythonOperator(
        task_id='branch',
        python_callable=random_branch_path,  #해당 인자에 파이썬 함수를 적용
    )
    
    t3 = BashOperator(
        task_id='my_name_ko',
        depends_on_past=False,
        bash_command='echo "안녕하세요."',
    )

    t4 = BashOperator(
        task_id='my_name_en',
        depends_on_past=False,
        bash_command='echo "Hi"',
    )

    complete = BashOperator(
        task_id='complete',
        depends_on_past=False,
        bash_command='echo "complete~!"',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    dummy_1 = DummyOperator(task_id="path1")

    #task 관계 설정 (순서)
    #t2가 어떤 상태냐에 따라 dummy_1혹은 t4로 가게 됨.(파이썬 함수의 결과값에 따라서)
    t1 >> t2 >> dummy_1 >> t3 >> complete
    t1 >> t2 >> t4 >> complete