#DAG를 정의하는데 필요한 모듈을 import하는 코드
#Airflow Module :DAG - dag를 정의하기 위함, operators - 실제 연산(실행)을 하기 위해 필요한 operators들을 불러오기 위함
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


#DAG를 만들거나 다른 task들을 만들 때, default로 제공하는 파라미터의 집합인 default_args를 선언하는 코드 (나중에 operator를 선언할 때 변경 가능)
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',                 #해당 task 소유자, unix 사용자 이름을 사용하는 것을 추천
    'depends_on_past': False,           #true일 경우, task instance가 순차적으로 실행됨(이전 task instance가 성공했거나, 건너뛴 경우에만 실행), start)date에 대한 task instance실행이 허용됨
    'email': ['airflow@example.com'],   #email 알람을 받는 메일 주소, 하나 혹은 여러 메일주소 입력이 가능
    'email_on_failure': False,          #task가 실패할 경우 메일을 보내는 여부
    'email_on_retry': False,            #task가 재시도 될 경우 메일을 보내는 여부
    'retries': 1,                       #task가 실패할 경우 재시도하는 횟수
    'retry_delay': timedelta(minutes=5),#재시도 사이의 delay 시간
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


#task를 DAG에 넣기 위해 DAG를 선언한다. DAG란? = 방향이 있는 비순환 그래프, 방향성이 있는 작업의 모음, 예약, 시작 날짜 및 종료날짜(선택사항)이 있고 짜여진 스케줄이 충족 될 때 실행한다.
with DAG(
    'tutorial',                          #DAG의 id이다, ASCII에 있는 영어, 숫자, 대시, 점, 밑줄로만 구성되야 함.
    default_args=default_args,           #default 파라미터 변수 dict
    description='A simple tutorial DAG', #airflow 웹 서버에서 보여지는 내용
    schedule_interval=timedelta(days=1), #얼마나 자주 DAG를 run 할 지 정하는 것, 스케쥴을 파악하기 위해서 timedelta 객체가 task instance의 execution_date에 추가된다.
    start_date=days_ago(2),              #스케줄러가 backfill을 시도하는 것(아마 스케줄러가 DAG를 실행 queue에 집어 넣는 것을 말하는 것)
    tags=['example'],                    #Optional(List[str]]=None)
) as dag:

    #tasks = operator를 인스턴스화 할 때 생성된다. 인스턴스화 된 연산자를 Task라고 부른다. task id는 task의 unique identifier이다.
    #task에 대한 우선순위 룰 : 1. 명시적으로 전달된 arguments, 2.default_args dictionary에 있는 값, 3.해당하는 operator에 있는 default value
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    
    #BashOperator를 인스턴스화 할 때 받는 다양한 argument가 있을 수 있다. task에는 반드시 task id와 owner가 전달되어야 한다.
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 5',
        retries=3,
    )
    t1.doc_md = dedent(  #textwrap.dedent(text)함수는 text 앞 쪽의 공백을 없애주는 함수다. 특히 트리플 쿼트 """에서 사용할 경우, 왼쪽 정렬해 주는 효과
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

    """
    )

    #DAG와 Task에 documentation을 만들 수 있다. DAG는 markdown 언어만 지원하고 Task 는 plain text, markdown, reStructuredText, json, yaml 등을 지원
    dag.doc_md = __doc__  # providing that you have a docstring at the beggining of the DAG
    dag.doc_md = """

    #Airflow에서는 Jinja 라는 template의 장점을 활용하고 pipeline 작성자에게 기본 제공되는 매개변수 및 매크로 집합을 제공한다. 또한 그들 자체의 own parameters와 macros 그리고 templates을 제공한다.
    This is a documentation placed anywhere
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"                   #{{ ds }}는 (today's "date stamp")를 의미
        echo "{{ macros.ds_add(ds, 7)}}"  #macros라는 함수 불러오기
        echo "{{ params.my_param }}"      #user가 직접 정의한 parameter
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},  #params이라는 파라미터 hook을 사용하여 파라미터 혹은 객체의 dictionary를 템플릿에 전달
    )

    t1 >> [t2, t3]   #dependency 정의 내리는 곳 = t1 작업이 끝나면 t2, t3를 시작하라는 뜻

"""
dependency 정의

t1.set_downstream(t2)  #t1 다음에 t2를 실행

t2.set_upstream(t1)    #t2 앞에 t1을 실행 (위와 하는 일이 동일)

t1 >> t2               #t1다음 t2실행

t2 << t1               #t2전에 t1실행 (위와 동일)

t1 >> t2 >> t3         #t1 실행 이후 t2 실행 이후 t3 실행

t1.set_downstream([t2, t3])  #3개 다 같은 뜻으로 t1실행 후 t2,3 실행
t1 >> [t2, t3]
[t2, t3] << t1
"""