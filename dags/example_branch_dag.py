# airflow branch

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


# DAG Graph 는 >>, <<, [] 를 이용해서 그릴 수 있음
# set_downstream , set_upstream

def random_branch_path():
    # library 여기서 import
    from random import randint

    return "path1" if randint(1, 2) == 1 else "my_name_en"

with DAG( **dag_args ) as dag:

    # task1
    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BranchPythonOperator(
        task_id='branch',
        python_callable=random_branch_path,
    )

    t3 = BashOperator(
        task_id='my_name_ko',
        depends_on_past=False,
        bash_command='echo "안녕하세요." ',
    )

    t4 = BashOperator(
        task_id='my_name_en',
        depends_on_past=False,
        bash_command='echo "Hi there"',
    )

    complete = BashOperator(
        task_id='complete',
        depends_on_past=False,
        bash_command='echo "complete~!"',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    dummy_1 = DummyOperator(task_id="path1")
    ## DAG Graph
    t1 >> t2 >> dummy_1 >> t3 >> complete # print ko
    t1 >> t2 >> t4 >> complete