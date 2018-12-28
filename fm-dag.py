from builtins import range
from datetime import timedelta

import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

ss = '/home/msr/spark-2.4.0-bin-hadoop2.7/bin/spark-submit'
pkg = '--packages mysql:mysql-connector-java:5.1.39,org.apache.spark:spark-avro_2.11:2.4.0'
py_file_loc = '/mnt/c/Users/MikeS/Documents/Github/Retail-Case-Study/'
args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='foodmart',
    default_args=args,
    schedule_interval='0 0 * * *',
    dagrun_timeout=timedelta(minutes=3),
)

start = DummyOperator(
	task_id='start',
	dag=dag
)

p1f = BashOperator(
	task_id='part1_full',
	bash_command=ss + " " + pkg + " "+ py_file_loc + "part1.py F",
	dag=dag
)

p1i = BashOperator(
        task_id='part1_inc',
        bash_command=ss + " " + pkg + " "+ py_file_loc + "part1.py I",
        dag=dag
)

p2 = BashOperator(
        task_id='part2',
        bash_command=ss + " " + pkg + " "+ py_file_loc + "part2.py",
        dag=dag
)

p3 = BashOperator(
        task_id='part3',
        bash_command=ss + " " + pkg + " "+ py_file_loc + "part3.py",
        dag=dag
)

end = DummyOperator(
        task_id='end',
        dag=dag
)

start >> p1f
start >> p1i
p1f >> p2
p1i >> p2
p2 >> p3
p3 >> end
