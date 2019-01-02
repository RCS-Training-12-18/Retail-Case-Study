from builtins import range
from datetime import timedelta

import airflow
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

ss = '/home/msr/spark-2.4.0-bin-hadoop2.7/bin/spark-submit'
mysql_pkg = 'mysql:mysql-connector-java:5.1.39'
avro_pkg = 'org.apache.spark:spark-avro_2.11:2.4.0'
p1pkg = '--packages ' + mysql_pkg + ',' + avro_pkg
p2pkg = '--packages ' + avro_pkg
py_file_loc = '/mnt/c/Users/MikeS/Documents/Github/Retail-Case-Study/'

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='foodmart',
    default_args=args,
    catchup = False,
    schedule_interval='*/45 * * * *',
    dagrun_timeout=timedelta(minutes=10),
)

start = DummyOperator(
	task_id='start',
	dag=dag
)

p1f = BashOperator(
	task_id='import_from_mysql_to_s3_full_load',
	bash_command=ss + " " + p1pkg + " "+ py_file_loc + "part1.py F",
	trigger_rule=TriggerRule.ONE_FAILED,
	dag=dag
)

p1i = BashOperator(
        task_id='import_from_mysql_to_s3_full_incremental',
        bash_command=ss + " " + p1pkg + " "+ py_file_loc + "part1.py I",
        dag=dag
)

p2 = BashOperator(
        task_id='data_cleansing',
        bash_command=ss + " " + p2pkg + " "+ py_file_loc + "part2.py",
	trigger_rule=TriggerRule.ONE_SUCCESS,
        dag=dag
)

p3 = BashOperator(
        task_id='aggregation_and_move_to_staging',
        bash_command=ss + " " + py_file_loc + "part3.py",
        dag=dag
)

p4 = BashOperator(
        task_id='move_data_from_s3_to_snowflake',
        bash_command=ss + " " + py_file_loc + "part4.py",
        dag=dag
)

end = DummyOperator(
        task_id='end',
        dag=dag
)

start >> p1i
p1i >> p1f
p1f >> p2
p1i >> p2
p2 >> p3
p3 >> p4
p4 >> end
