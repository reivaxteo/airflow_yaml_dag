from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago, datetime
import os
import sys

py_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(py_dir)
sys.path.append(base_path)

from utils.yaml_dag_functions import DagTemplate, find_dags, DagBuilder

yaml_path = os.path.join(base_path, 'yaml_scheduler')

if not os.path.exists(yaml_path):
    os.makedirs(yaml_path, exist_ok=True)

dag_paths = find_dags(yaml_path)

for dag_path in dag_paths:
    try:
        dag_template = DagTemplate(dag_path)
        dag = DagBuilder(dag_template, base_path)
        globals()[dag.name] = dag.build()
    except BaseException as err:
        print('Error in parsing {}\n{}'.format(
            dag_path,
            str(err)
        ))
        continue
