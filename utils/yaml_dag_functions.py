from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.models.skipmixin import SkipMixin
from airflow.utils.dates import days_ago, datetime
import os
import sys
import yaml
from typing import Dict, List, Optional
from sqlalchemy import create_engine

py_dir = os.path.dirname(os.path.abspath(__file__))
base_path = os.path.dirname(py_dir)
sys.path.append(base_path)
yaml_config_file_path = os.path.join(base_path, 'config', 'config.yaml')
with open(yaml_config_file_path, 'r') as yamlfile:
    cfg = yaml.safe_load(yamlfile)


class MyPythonOperator(BashOperator):
    template_fields = ('bash_command', 'env', 'python', 'base_directory',)
    template_ext = ('.sh', '.bash', '.py',)

    def __init__(
            self,
            python_dir,
            *args, **kwargs):
        bash_command = '''python {python_file_path} {base_directory}
            '''.format(
            python_file_path=python_dir,
            base_directory=base_path)
        super(MyPythonOperator, self).__init__(bash_command=bash_command, *args, **kwargs)
        self.base_directory = base_path
        with open(python_dir) as file:
            self.python = file.read()
        # self.doc_md = self.python


class ErrorOperator(DummyOperator, SkipMixin):
    ui_color = "red"
    ui_fgcolor = "white"


def run_sql(**kwargs):
    con_target = kwargs.get('con_target')
    sql = kwargs.get('sql')
    if con_target == 'psql':
        engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(
            user=cfg['postgres']['pg_user'],
            password=cfg['postgres']['pg_password'],
            host=cfg['postgres']['pg_host'],
            port=cfg['postgres']['pg_port'],
            database=cfg['postgres']['pg_database']
        ))
        with engine.connect() as connection:
            connection.execute(sql)

    else:
        raise ValueError('Invalid connection type')


class DagTemplate:
    """Class to extract dag config from yaml and format for DagBuilder"""

    def __init__(self,
                 dag_yaml_path: str,
                 ):
        self.dag_yaml_path: str = dag_yaml_path
        self.dag_yaml_dir: str = os.path.dirname(dag_yaml_path)
        with open(dag_yaml_path) as f:
            self.dag_yaml_dict: Dict = dict(yaml.load(f, Loader=yaml.FullLoader))
        self.dag_defn: Dict = self.read_dag_yaml()
        self.task_defns: List[Dict] = self.read_tasks()

    def read_dag_yaml(self) -> Dict:
        if self.dag_yaml_dict.get('start_date_type') == 'days_ago':
            start_date = days_ago(self.dag_yaml_dict.get('start_date'))
        else:
            start_year = self.dag_yaml_dict.get('start_date').year
            start_month = self.dag_yaml_dict.get('start_date').month
            start_day = self.dag_yaml_dict.get('start_date').day

            start_date = datetime(int(start_year), int(start_month), int(start_day))
        if self.dag_yaml_dict.get('schedule_type') == 'minute':
            schedule_interval = timedelta(minutes=self.dag_yaml_dict.get('schedule_interval'))
        elif self.dag_yaml_dict.get('schedule_type') == 'hour':
            schedule_interval = timedelta(hours=self.dag_yaml_dict.get('schedule_interval'))
        elif self.dag_yaml_dict.get('schedule_type') == 'day':
            schedule_interval = timedelta(days=self.dag_yaml_dict.get('schedule_interval'))
        else:
            schedule_interval = self.dag_yaml_dict.get('schedule_interval', '0 0 * * *')

        dag_defn = {
            'dag_dir_path': self.dag_yaml_dict.get('root'),
            'dag_name': self.dag_yaml_dict.get('dag_name'),
            'catchup': self.dag_yaml_dict.get('catchup'),
            'default_args': {
                'owner': self.dag_yaml_dict.get('owner', 'airflow'),
                'depends_on_past': self.dag_yaml_dict.get('depends_on_past', False),
                'start_date': start_date,
                'email': self.dag_yaml_dict.get('email'),
                'email_on_failure': self.dag_yaml_dict.get('email_on_failure'),
                'email_on_retry': self.dag_yaml_dict.get('email_on_retry'),
                'retries': self.dag_yaml_dict.get('retries'),
                'retry_delay': timedelta(minutes=int(self.dag_yaml_dict.get('retry_delay_mins')))
            },
            'schedule_interval': schedule_interval
        }
        return dag_defn

    def read_tasks(self) -> List[Dict]:
        tasks = []
        for root, dirs, files in os.walk(self.dag_yaml_dir):
            for file in files:
                if file.endswith('.yaml') and file != 'dag.yaml':
                    abs_yaml_path = os.path.join(root, file)
                    # print(os.path.join(root, f))
                    with open(abs_yaml_path) as f:
                        tasks.append({
                            'root': root,
                            'file': file,
                            'yaml': dict(yaml.load(f, Loader=yaml.FullLoader))
                        })
        return tasks


class DagBuilder:
    """Class to store and run dags"""

    def __init__(
            self,
            dag_template: DagTemplate,
            base_path: str
    ):
        self.dag_template: DagTemplate = dag_template
        self.name: str = dag_template.dag_defn['dag_name']
        self.base_path: str = base_path
        self.dag: Optional[DAG] = None
        self.task_operators: Dict = {}
        self.root_task: Optional[BaseOperator] = None
        self.task_defns: List[Dict] = dag_template.task_defns

    def build(self):
        dag_template = self.dag_template
        self.dag = DAG(
            dag_template.dag_defn.get('dag_name'),
            catchup=dag_template.dag_defn.get('catchup'),
            default_args=dag_template.dag_defn.get('default_args', {}),
            schedule_interval=dag_template.dag_defn.get('schedule_interval', '0 0 * * *')
        )
        self.root_task = DummyOperator(
            task_id='_root',
            dag=self.dag
        )
        self.task_operators['_missing_dependencies'] = ErrorOperator(
            task_id='_missing_dependencies',
            dag=self.dag
        )
        self.add_tasks()
        self.add_task_dependencies()

        return self.dag

    def add_tasks(self):
        for task in self.task_defns:
            task_dict = task['yaml']
            if task_dict['operator_type'] == 'python':
                self.task_operators[task_dict['task_name']] = MyPythonOperator(
                    task_id=task_dict['task_name'],
                    python_dir=os.path.join(self.dag_template.dag_yaml_dir, task_dict['target']),
                    dag=self.dag
                )
            elif task_dict['operator_type'] == 'psql':
                self.task_operators[task_dict['task_name']] = PostgresOperator(sql=task_dict['sql'],
                                                                               task_id=task_dict['task_name'],
                                                                               postgres_conn_id=task_dict['con_target'],
                                                                               autocommit=True,
                                                                               dag=self.dag)

    def add_task_dependencies(self):
        for task in self.task_defns:
            task_dict = dict(task['yaml'])
            if not task['yaml']['dependencies']:
                self.task_operators[task_dict['task_name']].set_upstream(self.task_operators['_missing_dependencies'])
                continue
            for dependency_name in task['yaml']['dependencies']:
                if dependency_name == '_root':
                    self.task_operators[task_dict['task_name']].set_upstream(self.root_task)
                elif dependency_name in self.task_operators.keys():
                    self.task_operators[task_dict['task_name']].set_upstream(self.task_operators[dependency_name])


def find_dags(dir_path: str) -> List[str]:
    dag_paths = []
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if file == 'dag.yaml' and root != dir_path:
                abs_yaml_path = os.path.join(root, file)
                dag_paths.append(abs_yaml_path)
    return dag_paths
