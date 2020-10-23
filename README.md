# airflow_yaml_dag
Simple way to create dags using YAML for airflow

This guide assumes that you have basic working knowledge of linux commands and python

## How to install and use

### Install docker and airflow (skip if you have already installed docker)
1. Install docker by following instructions in https://docs.docker.com/get-docker/
2. Install airflow using puckel version
    1. Download repo https://github.com/puckel/docker-airflow by clicking code->download zip
    2. Unzip repo
    3. Open terminal at the unzipped folder path
    4. Execute `docker-compose -f docker-compose-LocalExecutor.yml up -d`
      
### Clone into airflow dags folder
1. Log in to airflow webserver instance using `docker exec -u root -it <name of airflow webserver> /bin/bash`. 
You can find out the name of the webserver instance using `docker ps -a`
2. Navigate to default airflow dags folder `/usr/local/airflow/dags`
3. Clone into default airflow dags folder by running `git clone https://github.com/reivaxteo/airflow_yaml_dag.git`
4. Install pyYaml by running `pip install PyYAML==5.3.1`

### Create dags and tasks in yaml folder
1. put yaml files and other scripts in `/usr/local/airflow/dags/yaml_scheduler`
2. examples have been included

Have fun! :)
