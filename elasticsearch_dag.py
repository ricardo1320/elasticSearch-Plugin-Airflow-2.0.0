"""
Installing ElasticSearch:
    curl -fsSL https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
    echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-7.x.list
    sudo apt update && sudo apt install elasticsearch
Inside sandbox (virtual python environment):
    pip install elasticsearch==7.10.1
Start and test it:
    sudo systemctl start elasticsearch
    curl -X GET 'http://localhost:9200'
"""

"""
    DAG to test the custom ElasticSearch Hook and PostgresToElastic Operator.
    Transfer data from Postgres (connections table of the metastore) to ElasticSearch in index:Connections.
"""

#Imports
from plugins.elasticsearch_plugin.hooks.elastic_hook import ElasticHook
from plugins.elasticsearch_plugin.operators.postgres_to_elastic import PostgresToElasticOperator 
from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2021, 5, 1)
}

#Functions
def _print_es_info():
    #Instantiate ElasticHook
    hook = ElasticHook()
    print(hook.info())


#DAG object
with DAG('elasticsearch_dag', schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    
    #Task to print the Info of the ElasticSearch Instance
    print_es_info = PythonOperator(
        task_id='print_es_info',
        python_callable=_print_es_info
    )

    #Task to trasfer data from Postgres to ElasticSearch.
    #Specifically the 'connections' table from Airflow metastore.
    connections_to_es = PostgresToElasticOperator(
        task_id='connections_to_es',
        sql = 'SELECT * FROM connection',
        index='connections'
    )

    #Dependencies
    print_es_info >> connections_to_es