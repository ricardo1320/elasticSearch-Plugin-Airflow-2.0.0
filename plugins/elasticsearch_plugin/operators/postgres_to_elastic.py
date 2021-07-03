"""
    This file creates a Custom Operator in order to transfer data from Postgres to ElasticSearch.
    This file has to be under the path: plugins/elasticsearch_plugin/operators
"""
"""
    Create a Postgres connection in the Airflow UI:
    admin -> connections -> postgres_default -> delete
    admin -> connections -> add new connection:
        Conn Id = postgres_default
        Conn Type = postgres
        Host = localhost
        Login: postgres
        Password: postgres
        Port: 5432
        Extra: {"cursor":"realdictcursor"}

    Create the password for Postgres in the CL:
    sudo -u postgres psql
    ALTER USER postgres PASSWORD 'postgres';
"""
"""
    Which information are we going to transfer?
    Well, the connections from the metastore:
        SELECT * FROM connection;
"""
"""
    Check if there is data in ElasticSearch (query ElasticSearch, the index: connections):
    curl -X GET "http://localhost:9200/connections/_search" -H "Content-type: application/json" -d '{"query":{"match_all":{}}}'
"""
#Imports
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from plugins.elasticsearch_plugin.hooks.elastic_hook import ElasticHook

from contextlib import closing
import json

#Class that inherits from BaseOperator
class PostgresToElasticOperator(BaseOperator):

    #Constructor, override init method
    def __init__(self, sql, index, postgres_conn_id="postgres_default", elastic_conn_id="elasticsearch_default", *args, **kwargs):
        #Initialize BaseOperator attributes
        super(PostgresToElasticOperator, self).__init__(*args, **kwargs)
        #Assign the parameters to the attributes of the class
        self.sql = sql
        self.index = index
        self.postgres_conn_id = postgres_conn_id
        self.elastic_conn_id = elastic_conn_id

    #Must override the 'execute' method
    def execute(self, context):
        #Logic to transfer data from Postgres to ElasticSearch
        #Instatiate the hooks
        es = ElasticHook(conn_id=self.elastic_conn_id)
        pg = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        #Get Postgres connection
        #Use 'closing' to automatically close the object when is not needed
        with closing(pg.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                #Define number of rows to fetch at a time
                cur.itersize = 1000
                #Execute the SQL request
                cur.execute(self.sql)

                #Loop through the cursor and create Json object for every row
                for row in cur:
                    doc = json.dumps(row, indent=2)
                    #Once we have the document we can add it to ElasticSearch
                    es.add_doc(index=self.index, doc_type='external', doc=doc)

