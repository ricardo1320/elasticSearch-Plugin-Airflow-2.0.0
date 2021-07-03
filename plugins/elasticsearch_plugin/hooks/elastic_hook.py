"""
    This file creates a plugin interacting with ElasticSearch,
    more specifically creates a Hook in order to interact with ElasticSearch.

    This file has to be under the path: plugins/elasticsearch_plugin/hooks
"""
#Imports
from airflow.hooks.base import BaseHook
from elasticsearch import Elasticsearch

#Class that inherits from BaseHook
class ElasticHook(BaseHook):

    #Constructor, override init method
    def __init__(self, conn_id='elasticsearch_default', *args, **kwargs):
        #Initialize BaseHook attributes
        super().__init__(*args, *kwargs)
        #Fetch the connectionId from the Hook, so we can get its attributes
        conn = self.get_connection(conn_id)

        #Dictionary to store the connection configuration
        conn_config = {}
        #List of hosts, as ElasticSearch can have multiple hosts corresponding to the multiple machines where ElasticSearch is running
        hosts = []

        #Check if we have the require attributes from the connection
        if conn.host:
            #If there are multiple connections, split them by comma and create a list
            hosts = conn.host.split(',')
        if conn.port:
            #Make sure the port is an Int, and add it to the config dictionary
            conn_config['port'] = int(conn.port)
        if conn.login:
            #Add login to the config dictionary
            conn_config['http_auth'] = (conn.login, conn.password) 

        #Create ElasticSearch object, pass the hosts and the connection configuration
        self.es = Elasticsearch(hosts, **conn_config)
        #As ElasticSearch stores data in indexes, we can specify and index
        self.index = conn.schema
    
    #Method to get Info about the ElasticSearch instance
    def info(self):
        return self.es.info()
    
    #Method to set a different index
    def set_index(self, index):
        self.index = index
    
    #Method to add a document (data) in the index
    def add_doc(self, index, doc_type, doc):
        self.set_index(index)
        res = self.es.index(index=index, doc_type=doc_type, body=doc)
        return res