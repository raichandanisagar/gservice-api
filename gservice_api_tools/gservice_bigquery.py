#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: sagarraichandani
"""

from googleapiclient.errors import HttpError
import pandas as pd
import json
import os
import csv
import re
from functools import reduce


class Bigquery:
  """Fetch query results, create & manage datasets, tables and tabledata"""
  def __init__(self, bigquery_service, project_id):
    self._bqservice = bigquery_service
    self._pid = project_id
  
  @staticmethod
  def _build_dataframe_from_query_response(query_response):
    data = []
    schema_fields = [schema_field['name'] for schema_field in 
                     query_response['schema']['fields']]
    schema_fields
    
    for row_index in range(int(query_response['totalRows'])):
      row_data = []
      for column_index in range(len(schema_fields)):
        row_data.append(query_response['rows'][row_index]['f'][column_index]['v'])
      data.append(row_data)
    return pd.DataFrame(data=data,columns=schema_fields)
  
  def fetch_query_results(self, query, query_params=None, legacy_sql=False, 
                          timeout=300, dryrun=False):
    """Submit query to bigquery and return results
    Args:
      query: str, SQL query to run on BQ
      query_params: Standard SQL only. Query variables/parameters, if any.      
      legacy_sql: bool, False runs on standard SQL; True implies legacy
      timeout: float, seconds for query completion; 300 by default
      dryrun: bool, if True query only provides details on bytes and cache
    Returns dict 
      job_complete, error_message, job_id, total_bytes_processed and dataframe of rows"""
      
    query_results = {'job_complete':False, 'error_message':None, 'job_id':None, 
                     'total_bytes_processed':0, 'result_dataframe': None}
    
    jobs = self._bqservice.jobs()
    query_config = {'query':query,'timeoutMS':timeout*1000,'dryRun':dryrun,
                    'useLegacySql':legacy_sql}
    
    if(query_params):
      query_config['queryParameters'] = query_params
      
    try:
      query_response = jobs.query(projectId=self._pid, body=query_config).execute()
      query_results['job_complete'] = query_response.get('jobComplete')
      query_results['job_id'] = query_response.get('jobReference').get('jobId')
      query_results['total_bytes_processed'] = query_response.get('totalBytesProcessed')
      query_results['result_dataframe'] = self._build_dataframe_from_query_response(query_response) 
      
    except HttpError as e:
      query_results['error_message'] = json.loads(e.content.decode('utf8'))['error']['message']
      
    return query_results
  
  
  def get_dataset(self, dataset_id):
    """Returns the dataset resource specified by the dataset_id for more details: 
      https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.datasets.html#get
    Args:
      dataset_id: str, unique dataset id"""
      
    try:
      return self._bqservice.datasets().get(projectId = self._pid, datasetId = dataset_id).execute()
    except HttpError as e:
      print (json.loads(e.content.decode('utf8'))['error']['message'])
      return {}
  
  
  def check_dataset(self, dataset_id):
    """Returns bool to check if dataset exists
    Args:
      dataset_id: str, unqiue dataset id"""
      
    return bool(self.get_dataset(dataset_id))
  
  
  def create_dataset(self, dataset_id, dataset_desc=None):
    """Creates a dataset in the project. Returns created dataset's information. Empty dict upon error
    Args:
      dataset_id: str, unique dataset id for dataset creation
      dataset_desc: str, description of the dataset"""
      
    request_body = {'datasetReference':{'datasetId':dataset_id}}
    if(dataset_desc):
      request_body['description']=dataset_desc
    try:
      return self._bqservice.datasets().insert(projectId=self._pid, body=request_body).execute()
    except HttpError as e:
      print(json.loads(e.content.decode('utf8'))['error']['message'])
      return {}
  
  
  def get_table(self, dataset_id, table_id):
    """Returns the table resource specified by dataset_id, table_id
    for more details: https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.tables.html#get
    Args:
      dataset_id: str, unique dataset id
      table_id: str, unique table id"""
    
    return self._bqservice.tables().get(projectId = self._pid, datasetId = dataset_id, tableId = table_id)
  
  
  def check_table(self, dataset_id, table_id):
    """Returns bool to check if table exists
    Args:
      dataset_id: str, unique dataset id
      "table_id: str, unique table id"""
      
    return bool(self.get_table(dataset_id, table_id))
  
  @staticmethod
  def _create_schema_from_csv(schema_filepath):
    allowed_names = re.compile(r'^[a-zA-Z_]+[a-zA-Z0-9_]+$')
    schema = {'fields':[]}
    
    with open(schema_filepath) as csv_file:
      csv_reader = csv.DictReader(csv_file)
      for i,row in enumerate(csv_reader):
        temp_dict= {}
        field_name= row.get('field_name')
        if(bool(re.match(allowed_names,field_name))==False):
          raise ValueError("Field name {} in row {} must match the regex ^[a-zA-Z_]+[a-zA-Z0-9_]+$".format(field_name,i+1))
          
        field_description= row.get('description')
        if(field_description==''):
          field_description=None
        
        field_mode= row.get('mode')
        mode_ref = ('REQUIRED','REPEATED')
        if(field_mode not in mode_ref):
          field_mode='NULLABLE'
        
        field_type= row.get('type')
        type_ref= ('STRING','BYTES','INTEGER','INT64','FLOAT','FLOAT64','BOOLEAN','BOOL','TIMESTAMP','DATE','TIME','DATETIME','RECORD','STRUCT')
        if(field_type not in type_ref):
          raise ValueError("{} type val ({}) must be in {}".format(field_name,field_type,', '.join(type_ref)))
          
        
        temp_dict['name']= field_name
        temp_dict['description']= field_description
        temp_dict['mode']= field_mode
        temp_dict['type']= field_type
        
        schema['fields'].append(temp_dict)
    return schema
  
  
  def create_table(self, dataset_id, table_id, schema=None, schema_from_file=None, 
           time_partition=False, time_partition_field='_PARTITIONTIME'):
    """Creates a table in dataset specified by dataset_id; return tuple of bool, dict table resource of the created table.
    Args:
      dataset_id: str, unique dataset id
      table_id: str, unique table id
      schema: dict, describes schema of table as per https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema
        Ignored if None
      schema_from_file: str, constructs schema from csv file
        Ignored if None
      time_partition: bool, creates a time partition column if True
      time_partition_field: str, field to partition upon if time_partition=True"""
    
    table_request_body = {'tableReference':{'projectId':self._pid,'datasetId':dataset_id,
                        'tableId':table_id}}
    if(schema):
      table_request_body['schema'] = schema
    elif(schema_from_file!=None and os.path.exists(schema_from_file)):
      table_request_body['schema'] = self._create_schema_from_csv(schema_from_file)
    
    if(time_partition):
      table_request_body['timePartitioning'] = {'field':time_partition_field,'type':'DAY'}
      
    try:
      table_response = self._bqservice.tables().insert(projectId=self._pid, datasetId=dataset_id,
                           body=table_request_body).execute()
      return (True,table_response)
    except HttpError as e:
      print("Failed to create {}.{}\n {}".format(dataset_id,table_id,json.loads(e.content.decode('utf8'))['error']['message']))
      return (False,{})
    
  @staticmethod
  def _create_rows_from_file(rows_filepath):
    
    with open(rows_filepath) as csv_file:
      rows=[]
      csv_reader = csv.DictReader(csv_file)
      for row in csv_reader:
        rows.append(dict(row))
    return rows
  
  
  def stream_data(self, dataset_id, table_id, rows=None, rows_from_file=None, insert_id_key=None):
    """Streams data into Bigquery table; return tuple of bool, dict table resource of the created table. 
    Args:
      dataset_id: str, unique dataset id
      table_id: str, unique table id
      rows: list of dict (key-value pairs), rows to insert. More details https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/python/latest/bigquery_v2.tabledata.html#insertAll
      rows_from_file: str, filepath of csv to construct rows from
      insert_id_key: str, unique key for each row"""
    
    if(rows!=None):
      pass
    elif(rows_from_file!=None and os.path.exists(rows_from_file)):
      rows = self._create_rows_from_file(rows_from_file)
    else:
      raise ValueError("Expected one of rows or rows_from_file")
    
    insert_data = []
    for row in rows:
      temp_row = {}
      temp_row['json'] = row
      
      if insert_id_key is not None:
        keys = insert_id_key.split('.')
        val = reduce(lambda d, key: d.get(key) if d else None, keys, row)
        if val is not None:
          temp_row["insertId"] = val
      
      insert_data.append(temp_row)
    
    insert_request_body = {'rows':insert_data}
    
    try:
      insert_response = self._bqservice.tabledata().insertAll(projectId= self._pid, datasetId= dataset_id,
                             tableId= table_id, body=insert_request_body).execute()
      return (True,insert_response)
    except HttpError as e:
      print("Failed to create {}.{}\n {}".format(dataset_id,table_id,json.loads(e.content.decode('utf8'))['error']['message']))
      return (False,{})
      
    
    