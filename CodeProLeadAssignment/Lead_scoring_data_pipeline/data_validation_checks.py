

##############################################################################
# Import necessary modules
# ############################################################################# 
import pandas as pd

import os
import sqlite3
from sqlite3 import Error
import collections
import sys
import importlib.util

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

constants = module_from_file("constants", "/home/airflow/dags/Lead_scoring_data_pipeline/constants.py")

schema = module_from_file("schema","/home/airflow/dags/Lead_scoring_data_pipeline/schema.py")
###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    # Read the csv file
    raw_data = pd.read_csv(constants.DATA_DIRECTORY)
    raw_data.drop(columns=['index'], inplace=True, axis=1, errors='ignore')
    data_cols = raw_data.columns.to_list()
    
    if collections.Counter(data_cols) == collections.Counter(schema.raw_data_schema):
        print('Raw datas schema is in line with the schema present in schema.py')
    else:
        print('Raw datas schema is NOT in line with the schema present in schema.py')

###############################################################################
# Define function to validate model's input schema
# ############################################################################## 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        raw_data_schema_check
    '''
    conn = sqlite3.connect(constants.DB_PATH+constants.DB_FILE_NAME)
    df = pd.read_sql('select * from model_input', conn)
    
    df.drop(columns=['index'], inplace=True, axis=1, errors='ignore')
    
    source_columns = df.columns.to_list()
    
    result =  all(col in source_columns for col in schema.model_input_schema)
    if result:
        print('Models input schema is in line with the schema present in schema.py')
    else:
        print('Models input schema is NOT in line with the schema present in schema.py') 

    
    
