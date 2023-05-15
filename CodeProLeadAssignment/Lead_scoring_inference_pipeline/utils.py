
import mlflow
import mlflow.sklearn
import pandas as pd

import sqlite3

import os
import logging

from datetime import datetime
from sklearn.metrics import roc_auc_score
import lightgbm as lgb
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix
import collections
import importlib.util

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

constants = module_from_file("constants", "/home/airflow/dags/Lead_scoring_inference_pipeline/constants.py") 
###############################################################################
# Define the function to train the model
# ##############################################################################


def encode_features():
    '''
    This function one hot encodes the categorical features present in our  
    training dataset. This encoding is needed for feeding categorical data 
    to many scikit-learn models.

    INPUTS
        db_file_name : Name of the database file 
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES : list of the features that needs to be there in the final encoded dataframe
        FEATURES_TO_ENCODE: list of features  from cleaned data that need to be one-hot encoded
        **NOTE : You can modify the encode_featues function used in heart disease's inference
        pipeline for this.

    OUTPUT
        1. Save the encoded features in a table - features

    SAMPLE USAGE
        encode_features()
    '''
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)
    input_data = pd.read_sql('select * from interactions_mapped', conn)
    input_data.drop(columns=['level_0', 'index'], axis = 1, inplace=True, errors='ignore')
    conn.close()
    print("Data has been extracted successfully.")

    features_to_encode=constants.FEATURES_TO_ENCODE
    one_hot_encoded_features=constants.ONE_HOT_ENCODED_FEATURES
    
    df = input_data[features_to_encode]
    
    encoded_df = pd.DataFrame(columns= features_to_encode)
    temp_df = pd.DataFrame()
    # One-Hot Encoding using get_dummies for the specified categorical features
    for f in features_to_encode:
        if(f in df.columns):
            encoded = pd.get_dummies(input_data[f])
            encoded = encoded.add_prefix(f + '_')
            temp_df = pd.concat([temp_df, encoded], axis=1)
        else:
            print('Feature not found')
    
    # Implement these steps to prevent dimension mismatch during inference
    for feature in one_hot_encoded_features:
        if feature in input_data.columns:
            encoded_df[feature] = input_data[feature]
        if feature in temp_df.columns:
            encoded_df[feature] = temp_df[feature]
    # fill all null values
    encoded_df.fillna(0, inplace=True)
    
    
    store_data_to_db(constants.DB_PATH ,constants.DB_FILE_NAME, encoded_df, 'features')

    print('input data has been written successfully to tables features')
###############################################################################
# Define the function to load the model from mlflow model registry
# ##############################################################################

def get_models_prediction():
    '''
    This function loads the model which is in production from mlflow registry and 
    uses it to do prediction on the input dataset. Please note this function will the load
    the latest version of the model present in the production stage. 

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        model from mlflow model registry
        model name: name of the model to be loaded
        stage: stage from which the model needs to be loaded i.e. production


    OUTPUT
        Store the predicted values along with input data into a table

    SAMPLE USAGE
        load_model()
    '''
    mlflow.set_tracking_uri(constants.TRACKING_URI)
    mlflow.set_experiment(constants.EXPERIMENT)

    # current working directory
    print(os.getcwd())

    # switch to a new directory
    os.chdir('/home/airflow/')

    # current working directory after switching"
    print(os.getcwd())

    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)
    X = pd.read_sql('select * from features', conn)
         
    model_uri = f"models:/{constants.MODEL_NAME}/{constants.STAGE}".format(model_name=constants.MODEL_NAME, model_stage=constants.STAGE)
    loaded_model = mlflow.pyfunc.load_model(model_uri)
    
    predictions = loaded_model.predict(pd.DataFrame(X))
    print(predictions)
    predicted_output = pd.DataFrame(predictions, columns=['predicted_output']) 
    store_data_to_db(constants.DB_PATH ,constants.DB_FILE_NAME, predicted_output, 'predicted_output')
    return "Predictions are done and save in Final_Predictions Table"

###############################################################################
# Define the function to check the distribution of output column
# ##############################################################################

def prediction_ratio_check():
    '''
    This function calculates the % of 1 and 0 predicted by the model and  
    and writes it to a file named 'prediction_distribution.txt'.This file 
    should be created in the ~/airflow/dags/Lead_scoring_inference_pipeline 
    folder. 
    This helps us to monitor if there is any drift observed in the predictions 
    from our model at an overall level. This would determine our decision on 
    when to retrain our model.
    

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be

    OUTPUT
        Write the output of the monitoring check in prediction_distribution.txt with 
        timestamp.

    SAMPLE USAGE
        prediction_col_check()
    '''
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)
    input_data = pd.read_sql('select * from predicted_output', conn)
    input_data.drop(columns=['level_0', 'index'], axis = 1, inplace=True, errors='ignore')
    conn.close()
          
    print("Data has been extracted successfully.")
    outputfile_name = constants.SCRIPTS_OUTPUT+'prediction_distribution_'+ datetime.now().strftime("%Y%m%d%H%M%S") +'.txt'
          
    output = input_data.groupby(['predicted_output']).size().reset_index(name='counts')
    count_0 = output[output['predicted_output'] == 0]
    count_0 = count_0['counts'][0]
    count_1 = output[output['predicted_output'] == 1]
    count_1 = count_1['counts'][1]

    result_1 = round((count_1/len(input_data.index))*100, 2)
    result_0 = round((count_0/len(input_data.index))*100, 2)
    data = {'is_churn':['0', '1'], 'percentage(%)':[result_0, result_1]}  
    result_df = pd.DataFrame(data)
    result_df.set_index(['is_churn'])
    result_df.to_csv(outputfile_name, header=None, index=None, sep='\t')

    print('Output file has been generated successfully ' + outputfile_name)
###############################################################################
# Define the function to check the columns of input features
# ##############################################################################


def input_features_check():
    '''
    This function checks whether all the input columns are present in our new
    data. This ensures the prediction pipeline doesn't break because of change in
    columns in input data.

    INPUTS
        db_file_name : Name of the database file
        db_path : path where the db file should be
        ONE_HOT_ENCODED_FEATURES: List of all the features which need to be present
        in our input data.

    OUTPUT
        It writes the output in a log file based on whether all the columns are present
        or not.
        1. If all the input columns are present then it logs - 'All the models input are present'
        2. Else it logs 'Some of the models inputs are missing'

    SAMPLE USAGE
        input_col_check()
    '''
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)
    input_data = pd.read_sql('select * from features', conn)
    input_data.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
    conn.close()
          
    source_cols = input_data.columns.to_list()
    print(source_cols)
    if collections.Counter(source_cols) == collections.Counter(constants.ONE_HOT_ENCODED_FEATURES):
        print('All the models input are present')
    else:
        print('Some of the models inputs are missing')
       

    ########################################################################################
#Common Functions
""
def store_data_to_db(db_path, db_file_name, input_data, table):
    conn = sqlite3.connect(db_path + db_file_name)
    input_data.to_sql(name=table, con=conn, if_exists='replace')
    print('input_data has been saved successfully to table ' + table);
    conn.close()
