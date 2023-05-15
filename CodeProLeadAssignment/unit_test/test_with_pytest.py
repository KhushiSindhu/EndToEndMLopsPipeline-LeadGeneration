##############################################################################
# Import the necessary modules
# #############################################################################
import sqlite3
import pandas as pd
from constants import *
from utils import *
import collections 
###############################################################################
# Write test cases for load_data_into_db() function
# ##############################################################################

def test_load_data_into_db():
    """_summary_
    This function checks if the load_data_into_db function is working properly by
    comparing its output with test cases provided in the db in a table named
    'loaded_data_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_get_data()

    """
    build_dbs()
    load_data_into_db()
       
    source_df = read_data_from_database(DB_FILE_NAME, 'loaded_data') # Read the actual loaded data 
    target_df = read_data_from_database(UNIT_TEST_DB_FILE_NAME, 'loaded_data_test_case') #Read the Loaded test data
  
    source_df.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
   
    print(source_df.shape)
    print(target_df.shape)
   
    # Assert 
    verify_equality(source_df, target_df)  

    print("Test case passed!")

    

###############################################################################
# Write test cases for map_city_tier() function
# ##############################################################################
def test_map_city_tier():
    """_summary_
    This function checks if map_city_tier function is working properly by
    comparing its output with test cases provided in the db in a table named
    'city_tier_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_map_city_tier()

    """
    map_city_tier() #load data into database
    
    source_df = read_data_from_database(DB_FILE_NAME, 'city_tier_mapped')# Read the actual loaded data
    target_df = read_data_from_database(UNIT_TEST_DB_FILE_NAME, 'city_tier_mapped_test_case')#Read the Loaded test data
   
    source_df.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
    
    print(source_df.shape)
    print(target_df.shape)
    
    # Assert 
    verify_equality(source_df, target_df)  

    print("Test case passed!")

###############################################################################
# Write test cases for map_categorical_vars() function
# ##############################################################################    
def test_map_categorical_vars():
    """_summary_
    This function checks if map_cat_vars function is working properly by
    comparing its output with test cases provided in the db in a table named
    'categorical_variables_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'
    
    SAMPLE USAGE
        output=test_map_cat_vars()

    """    
    map_categorical_vars() #load data into database
    
    source_df = read_data_from_database(DB_FILE_NAME, 'categorical_variables_mapped')# Read the actual loaded data
    #Read the Loaded test data
    target_df = read_data_from_database(UNIT_TEST_DB_FILE_NAME, 'categorical_variables_mapped_test_case')

    source_df.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
    
    print(source_df.shape)
    print(target_df.shape)
    
    
    # Assert 
    verify_equality(source_df, target_df)  

    print("Test case passed!")

###############################################################################
# Write test cases for interactions_mapping() function
# ##############################################################################    
def test_interactions_mapping():
    """_summary_
    This function checks if test_column_mapping function is working properly by
    comparing its output with test cases provided in the db in a table named
    'interactions_mapped_test_case'

    INPUTS
        DB_FILE_NAME : Name of the database file 'utils_output.db'
        DB_PATH : path where the db file should be present
        UNIT_TEST_DB_FILE_NAME: Name of the test database file 'unit_test_cases.db'

    SAMPLE USAGE
        output=test_column_mapping()

    """ 
    interactions_mapping() #load data into database
    
    source_df = read_data_from_database(DB_FILE_NAME, 'interactions_mapped')# Read the actual loaded data
    
    #Read the Loaded test data
    target_df = read_data_from_database(UNIT_TEST_DB_FILE_NAME, 'interactions_mapped_test_case')
   
    source_df.drop(columns=['index'], axis = 1, inplace=True, errors='ignore')
    print(source_df.shape)
    print(target_df.shape)
    
    # Assert 
    verify_equality(source_df, target_df)  

    print("Test case passed!")
##############################################################################

###############################################################################
# Common functions
# ##############################################################################

def read_data_from_database(db_name, table_name):
    conn = sqlite3.connect(DB_PATH + db_name)
    df = pd.read_sql('select * from ' + table_name, conn)
    return df
    
def verify_equality(source_df, target_df):
    """
      Common function to verify source and target data frame
    """
    source_cols = source_df.columns.to_list()
    target_cols = target_df.columns.to_list()
    assert source_df.shape[0] == target_df.shape[0], "Number of rows in both dataframe are not same"
    assert source_df.shape[1] == target_df.shape[1], "Number of columns in both dataframe are not same"
    assert collections.Counter(source_cols) == collections.Counter(target_cols), "Column names do not match"
    

