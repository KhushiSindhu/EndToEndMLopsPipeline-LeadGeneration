##############################################################################
# Import necessary modules and files

""


import pandas as pd
import os
import sqlite3
from sqlite3 import Error
import constants

# Importing city_tier_mapping
from city_tier_mapping import city_tier_mapping

# Import the list of significant levels of first_platform_c, first_utm_medium_c, and first_utm_source_c
from significant_categorical_level import list_platform, list_medium, list_source


###############################################################################
# Define the function to build database
# ##############################################################################
def build_dbs():
    """
    This function checks if the db file with specified name is present
    in the /Assignment/01_data_pipeline/scripts folder. If it is not present it creates
    the db file with the given name at the given path.


    INPUTS
         DB_FILE_NAME : Name of the database file 'utils_output.db'
         DB_PATH : path where the db file should exist

    OUTPUT
    The function returns the following under the given conditions:
         1. If the file exists at the specified path
                 prints 'DB Already Exists' and returns 'DB Exists'

         2. If the db file is not present at the specified loction
                 prints 'Creating Database' and creates the sqlite db
                 file at the specified path with the specified name and
                 once the db file is created prints 'New DB Created' and
                 returns 'DB created'


    SAMPLE USAGE
        build_dbs()
    """

    db_path = constants.DB_PATH + constants.DB_FILE_NAME  #  + constants.DB_FILE_NAME
    try:
        con = sqlite3.connect(db_path)
        print("DB Already Exists")
        return "DB Exists"
    except:
        print("Creating Database")
        con = sqlite3.connect(db_path)
        print("New DB Created")
        return "DB Created"


###############################################################################
# Define function to load the csv file to the database
# ##############################################################################

def load_data_into_db():
    """
    This function loads the data present in data directory into the db
    which was created previously.
    It also replaces any null values present in 'total_leads_droppped' and
    'referred_lead' columns with 0.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv'
                        file is present


    OUTPUT
        Saves the processed dataframe in the db in a table named 'loaded_data'.
        If the table with the same name already exsists then the function
        replaces it.


    SAMPLE USAGE
        load_data_into_db()
    """
    # Read the csv file
    data = pd.read_csv(constants.DATA_DIRECTORY)

    # Replace any null values in 'total_leads_dropped' and 'referred_lead' columns with 0
    data['total_leads_droppped'].fillna(0, inplace=True)
    data['referred_lead'].fillna(0, inplace=True)

    # Connect to the database
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)

    # Save the dataframe to the database
    data.to_sql('loaded_data', conn, if_exists='replace', index=False)

    # Commit the changes
    conn.commit()

    # Close the connection
    conn.close()

    print("Data Loaded Successfully into the DB!")


###############################################################################
# Define function to map cities to their respective tiers
# ##############################################################################


def map_city_tier():
    """
    This function maps all the cities to their respective tier as per the
    mappings provided in the city_tier_mapping.py file. If a
    particular city's tier isn't mapped(present) in the city_tier_mapping.py
    file then the function maps that particular city to 3.0 which represents
    tier-3.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be
        city_tier_mapping : a dictionary that maps the cities to their tier


    OUTPUT
        Saves the processed dataframe in the db in a table named
        'city_tier_mapped'. If the table with the same name already
        exsists then the function replaces it.


    SAMPLE USAGE
        map_city_tier()

    """
    # Connecting to the database
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)

    # Loading the data from loaded_data table
    df = pd.read_sql_query("SELECT * from loaded_data", conn)

    # Mapping city to its respective tier
    df['city_tier'] = df['city_mapped'].map(city_tier_mapping).fillna(3.0)
    
    df.drop(columns=['city_mapped','index'],axis=1,inplace=True,errors='ignore')
    
    # Replacing the existing city_tier_mapped table in the database
    df.to_sql("city_tier_mapped", conn, if_exists="replace", index=False)

    # Committing the changes
    conn.commit()

    # Closing the connection
    conn.close()

    print("City tier mapping complete!")


###############################################################################
# Define function to map insignificant categorial variables to "others"
# ##############################################################################


def map_categorical_vars():
    """
    This function maps all the insignificant variables present in 'first_platform_c'
    'first_utm_medium_c' and 'first_utm_source_c'. The list of significant variables
    should be stored in a python file in the 'significant_categorical_level.py'
    so that it can be imported as a variable in utils file.


    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        list_platform : list of all the significant platform.
        list_medium : list of all the significat medium
        list_source : list of all rhe significant source

        **NOTE : list_platform, list_medium & list_source are all constants and
                 must be stored in 'significant_categorical_level.py'
                 file. The significant levels are calculated by taking top 90
                 percentils of all the levels. For more information refer
                 'data_cleaning.ipynb' notebook.


    OUTPUT
        Saves the processed dataframe in the db in a table named
        'categorical_variables_mapped'. If the table with the same name already
        exsists then the function replaces it.


    SAMPLE USAGE
        map_categorical_vars()
    """
    # Connect to the database file
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)

    # Load the data from the 'city_tier_mapped' table into a dataframe
    df = pd.read_sql_query("SELECT * FROM city_tier_mapped", conn)

    # Map the insignificant levels of first_platform_c to 'Other'
    df['first_platform_c'] = df['first_platform_c'].apply(lambda x: x if x in list_platform else 'Other')

    # Map the insignificant levels of first_utm_medium_c to 'Other'
    df['first_utm_medium_c'] = df['first_utm_medium_c'].apply(lambda x: x if x in list_medium else 'Other')

    # Map the insignificant levels of first_utm_source_c to 'Other'
    df['first_utm_source_c'] = df['first_utm_source_c'].apply(lambda x: x if x in list_source else 'Other')

    # Save the processed dataframe in the database
    df.to_sql('categorical_variables_mapped', conn, if_exists='replace', index=False)
    print("categorical_variables_mapped table has been saved in the database")
    # Close the connection
    conn.close()


##############################################################################
# Define function that maps interaction columns into 4 types of interactions
# #############################################################################
def interactions_mapping():
    """
    This function maps the interaction columns into 4 unique interaction columns
    These mappings are present in 'interaction_mapping.csv' file.


    INPUTS
        DB_FILE_NAME: Name of the database file
        DB_PATH : path where the db file should be present
        INTERACTION_MAPPING : path to the csv file containing interaction's
                                   mappings
        INDEX_COLUMNS_TRAINING : list of columns to be used as index while pivoting and
                                 unpivoting during training
        INDEX_COLUMNS_INFERENCE: list of columns to be used as index while pivoting and
                                 unpivoting during inference
        NOT_FEATURES: Features which have less significance and needs to be dropped

        NOTE : Since while inference we will not have 'app_complete_flag' which is
        our label, we will have to exculde it from our features list. It is recommended
        that you use an if loop and check if 'app_complete_flag' is present in
        'categorical_variables_mapped' table and if it is present pass a list with
        'app_complete_flag' column, or else pass a list without 'app_complete_flag'
        column.


    OUTPUT
        Saves the processed dataframe in the db in a table named
        'interactions_mapped'. If the table with the same name already exsists then
        the function replaces it.

        It also drops all the features that are not requried for training model and
        writes it in a table named 'model_input'


    SAMPLE USAGE
        interactions_mapping()
    """
    conn = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)

    # Load the categorical_variables_mapped table from the database
    df_interactions = pd.read_sql_query("SELECT * FROM categorical_variables_mapped", conn)

    # remove duplicate rows
    df_interactions = df_interactions.drop_duplicates()

    # read the interaction mapping file
    df_interaction_mapping = pd.read_csv(constants.INTERACTION_MAPPING, index_col=[0])

    if 'app_complete_flag' in df_interactions.columns:
        # During training
        index_columns = constants.INDEX_COLUMNS_TRAINING
    else:
        # During Inference
        index_columns = constants.INDEX_COLUMNS_INFERENCE

    # Unpivot the 
    df_unpivot = pd.melt(df_interactions, id_vars=index_columns, var_name='interaction_type',
                         value_name='interaction_value')

    # map interaction type column with the mapping file to get interaction mapping
    df = pd.merge(df_unpivot, df_interaction_mapping, on='interaction_type', how='left')

    # dropping the interaction type column as it is not needed
    df = df.drop(['interaction_type'], axis=1)

    # pivoting the interaction mapping column values to individual columns in the dataset
    df_pivot = df.pivot_table(values='interaction_value', index=index_columns, columns='interaction_mapping',
                              aggfunc='sum')
    df_pivot = df_pivot.reset_index()

    # Save the pivot table to the database
    df_pivot.to_sql("interactions_mapped", conn, if_exists="replace")

    # During training
    if 'app_complete_flag' in df_interactions.columns:
        # copy the pivot table into model input df
        model_input = df_pivot

        # Drop the features that are not required for training the model
        model_input.drop(constants.NOT_FEATURES, axis=1, inplace=True)

        # Save the updated model input table to the database
        model_input.to_sql("model_input", conn, if_exists="replace")

    # Close the database connection
    conn.close()

    return "Interactions mapping completed successfully"
