# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = '/home/airflow/dags/Lead_scoring_data_pipeline/' 
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'
#DB_FILE_NAME="utils_output.db"
#DB_PATH = '/home/Assignment/01_data_pipeline/scripts/'
#UNIT_TEST_DB_FILE_NAME = 'utils_output.db'
DATA_DIRECTORY = '/home/Assignment/01_data_pipeline/notebooks/Data/leadscoring.csv'
INTERACTION_MAPPING = '/home/airflow/dags/Lead_scoring_data_pipeline/mapping/interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead']
NOT_FEATURES = []
LEAD_SCORING_FILE='/home/airflow/dags/Lead_scoring_data_pipeline/data/leadscoring_inference.csv'

