# You can create more variables according to your project. The following are the basic variables that have been provided to you
#DB_PATH = 'Assignment/02_training_pipeline/notebooks/'
DB_FILE_NAME = 'utils_output.db'
DB_PATH = '/home/Assignment/01_data_pipeline/scripts/'
UNIT_TEST_DB_FILE_NAME = 'unit_test_cases.db'
DATA_DIRECTORY = '/home/Assignment/01_data_pipeline/scripts/leadscoring_test.csv'
INTERACTION_MAPPING = '/home/Assignment/01_data_pipeline/scripts/interaction_mapping.csv'
INDEX_COLUMNS_TRAINING = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead', 'app_complete_flag']
INDEX_COLUMNS_INFERENCE = ['created_date', 'first_platform_c',
       'first_utm_medium_c', 'first_utm_source_c', 'total_leads_droppped', 'city_tier',
       'referred_lead']
NOT_FEATURES = []

