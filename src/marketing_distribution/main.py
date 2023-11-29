from utility.db import make_engine
from utility.utils import parse_envs, create_logger
from data_masking import detokenize, upload_output_file
import traceback

env_name, environment_secrets = parse_envs()
print('Environment is:', env_name)
print('environment_secrets:', environment_secrets)
DB_ENV: str = environment_secrets["DB_ENV"]
AWS_REGION: str = environment_secrets["AWS_REGION"]
QUEUE_NAME: str = environment_secrets["QUEUE_NAME"]
AWS_ACC_ID: str = environment_secrets["AWS_ACC_ID"]



logger = create_logger(logger_name="Marketing-Distribution", log_group_name="Marketing-Distribution")
# what are the column need in the output file, can we use patients_lifecylce view in select, First name and last names are tokenized
def execute():
    try:
        engine = make_engine(db_env=DB_ENV)
        conn = engine.connect()

        # De-Tokenize data
        detokenized_df = detokenize(conn)

        upload_output_file(detokenized_df)

        # Import the De-Tokenized data into table
        #import_data(detokn_data_df, engine)

    except Exception as e:
        logger.error(f"EXCEPTION! {e}")
        print(traceback.format_exc())


if __name__ == "__main__":
    execute()
