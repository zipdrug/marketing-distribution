from queries.sql import Patients_lifecylce_query
import requests
import csv
import pandas as pd
import os
import logging
from utility.utils import parse_envs, assume_role
from utility.getSecrets import SecretsManager
from botocore.exceptions import ClientError

def detokenize(conn):

    '''url = 'https://pty-api.awse1.ingenio-rx.com/static/unprotect/csv'
    user, password = 'SRC_PIP_ZipDrugNLZ', 'TyAuDvYf!jxSNeK8MCgR'
    '''

    env_name, environment_secrets = parse_envs()
    key_name = environment_secrets["PROTEGRITY"]
    zd_secrets = SecretsManager(secret_name=key_name)
    zd_secrets.transform_string()
    protegrity_secrets = zd_secrets.secret_dict
    csv_url = protegrity_secrets["csv_url"]
    username = protegrity_secrets["username"]
    password = protegrity_secrets["password"]
    headers = {'content-type': 'application/csv'}

    patient_df = pd.DataFrame(columns=['member_id', 'pk_key'])

    patient_lc_ext_df = pd.read_sql(sql=Patients_lifecylce_query, con=conn)

    patient_df["member_id"] = patient_lc_ext_df["member_ids"]
    patient_df["pk_key"] = patient_lc_ext_df["pk_key"]

    file_nm = "marketing-distribution_raw_data.csv"

    patient_df.to_csv(file_nm, sep=",", index=False)

    with open(file_nm, 'r') as file:
        data = file.read()

    response = requests.post(csv_url, auth=(username, password), verify=False, data=data, headers=headers)

    file_name = "marketing-distribution_detok_data.csv"

    # Store the De-Tokenize data in to CSV file.
    with open(file_name, "w") as f:
        f.write(response.text)

    data_df = pd.read_csv(file_name, delimiter=',').rename(columns=str.lower)

    merge_dt = pd.merge(patient_lc_ext_df, data_df, on="pk_key", how="inner").drop_duplicates()

    final_df = pd.DataFrame(columns=['Member ID', 'Patient Address State', 'Assigned Pharmacy ID', 'Plan Type', 'Program Eligibility', 'Patient ID', 'Pharmacy ID', 'Is Minor', 'Created Date'])

    final_df["Member ID"] = merge_dt["member_id"]
    final_df["Patient Address State"] = merge_dt["patient_address_state"]
    final_df["Assigned Pharmacy ID"] = merge_dt["assigned_pharmacy_id"]
    final_df["Plan Type"] = merge_dt["plan_type"]
    final_df["Program Eligibility"] = merge_dt["program_eligibility"]
    final_df["Patient ID"] = merge_dt["patient_id"]
    final_df["Pharmacy ID"] = merge_dt["potential_pharmacy_id"]
    final_df["Is Minor"] = merge_dt["is_minor"]
    final_df["Created Date"] = merge_dt["lead_created_date"]


    '''
    # Execute the source extract query
    conn.execute(Patients_lifecylce_query)

    # Write the source data in to CSV file
    with open("TokenizeIn.csv", "w") as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in conn.description])  # write headers
        csv_writer.writerows(conn)

    # Read the source data from CSV file.
    with open("TokenizeIn.csv", 'r') as file:
        data = file.read()

    # Post the source CSV data to De-Tokenize
    response = requests.post(csv_url, auth=(username, password), verify=False, data=data, headers=headers)

    # Store the De-Tokenize data in to CSV file.
    with open("DeTokenizeOut.csv", "w") as f:
        f.write(response.text)

    # Store the De-Tokenized data into DF.
    data_df = pd.read_csv("DeTokenizeOut.csv", delimiter=',').rename(columns=str.lower)
    '''

    return final_df

def upload_output_file(detokenized_df):

    final_outputfile = "marketing-distribution .csv"
    detokenized_df.to_csv(final_outputfile, sep=",", index=False)

    environment_secrets = parse_envs()
    bucket = environment_secrets["S3_ARCHIVE_BUCKET"]
    prefix = environment_secrets["S3_ARCHIVE_PREFIX"]

    patient_lc_object_name = prefix + os.path.basename(final_outputfile)
    upload_to_s3(final_outputfile, bucket, patient_lc_object_name)

def upload_to_s3(file_name, bucket, object_name):
    """
    :param file_name:
    :param bucket:
    :param object_name:
    :return:
    """
    new_session = assume_role()
    s3_bucket_name = bucket
    s3_bucket = new_session.resource("s3").Bucket(s3_bucket_name)

    try:
        s3_bucket.upload_file(Filename=file_name, Key=object_name)
        print("S3 Upload Successful")

    except ClientError as e:
        logging.error(e)
        return False

    return True

'''def import_data(data, engine):
    detok_df = data.rename(columns={"firstname": "first_name", "lastname": "last_name", "dob_mmddyyyy": "birthday", "pk_key": "id"})
    detok_df.to_sql(name="patients_lc_import", con=engine, if_exists="replace", index=False, method="multi")
'''
