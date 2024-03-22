#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard libraries
import os
import json
import sqlite3
from datetime import datetime, timedelta

# External libraries
from dotenv import load_dotenv
from simple_salesforce import Salesforce
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Local libraries
from user_connection_table import User_connection_table
from importing_job_table import Importing_job_table

# Load environment variables
load_dotenv()
SALES_KEY = os.getenv('SALES_CLIENT_KEY')
SALES_SECRET = os.getenv('SALES_CLIENT_SECRET')
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') or 'salesforce'
GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# Util func
def get_salesforce_object_data(instance_url, access_token, object_name, from_date, to_date):
    sf = Salesforce(instance_url=instance_url, session_id=access_token)
    meta = getattr(sf, object_name).describe()
    fields = [field['name'] for field in meta['fields']]
    query = 'SELECT ' + ', '.join(fields)

    query += f' FROM {object_name} WHERE IsDeleted=False'
    if from_date != '':
        query += f' AND LastModifiedDate>={from_date}T00:00:00Z'

    if to_date != '':
        query += f' AND LastModifiedDate<={to_date}T23:59:59Z'
    
    return sf.query_all(query), meta
    

def save_object_data_to_bigquery(instance_url, access_token, object_name, from_date, to_date):
    data, meta = get_salesforce_object_data(instance_url, access_token, object_name, from_date, to_date)
    records = []
    for record in data['records']:
        r = {}
        for key, value in record.items():
            if key == 'attributes':
                continue
            if isinstance(value, dict):
                r[key] = str(value)
            else:
                r[key] = value
        records.append(r)

    # Connect bigquery
    client = bigquery.Client.from_service_account_json("./googleapis.json")
    dataset = client.dataset(DATASET_ID, project = GOOGLE_CLOUD_PROJECT)
    table_ = dataset.table(object_name)
    schema = []
    gsql_types = 'ARRAY, BIGNUMERIC, BOOL, BYTES, DATE, DATETIME, FLOAT64, GEOGRAPHY, INT64, INTERVAL, JSON, NUMERIC, RANGE, STRING, STRUCT, TIME, TIMESTAMP,'
    for field in meta['fields']:
        type = field['type'].upper()
        if gsql_types.find(type + ',') == -1 or type == 'DATETIME':
            type = 'STRING'
        schema.append(bigquery.SchemaField(field['name'], type))

    # Get a table
    try:
        table = client.get_table(table_)
    except NotFound:
        table = bigquery.Table(table_, schema=schema)
        table.clustering_field=['OwnerId']
        table = client.create_table(table)

    # Save data
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema = schema
    return str(client.load_table_from_json(records, table, job_config=job_config).result().job_id)


def main():
    db = sqlite3.connect("sqlite_db", detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row
    rows = Importing_job_table.get_jobs(db)
    for row in rows:
        try:
            token_info = User_connection_table.get_token_info(row[0], db)
            token = json.loads(token_info)
            if token['id'] != row[0]:
                print('TOKEN ERROR: No connection info for user', row[0])
                continue

            today = datetime.now().date()
            yesterday = today + timedelta(days=-1)
            save_object_data_to_bigquery(token['instance_url'], token['access_token'], row[1], row[2], yesterday)
            print(f"{row[0]}: Imported {row[1]} records from {row[2]} to {yesterday}.")
            Importing_job_table.update_last_date(db, row[0], row[1], today.strftime('%Y-%m-%d'))
        except Exception as err:
            print("LOOP ERROR:", err)
    db.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print("MAIN ERROR:", err)
        exit(1)