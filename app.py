#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard libraries
import os
import json
import requests
from datetime import datetime, timedelta

# External libraries
import sqlite3
from dotenv import load_dotenv
from flask import Flask, request, redirect
from simple_salesforce import Salesforce
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Local libraries
from db import init_db_command
from user_connection_table import User_connection_table
from importing_job_table import Importing_job_table


# Load environment variables
load_dotenv()
SERVICE_IP = os.getenv('SERVICE_IP') or '0.0.0.0'
SERVICE_PORT = os.getenv('SERVICE_PORT') or 4444
SALES_KEY = os.getenv('SALES_CLIENT_KEY')
SALES_SECRET = os.getenv('SALES_CLIENT_SECRET')
DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') or 'salesforce'
GOOGLE_CLOUD_PROJECT = os.getenv('GOOGLE_CLOUD_PROJECT')

# Flask app setup
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Naive database setup
try:
    init_db_command()
except sqlite3.OperationalError:
    # Assume it's already been created
    pass


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


# API routes
@app.post("/get_oauth_url")
def get_oauth_url():
    try:
        redirect_uri =  request.form['redirect_uri']
        url = "https://login.salesforce.com/services/oauth2/authorize?response_type=code&client_id="+SALES_KEY+"&redirect_uri="+redirect_uri
        return url
    except Exception as err:
        return str(err), 405


@app.post("/login_oauth_callback")
def login_oauth_callback():
    try:
        redirect_uri =  request.form['redirect_uri']
        authorization_code =  request.form['authorization_code']
        uri_token_request = 'https://login.salesforce.com/services/oauth2/token'
        ret = requests.post(uri_token_request, data={
            'grant_type': 'authorization_code',
            'code': authorization_code,
            'client_id': SALES_KEY,
            'client_secret': SALES_SECRET,
            'redirect_uri': redirect_uri
        }).json()

        if 'id' not in ret:
            return str(ret), 406

        priv = User_connection_table.get_token_info(ret['id'])
        if priv == None:
            User_connection_table.create(ret['id'], json.dumps(ret))
        else:
            User_connection_table.update_token_info(ret['id'], json.dumps(ret))
        return ret['id']
    except Exception as err:
        return str(err), 405


@app.post("/get_object_data")
def get_object_data():
    try:
        object_name = request.form['object_name']
        from_date = request.form['from_date'] if 'from_date' in request.form else '' # 2024-02-06
        to_date = request.form['to_date'] if 'to_date' in request.form else ''
        user_id = request.form['user_id']

        token_info = User_connection_table.get_token_info(user_id)
        token = json.loads(token_info)

        data, meta = get_salesforce_object_data(token['instance_url'], token['access_token'], object_name, from_date, to_date)
        return data

    except Exception as err:
        return str(err), 405
    

@app.post("/create_importing_job")
def create_importing_job():
    try:
        object_name = request.form['object_name']
        start_date = request.form['start_date']
        user_id = request.form['user_id']

        token_info = User_connection_table.get_token_info(user_id)
        token = json.loads(token_info)
        if token['id'] != user_id:
            return 'No user connection info', 406

        priv = Importing_job_table.get_row(user_id, object_name)
        if priv == None:
            Importing_job_table.create(user_id, object_name, start_date, start_date)
        else:
            Importing_job_table.update_row(user_id, object_name, start_date, start_date)
        return 'OK'
    except Exception as err:
        return str(err), 405
    

@app.post("/pause_importing_job")
def pause_importing_job():
    try:
        object_name = request.form['object_name']
        user_id = request.form['user_id']

        priv = Importing_job_table.get_row(user_id, object_name)
        if priv == None:
            return 'No record', 406
        else:
            Importing_job_table.update_row(user_id, object_name, priv[3], priv[4], 0)
        return 'OK'
    except Exception as err:
        return str(err), 405


@app.post("/resume_importing_job")
def resume_importing_job():
    try:
        object_name = request.form['object_name']
        user_id = request.form['user_id']

        priv = Importing_job_table.get_row(user_id, object_name)
        if priv == None:
            return 'No record', 406
        else:
            Importing_job_table.update_row(user_id, object_name, priv[3], priv[4], 1)
        return 'OK'
    except Exception as err:
        return str(err), 405


@app.post("/handle_importing_job")
def handle_importing_job():
    try:
        object_name = request.form['object_name']
        user_id = request.form['user_id']

        token_info = User_connection_table.get_token_info(user_id)
        token = json.loads(token_info)
        if token['id'] != user_id:
            return 'No user connection info', 406

        row = Importing_job_table.get_row(user_id, object_name)
        if row == None:
            return 'No importing job', 407
        
        if row[5] == 0:
            return 'The job is paused', 408

        today = datetime.datetime.now().date()
        yesterday = today + datetime.timedelta(days=-1)
        save_object_data_to_bigquery(token['instance_url'], token['access_token'], object_name, row[4], yesterday)
        Importing_job_table.update_row(user_id, object_name, row[3], datetime.strptime(today, '%Y-%m-%d'))
        return f"{user_id}: Imported {object_name} records from {row[4]} to {yesterday}."
    except Exception as err:
        return str(err), 405
    

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


if __name__ == '__main__':
    app.run(threaded=True, host=SERVICE_IP, port=SERVICE_PORT, ssl_context="adhoc")