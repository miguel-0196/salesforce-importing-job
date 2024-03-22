#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard libraries
import os
import json
import requests

# External libraries
import sqlite3
from dotenv import load_dotenv
from flask import Flask, request

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


# Flask app setup
app = Flask(__name__)
app.secret_key = os.urandom(24)

# Naive database setup
try:
    init_db_command()
except sqlite3.OperationalError:
    # Assume it's already been created
    pass


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


@app.route("/handle_importing_job")
def handle_importing_job():
    try:
        import handle_importing_job
        handle_importing_job.main()
        return 'OK'
    except Exception as err:
        return str(err), 405


if __name__ == '__main__':
    app.run(threaded=True, host=SERVICE_IP, port=SERVICE_PORT, ssl_context="adhoc")