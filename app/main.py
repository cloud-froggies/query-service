from typing import Optional, List
from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.param_functions import Query
from fastapi.responses import RedirectResponse
import json

import logging
import requests
import boto3
import os
import uuid
import datetime
import pymysql
import http.client

DB_ENDPOINT = os.environ.get('db_endpoint')
DB_ADMIN_USER = os.environ.get('db_admin_user')
DB_ADMIN_PASSWORD = os.environ.get('db_admin_password')
DB_NAME = os.environ.get('db_name')

app = FastAPI(title='Click Service', version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

tracking_click_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/tracking/click'

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

def get_db_conn():
    try:
        conn = pymysql.connect(host=DB_ENDPOINT, user=DB_ADMIN_USER, passwd=DB_ADMIN_PASSWORD, db=DB_NAME, connect_timeout=5)
        return conn
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        raise
    
@app.get("/")
def read_root():
    return {"Service": "Click"}

@app.get("/table")
def table():
    client = boto3.client('dynamodb')

    response = client.describe_table(
        TableName='sessions'
    )

    return {"Service": response}

# @app.get("/click", response_class=RedirectResponse, status_code=302)
# async def click(query_id: str, impression_id: str):

    # conn = get_db_conn()
    # now_timestamp_iso = datetime.datetime.now().isoformat()
    
    # # click
    # dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    # table = dynamodb.Table('sessions')
    # response = table.get_item(
    #     Key={
    #         'query_id': query_id,
    #         'impression_id': impression_id
    #     }
    # )
    # advertiser_url = response['Item']['advertiser_url']

    # # track click
    # click_id = str(uuid.uuid4())
    
    # # using db connection
    # with conn.cursor(pymysql.cursors.DictCursor) as cursor:
    #     sql_query = """SELECT campaign_id FROM ads WHERE advertiser_url = %s"""
    #     cursor.execute(sql_query, (advertiser_url))
    #     campaign_id = cursor.fetchone()
        
    #     sql_query = """SELECT advertiser_id FROM advertiser_campaigns WHERE id = %s"""
    #     cursor.execute(sql_query, (campaign_id))
    #     advertiser_id = cursor.fetchone()
        
    #     sql_query = """SELECT publisher_id FROM publisher_exclusions WHERE advertiser_id = %s"""
    #     cursor.execute(sql_query, (advertiser_id))
    #     publisher_id = cursor.fetchone()
        
    #     sql_query4 = """SELECT category FROM advertiser_campaigns WHERE id = %s"""
    #     cursor.execute(sql_query, (campaign_id))
    #     category = cursor.fetchone()
        
    #     sql_query = """SELECT id FROM ads WHERE url = %s"""
    #     cursor.execute(sql_query, (advertiser_url))
    #     ad_id = cursor.fetchone()
        
    #     sql_query6 = """SELECT zip_code FROM campaign_targeting WHERE campaign_id = %s"""
    #     cursor.execute(sql_query6, (campaign_id))
    #     zip_code = cursor.fetchone()
    
    # tracking_click_params = {
    #     "query_id": query_id,
    #     "impression_id": impression_id,
    #     "click_id": click_id,
    #     "timestamp": now_timestamp_iso,
    #     "publisher_id": publisher_id,
    #     "advertiser_id": advertiser_id,
    #     "advertiser_campaign_id": campaign_id,
    #     "category": category,
    #     "ad_id": ad_id,
    #     "zip_code": str(zip_code),
    #     "advertiser_price": 0.0,
    #     "publisher_price": 0.0,
    #     "position": 0
    # }
    
    # tracking_click_response = requests.get(tracking_click_endpoint, params=tracking_click_params)
    # logger.error(tracking_click_response)

    # return advertiser_url


@app.get("/click", response_class=RedirectResponse, status_code=302)
async def click(query_id: str, impression_id: str):
    try:
        conn = get_db_conn()
        now_timestamp_iso = datetime.datetime.now().isoformat()

        # track click
        click_id = str(uuid.uuid4())

        tracking_click_params_dummy = {
            "query_id": str(query_id),
            "impression_id": str('impression_id'),
            "click_id": click_id,
            "timestamp": str(now_timestamp_iso),
            "publisher_id": str(9999),
            "advertiser_id" : str(9999),
            "advertiser_campaign_id": str(9999),
            "category": str(9999),
            "ad_id": str(9999),
            "zip_code": str('0000'),
            "advertiser_price": str(99.99),
            "publisher_price": str(99.99),
            "position": str(9999)
        }
        
        conn = http.client.HTTPConnection(host=tracking_click_endpoint)
        headers = {'Content-type': 'application/json'}
        json_data = json.dumps(tracking_click_params_dummy)
        conn.request('POST', '/', json_data, headers)
        response = conn.getresponse()
        response.read().decode()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return 'Success post'
