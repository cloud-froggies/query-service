# 2.22.0 es la version que deberia funcionar
from flask import Flask
from typing import Optional, List
import json
import logging
import requests
import boto3
import os
import uuid
import datetime
import pymysql
import http.client


app = Flask(__name__)

DB_ENDPOINT = os.environ.get('db_endpoint')
DB_ADMIN_USER = os.environ.get('db_admin_user')
DB_ADMIN_PASSWORD = os.environ.get('db_admin_password')
DB_NAME = os.environ.get('db_name')

query_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/query'
matching_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/matching'
exclusion_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/exclusion'
targeting_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/targeting'
ranking_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/ranking'
ads_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/ads'
pricing_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/pricing'
click_endpoint = 'http://public-18635190.us-east-2.elb.amazonaws.com/click'
tracking_query_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/tracking/query'
tracking_impression_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/tracking/impression'


def get_db_conn():
    try:
        conn = pymysql.connect(host=DB_ENDPOINT, user=DB_ADMIN_USER, passwd=DB_ADMIN_PASSWORD, db=DB_NAME, connect_timeout=5)
        return conn
    except pymysql.MySQLError as e:
        raise
    
    
def put_items(items):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-2')
    table = dynamodb.Table('sessions')

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(
                Item=item
            )
    return table


@app.route('/')
def read_root():
    table = put_items([])
    return {"Service": "Query", "table.creation_date_time": table.creation_date_time, 'tomorrow': datetime.datetime.now().timestamp() + (24*60*60)}


@app.get("/query")
def query(category: int, publisher: int, zip_code: int, maximum: int = None):
    try:
        now_timestamp = datetime.datetime.now()

        conn = get_db_conn()
        
        # matching
        matching_params = {"category": category}
        matching_response = requests.get(matching_endpoint, params=matching_params)
        matching_response.raise_for_status()
        campaign_ids = [campaign['id'] for campaign in matching_response.json()]
        advertiser_campaigns = ','.join(map(str, campaign_ids))

        # exclusion & targeting
        exclusion_params = {"advertiser_campaigns": advertiser_campaigns, 'publisher': publisher}
        exclusion_response = requests.get(exclusion_endpoint, exclusion_params)

        targeting_params = {"advertiser_campaigns": advertiser_campaigns, "zip_code": zip_code}
        targeting_response = requests.get(targeting_endpoint, targeting_params)

        valid_campaigns = list(set(exclusion_response.json()).intersection(set(targeting_response.json())))
        valid_advertiser_campaigns = ','.join(map(str, valid_campaigns))

        campaign_bids = [campaign['bid'] for campaign in matching_response.json() if (campaign['id'] in valid_campaigns)]
        campaign_bids = ','.join(map(str, campaign_bids))

        # ranking
        ranking_params = {"advertiser_campaigns": valid_advertiser_campaigns, "advertiser_campaigns_bids": campaign_bids}
        if(maximum):
            ranking_params["maximum"] = maximum

        ranking_response = requests.get(ranking_endpoint, params=ranking_params)

        # ads
        ads_params = {"advertiser_campaigns": valid_advertiser_campaigns}
        ads_response = requests.get(ads_endpoint, params=ads_params)

        # pricing
        pricing_params = {"advertiser_campaigns": valid_advertiser_campaigns, "advertiser_campaigns_bids": campaign_bids, "publisher": publisher}
        pricing_response = requests.get(pricing_endpoint, params=pricing_params)

        # query & tracking (impression events)
        query_id = str(uuid.uuid4())
        response_ads_aray = []
        dynamo_ads_array = []
        
        for index in range(0, len(ads_response.json())):
            ad = ads_response.json()[index]
            impression_id = str(uuid.uuid4())
            response_ads_aray.append({
                "impression_id": impression_id,
                "headline": ad["headline"],
                "description": ad["description"],
                "click_url": f'{click_endpoint}?query_id={query_id}&impression_id={impression_id}'
            })

            dynamo_ads_array.append({
                "query_id": query_id,
                "impression_id": impression_id,
                "advertiser_url": ad["url"],
                # hours * minutes * seconds
                "expdate": int(now_timestamp.timestamp() + (24*60*60))
            })

            # using list comprehension
            # publisher_price = [campaign for campaign in pricing_response if campaign['id']==ad['campaign_id']][0]['price']
            # advertiser_price = [campaign for campaign in matching_response if campaign['id']==ad['campaign_id']][0]['bid'] - publisher_price
            publisher_price = 0.0
            advertiser_price = 0.0

            # using db connection
            with conn.cursor(pymysql.cursors.DictCursor) as cursor:
                sql_query = """SELECT advertiser_id FROM advertiser_campaigns WHERE id = %s"""
                cursor.execute(sql_query,(ad['campaign_id']))
                advertiser_id = cursor.fetchone()
            
            tracking_impression_params = {
                "query_id":"6",
                "impression_id":"25",
                "timestamp":"2021-10-12T00:34:18.946089",
                "publisher_id":"1",
                "advertiser_id":"4",
                "advertiser_campaign_id":"88",
                "category":"2",
                "ad_id":"12",
                "zip_code":"222333",
                "advertiser_price":"30",
                "publisher_price":"90",
                "position":"16"
            }
            tracking_impression_response = requests.post(tracking_impression_endpoint, data=json.dumps(tracking_impression_params))
            tracking_impression_response.raise_for_status()


        query_response = {
            'headers': {
                'query_id': query_id
            },
            'ads': response_ads_aray
        }
        
        # cache query event
        dynamo_response = put_items(dynamo_ads_array)

        tracking_query_params = {
            "query_id":"123aadfassd",
            "timestamp":"2021-10-12T00:34:18.946089",
            "publisher_id":"2",
            "category":1,
            "zip_code":"abc123"
        }
        tracking_query_response = requests.post(tracking_query_endpoint, data=tracking_query_params)
        tracking_query_response.raise_for_status()

    except Exception as e:
        pass 
    
    return query_response


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')