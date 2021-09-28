from typing import Optional, List

from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.param_functions import Query

import logging 
import requests 
import boto3
import os
import uuid
import datetime

app = FastAPI(title='Query Service',version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

query_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/query'
matching_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/matching'
exclusion_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/exclusion'
targeting_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/targeting'
ranking_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/ranking'
ads_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/ads'
pricing_endpoint = 'http://internal-private-1191134035.us-east-2.elb.amazonaws.com/pricing'
click_endpoint = 'http://public-18635190.us-east-2.elb.amazonaws.com/click'


if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)


def put_items(items):
    # ACCESS_KEY = os.environ.get('ACCESS_KEY')
    # SECRET_KEY = os.environ.get('SECRET_KEY')
    # SESSION_TOKEN = os.environ.get('SESSION_TOKEN')

    # dynamodb_client = boto3.client(
    #     'dynamodb',
    #     aws_access_key_id=ACCESS_KEY,
    #     aws_secret_access_key=SECRET_KEY,
    #     aws_session_token=SESSION_TOKEN
    # )

    dynamodb = boto3.resource('dynamodb',region_name='us-east-2')
    table = dynamodb.Table('sessions')

    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(
                Item = item
            )
    return table


@app.get("/")
def read_root():
    table = put_items([])
    return {"Service": "Query", "table.creation_date_time" : table.creation_date_time,'tomorrow':datetime.datetime.now().timestamp() + (24*60*60)}


@app.get("/query")
async def query(category:int,publisher:int,zip_code:int,maximum:int=None):
        matching_params = {"category":category}
        matching_response = requests.get(matching_endpoint,params=matching_params)
        matching_response.raise_for_status()
        logger.error(matching_response)
        logger.error(matching_response.json())
        campaign_ids = [campaing['id'] for campaing in matching_response.json()]
        advertiser_campaigns = ','.join(map(str,campaign_ids))
        logger.error(advertiser_campaigns)

        exclusion_params = {"advertiser_campaigns": advertiser_campaigns,'publisher':publisher}
        exclusion_response = requests.get(exclusion_endpoint,exclusion_params)

        targeting_params = {"advertiser_campaigns": advertiser_campaigns,"zip_code":zip_code}
        targeting_response = requests.get(targeting_endpoint,targeting_params)

        logger.error(exclusion_response.json())
        logger.error(targeting_response.json())

        valid_campaigns = list(set(exclusion_response.json()).intersection(set(targeting_response.json())))
        valid_advertiser_campaigns = ','.join(map(str,valid_campaigns))
        logger.error(valid_campaigns)

        campaing_bids = [campaing['bid'] for campaing in matching_response.json() if (campaing['id'] in valid_campaigns)]
        campaing_bids = ','.join(map(str,campaing_bids))

        logger.error(valid_advertiser_campaigns)

        ranking_params = {"advertiser_campaigns":valid_advertiser_campaigns, "advertiser_campaigns_bids":campaing_bids}
        if(maximum):
            ranking_params["maximum"]=maximum
        
        ranking_response = requests.get(ranking_endpoint,params=ranking_params)

        logger.error(ranking_response.json())

        ads_params = {"advertiser_campaigns":valid_advertiser_campaigns}
        ads_response = requests.get(ads_endpoint,params=ads_params)

        logger.error(ads_response.json())

        pricing_params = {"advertiser_campaigns":valid_advertiser_campaigns, "advertiser_campaigns_bids":campaing_bids,"publisher":publisher}
        pricing_response = requests.get(pricing_endpoint,params=pricing_params)
        logger.error(pricing_response.json())

        query_id = str(uuid.uuid4())

        response_ads_aray = []
        dynamo_ads_array = []
        for ad in ads_response.json():
            impression_id = str(uuid.uuid4())
            response_ads_aray.append({
                "impression_id": impression_id,
                "headline": ad["headline"],
                "description": ad["description"],
                "click_url":f'{click_endpoint}?query_id={query_id}&impression_id={impression_id}'
            })

            dynamo_ads_array.append({
                "query_id":query_id,
                "impression_id": impression_id,
                "advertiser_url": ad["url"],
                "expdate": int(datetime.datetime.now().timestamp() + (24*60*60))#hours,minutes,seconds
            })
        
        dynamo_response = put_items(dynamo_ads_array)

        response = {
            'headers':{
                'query_id':query_id
                }
            ,
            'ads':response_ads_aray
        }

        return response


     


     


