from typing import Optional,List

from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from fastapi.param_functions import Query

import logging 
import requests 

app = FastAPI(title='Query Service',version='0.1')
gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
logger.setLevel(gunicorn_logger.level)

query_endpoint = 'http://18.222.133.9:80/query'
matching_endpoint = 'http://18.222.133.9:81/matching'
exclusion_endpoint = 'http://18.222.133.9:82/exclusion'
targeting_endpoint = 'http://18.222.133.9:83/targeting'
ranking_endpoint = 'http://18.222.133.9:84/ranking'
ads_endpoint = 'http://18.222.133.9:85/ads'
pricing_endpoint = 'http://18.222.133.9:86/pricing'

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)


@app.get("/")
def read_root():
    return {"Service": "Query"}


@app.get("/query")
async def query(category:int,publisher:int,zip_code:int,maximum:int=None):
    # try:
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

        ads_aray = []
        for ad in ads_response.json():
            ads_aray.append({
                "headline": ad["headline"],
                "description": ad["description"],
                "url":ad["url"]
            })

        return {'ads':ads_aray}
    # except: 
    #     raise HTTPException(status_code=404, detail= f'No se encontraron anuncios {category}') 
 


     


     


