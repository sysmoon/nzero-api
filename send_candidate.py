# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------------------------
# Created on Mon Jun 30 2020
# Copyright (c) 2020 SK TELECOM CO.
# Team: Mobility Labs
# Author: 문형권 (Daniel Moon)
# Email: sysmoon@sk.com
# --------------------------------------------------------------------------------------------

import os
import sys
import campaign_pb2
from eventhub import send
import asyncio
from pymongo import MongoClient
import psycopg2
import psycopg2.extras
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import logging
from datetime import date
from pyproj import Proj, transform

# define
BLOB_CONNECTION_STR=os.getenv('BLOB_CONNECTION_STR')
MONGO_HOST=os.getenv('MONGO_HOST')
MONGO_PORT=int(os.getenv('MONGO_PORT', 27017))
MONGO_USERNAME=os.getenv('MONGO_USERNAME')
MONGO_PWD=os.getenv('MONGO_PWD')

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# mongodb
mongoClient = MongoClient(
  host=MONGO_HOST,
  port=MONGO_PORT,
  username=MONGO_USERNAME,
  password=MONGO_PWD,
  ssl=True
)

async def getUtm2Wgs84(utm_x, utm_y):
    try:
        proj_utmk = Proj(init='epsg:32652')
        proj_wgs84 = Proj(init='epsg:4326')
        wgs_x, wgs_y = transform(proj_utmk, proj_wgs84, utm_x, utm_y)
        return (wgs_x, wgs_y)
    except Exception as e:
        print(e)

async def getAddCandidate():
  try:
    db = mongoClient['candidate']
    logger.info('mongodb connected')

    # query to get add_candidate which status is I(Insert) and trsfer_check lower than 1
    cursor = db.add_candidate.find({"status": 'I', "trsfer_chk": {"$lt": '1'}})

    return cursor
  except Exception as e:
    print(e)

async def getDelCandidate():

  try:
    db = mongoClient['candidate']
    logger.info('mongodb connected')

    cursor = db.del_candidate.aggregate([
      {
        "$match": {
          "travel_cnt": {"$gt": 60}
        }
      },
      {
        "$addFields": {
            "observe_rate":{"$divide": ["$observe_cnt", "$travel_cnt"]}
        }
      }
      ])

    return cursor
  except Exception as e:
    print(e)

async def sendCandidate():
  add_candidates = await getAddCandidate()
  del_candidates = await getDelCandidate()

  canidates = {
    'add': add_candidates,
    'del': del_candidates
  }

  today = date.today()
  todaystr = today.strftime('%Y-%m-%d')

  for type,candidates in canidates.items():
    logger.info('****************************')
    logger.info('start to sending {} candidate'.format(type))
    logger.info('****************************')
    for candidate in candidates:
      # exception
      # if type is del and observe_rate is greater than 30% do not send as a del_candidate.
      if(type == 'del'):
        if(candidate['observe_rate'] > 0.3):
          continue

      # create campaign packet using protobuf
      campaignpacket = campaign_pb2.CampaignPacket()

      campaignpacket.ver = '0.1'
      campaignpacket.type = type
      campaignpacket.hdmap_id = candidate['hdmap_id']

      if type == 'add':
        campaignpacket.dl_cnt = candidate['dl_cnt']
      elif type == 'del':
        campaignpacket.observe_rate = round(candidate['observe_rate'], 2)

      # coordinate projection utm52n > wgs84
      wgs_xy = getUtm2Wgs84(candidate['x'], candidate['y'])

      campaignpacket.category = candidate['cate']
      campaignpacket.attribute = int(candidate['attribute'])
      campaignpacket.x = wgs_xy[0]
      campaignpacket.y = wgs_xy[1]
      campaignpacket.z = candidate['z']
      campaignpacket.heading = candidate['heading']
      campaignpacket.date = todaystr

      logger.info(campaignpacket)
      serializeddata = campaignpacket.SerializeToString()
      await send(serializeddata)

async def main():
  serializeddata = await sendCandidate()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
