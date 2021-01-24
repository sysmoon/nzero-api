import os
import sys
# private modules
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))
#from protobuf import campaign_pb2
import campaign_pb2
from eventhub import send
import asyncio
from pymongo import MongoClient
import psycopg2
import psycopg2.extras
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import logging

# define
BLOB_CONNECTION_STR=os.getenv('BLOB_CONNECTION_STR')
MONGO_HOST=os.getenv('MONGO_HOST')
MONGO_PORT=int(os.getenv('MONGO_PORT', 27017))
MONGO_USERNAME=os.getenv('MONGO_USERNAME')
MONGO_PWD=os.getenv('MONGO_PWD')

POSTGRESQL_HOST=os.getenv('POSTGRESQL_HOST')
POSTGRESQL_PORT=os.getenv('POSTGRESQL_PORT', 5432)
POSTGRESQL_DBNAME=os.getenv('POSTGRESQL_DBNAME')
POSTGRESQL_USERNAME=os.getenv('POSTGRESQL_USERNAME')
POSTGRESQL_PWD=os.getenv('POSTGRESQL_PWD')

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

async def blobDownload(container_name, blob_dir, filename):
  try:
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STR)
    full_filename = '{}/{}'.format(blob_dir, filename)
    print('blobname={}'.format(full_filename))
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=full_filename)
    filebytes = blob_client.download_blob().readall()
  except Exception as e:
    logger.exception(e)
    return None
  else:
    return filebytes

async def getMatchedCampaignInfo(hdmap_id):
  try:
    conn_info = {
      'host': POSTGRESQL_HOST,
      'port': POSTGRESQL_PORT,
      'dbname': POSTGRESQL_DBNAME,
      'user': POSTGRESQL_USERNAME,
      'password': POSTGRESQL_PWD
    }

    postgreClient = psycopg2.connect(**conn_info)
    sql = '''
    select a.obj_key as hdmap_id, b.*
    from campaign_scenario a, campaign_blob_info b
    where a.obj_key = '{hdmap_id}' and a.campaign_id = b.campaign_id;
    '''.format(hdmap_id=hdmap_id)

    cursor = postgreClient.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(sql)
    results = cursor.fetchone()

    if results != None:
      return dict(results)

    return results
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
          "travel_cnt": {"$gt": 30}
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

      campaignpacket.category = candidate['cate']
      campaignpacket.attribute = int(candidate['attribute'])
      campaignpacket.x = candidate['x']
      campaignpacket.y = candidate['y']
      campaignpacket.z = candidate['z']
      campaignpacket.heading = candidate['heading']

      # get campaign image matched by hdmap_id
      campaigninfo = await getMatchedCampaignInfo(candidate['hdmap_id'])

      if campaigninfo != None: # image is option
        imagebytes = await blobDownload(
          container_name=campaigninfo['blob_container'],
          blob_dir=campaigninfo['blob_dir'],
          filename=campaigninfo['blob_file_nm']
        )

        if(imagebytes != None):
          campaignpacket.image.image_data = imagebytes
          campaignpacket.image.type = 'jpeg'
          campaignpacket.image.blob_container = campaigninfo['blob_container']
          campaignpacket.image.blob_dir = campaigninfo['blob_dir']
          campaignpacket.image.blob_file_nm = campaigninfo['blob_file_nm']

      serializeddata = campaignpacket.SerializeToString()
      logger.info(campaignpacket)
      await send(serializeddata)

async def main():
  serializeddata = await sendCandidate()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(main())
