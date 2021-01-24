# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------------------------
# Created on Mon Jun 30 2020
# Copyright (c) 2020 SK TELECOM CO.
# Team: Mobility Labs
# Author: 문형권 (Daniel Moon)
# Email: sysmoon@sk.com
# --------------------------------------------------------------------------------------------

import asyncio
import os
import sys
from azure.eventhub.aio import EventHubConsumerClient
import io
import datetime
import json
from google.protobuf.json_format import MessageToJson
# private modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(os.path.dirname(__file__)))))
import campaign_pb2
# from src import config
from google.protobuf.json_format import MessageToJson
import logging

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# define
EVENTHUB_NAME = os.environ["EH_NAME"]
CONNECTION_STR = os.environ["EH_CONNECTION_STR"]
CONSUMER_GROUP = os.environ['EH_CONSUMER_GROUP']

KST = datetime.timezone(datetime.timedelta(hours=9))

async def on_event(partition_context, event):
    try:
        event_data_bytes = next(event.body)

        # Read object of DRODPacket
        campaignpacket = campaign_pb2.CampaignPacket()

        #event_data_bytes = next(event.body)
        campaignpacket.ParseFromString(event_data_bytes)

        # create directory by name (hdmap_id)
        if(campaignpacket.hdmap_id):
            # targetpath = '{basepath}/{blob_container}/{blob_dir}'.format(
            #     basepath='output',
            #     blob_container=campaignpacket.image.blob_container,
            #     blob_dir=campaignpacket.image.blob_dir,
            # )

            targetpath = '{basepath}/{type}/{hdmap_id}'.format(
                basepath='output',
                type=campaignpacket.type,
                hdmap_id=campaignpacket.hdmap_id
            )

            logger.info('tagetpath={}'.format(targetpath))
            os.makedirs(targetpath, exist_ok=True)

            jsonmsg = MessageToJson(campaignpacket)
            logger.info(jsonmsg)
            jsonobj = json.loads(jsonmsg)

            # save info.json
            if 'image' in jsonobj.keys():
                del jsonobj['image']

            # save candidate info to info.json
            with open('{}/info.json'.format(targetpath), 'w') as f:
                f.write(json.dumps(jsonobj))

            if campaignpacket.image.image_data:
                with open('{}/capture.jpg'.format(targetpath), 'wb') as f:
                    f.write(campaignpacket.image.image_data)
    except Exception as e:
        logger.exception(e)


async def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))


async def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


async def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))


async def main():
    try:
        arg_names = ['command', 'offset']
        args = dict(zip(arg_names, sys.argv))

        if 'offset' in args.keys():
            offset_split = args['offset'].split('/')
            start_position = datetime.datetime(int(offset_split[0]), int(offset_split[1]), int(offset_split[2]), 0, 0, 0, tzinfo=KST)
        else:
            start_position = '@latest'

        logger.info('start consuming from offset {}'.format(start_position))

        client = EventHubConsumerClient.from_connection_string(
            conn_str=CONNECTION_STR,
            consumer_group=CONSUMER_GROUP,
            eventhub_name=EVENTHUB_NAME
        )
        async with client:
            await client.receive(
                on_event=on_event,
                on_error=on_error,
                on_partition_close=on_partition_close,
                on_partition_initialize=on_partition_initialize,
                starting_position=start_position
                # starting_position="-1",  # "-1" is from the beginning of the partition.
                # starting_position="@latest",  # "-1" is from the beginning of the partition.
            )

        logger.info('exit')

    except Exception as e:
        logger.exception(e)
        logger.error('usage: python {} {}'.format(__file__, '2020/12/25'))

def get_epochtime_ms():
    return round(datetime.datetime.utcnow().timestamp() * 1000)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
