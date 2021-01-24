import asyncio
import io
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
import os
import sys
# import config

# Eventhub
EH_NAME = os.environ["EH_NAME"]
EH_CONNECTION_STR = os.environ["EH_CONNECTION_STR"]

async def send(data):
    # Create a producer client to send messages to the event hub.
    # Specify a connection string to your event hubs namespace and
    # the event hub name.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EH_CONNECTION_STR, eventhub_name=EH_NAME,
    )

    async with producer:
        # Create a batch.
        event_data_batch = await producer.create_batch()

        # Add events to the batch.
        event_data_batch.add(EventData(data))

        # Send the batch of events to the event hub.
        await producer.send_batch(event_data_batch)
