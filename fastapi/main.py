from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


import aiokafka
import json
import schedule
import os
import logging
import asyncio
import uvicorn


# instantiate the API
app = FastAPI()


# declare static and templates folders
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# global variables
consumer_task = None
job_task = None
consumer = None
msg_list = []
initialize_json = b'{"messages": []}'


# env variables
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'topic_solver')
KAFKA_CONSUMER_GROUP = os.getenv('KAFKA_CONSUMER_GROUP', 'group-rest')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')


# initialize logger
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
log = logging.getLogger(__name__)


@app.on_event("startup")
async def startup_event():
    log.info('Initializing API ...')
    await initialize()
    await consume()
    await write()


@app.on_event("shutdown")
async def shutdown_event():
    log.info('Shutting down API')
    await consumer.stop()


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


async def initialize():
    global consumer
    log.debug(f'Initializing KafkaConsumer for topic {KAFKA_TOPIC}, group_id {KAFKA_CONSUMER_GROUP}'
              f' and using bootstrap servers {KAFKA_BOOTSTRAP_SERVERS}')
    consumer = aiokafka.AIOKafkaConsumer(KAFKA_TOPIC, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id=KAFKA_CONSUMER_GROUP)
    # get cluster layout and join group

    await consumer.start()


async def consume():
    global consumer_task
    consumer_task = asyncio.create_task(send_consumer_message(consumer))


async def send_consumer_message(consumer):
    global msg_list
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset, msg.key, msg.value, msg.timestamp)
            msg_value = msg.value
            msg_json = msg_value.decode('utf8').replace("'", '"')
            #print(msg_json)
            msg_list.append(msg_json)
        consumer.stop()
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        log.warning('Stopping consumer')
        await consumer.stop()


async def write():
    global job_task
    job_task = asyncio.create_task(write_consumer_data())


def job():
    global msg_list
    if len(msg_list) != 0:
        print("Writing data...")
        if os.path.isfile('data/data.json'):
            for i in msg_list:
                with open('data/data.json','r+') as file:
                    file_data = json.load(file)
                    data = json.loads(i)
                    file_data["messages"].append(data)
                    file.seek(0)
                    print(data)
                    json.dump(file_data, file, indent=4)
                file.close()
        else:
            with open('data/data.json','w') as file:
                data = json.loads(initialize_json)
                json.dump(data, file, indent=4)
            for i in msg_list:
                with open('data/data.json','r+') as file:
                    file_data = json.load(file)
                    data = json.loads(i)
                    file_data["messages"].append(data)
                    file.seek(0)
                    print(data)
                    json.dump(file_data, file, indent=4)
                file.close()
                
        msg_list = []
    else:
        print("No new data for writing...")


schedule.every(60).seconds.do(job)


async def write_consumer_data():
    while True:
        schedule.run_pending()
        await asyncio.sleep(1)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)