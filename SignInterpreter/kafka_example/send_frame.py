#import pickle
from aiokafka import AIOKafkaProducer
import json
from json import JSONEncoder
import numpy as np
from kafka_example.config import BOOTSTRAP_SERVERS, TOPIC
import asyncio
from predict import extraerkeypoints
from kafka_example.client import get_producer, DEFAULT_TOPIC


global producer

class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

def serializer(value):

    data = {
        "msg": f"Hola",
        "send_to": list(value) # ["telegram"],["slack"]
    }
    try:

        print("!!!!!!!!!!"+data)
    except Exception as e:
        print("error pintandooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
        print(e)
    # https://aiokafka.readthedocs.io/en/stable/producer.html
    try:
        serialized = bytearray(value, "utf-8")
    except Exception as e:
        print("error seriasjajsasssssfafadsfasdgasdgasdgasdgasjdgasj khja kfjnhakld jnkjnñjfñlb")
        print(e)

    return serialized
    #return pickle.dumps(value)
    #return value
     # a 2 by 5 array
    #lista = value.tolist()
    #numpyData = {"array": value}
    #return bytearray(json.dumps(numpyData,cls=NumpyArrayEncoder),"utf-8")
    # result = {'name' : 'point', 'value':value.tolist()}# podria valer pero es muy largo el array y da error _producer_magic
    # return json.dumps(result).encode()

      #serialized = bytearray(json.dumps(data), "utf-8")


async def connect_kafka():
    global producer
    print(f"Connecting to topic {DEFAULT_TOPIC}...")
            # producer = AIOKafkaProducer(
            # bootstrap_servers=BOOTSTRAP_SERVERS,  # Our Kafka Connection
            # value_serializer=serializer)  # So JSON can be sent as messages
    try:
        producer = get_producer()
    except Exception as e:
        print("error generando")
        print(e)
    await producer.start()
    print(f"Kafka ready {BOOTSTRAP_SERVERS}")


async def local_send_frame(keypoints):
    print("Sending frame")
    try:
        meta = await producer.send_and_wait(DEFAULT_TOPIC, keypoints)
        print (meta)
    except Exception as e:
        print(e)
    print("Frame sent to kafkaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")


def send_frame_kafka(img,holistic):
    #frame_data = img.to_ndarray()

    # Extract keypoints
    # ...
    keypoints = extraerkeypoints(img,holistic)
    print(keypoints.shape)
    # Run the task
    try:
        loop = asyncio.get_running_loop()
    #####task = loop.create_task(local_send_frame(frame_data))
        task = loop.create_task(local_send_frame(keypoints))
    # print(task)
    except Exception as e:
        print(e)
    print(f"Frame keypoints sent to kafka topic {DEFAULT_TOPIC}")
