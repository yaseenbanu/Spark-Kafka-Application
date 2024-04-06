import requests
import json
import random
import time
from kafka import KafkaProducer

TOPIC_NAME = "PRODUCER_TOPIC"

def get_json_string():
    json_data = {
        "col_a": "",
        "col_b": "",
        "col_c": "",
        "col_d": ""
    }
    return json_data


def json_ser(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=json_ser)


if __name__ == "__main__":

    number = 0

    while True:
        data = get_json_string()

        data["col_a"] = str(number)
        data["col_b"] = str(random.randint(1, 3))
        data["col_c"] = str(random.random())
        data["col_d"] = random.choice(["a", "b"])

        data = json.dumps(data)

        number = number + 1
        print(f'Sending the Data to consumer...{data}')
        producer.send(TOPIC_NAME, data)
        producer.flush()
        time.sleep(10)

