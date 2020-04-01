import datetime
import json
import connexion
import yaml
import logging.config
from connexion import NoContent
import requests
from pykafka import KafkaClient

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


# Your function here
def add_inventory(inventoryReading):
    logger.info("Add Inventory")
    # r = requests.post('http://0.0.0.0:8090/report/inventory', data=Inventory(inventoryReading['item_id'],
    #                                                                          inventoryReading['name'],
    #                                                                          inventoryReading['manufacturer'],
    #                                                                          inventoryReading['warehouse']))
    client = KafkaClient(hosts="{}:{}".format(app_config["kafka"]["domain"], app_config["kafka"]["port"]))
    topic = client.topics[app_config["kafka"]["topic"]]
    producer = topic.get_sync_producer()
    msg = {
        "type": "inventory",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": inventoryReading
    }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    return NoContent, 201


def add_status(itemStatus):
    logger.info("Add Status")
    # r = requests.post('http://0.0.0.0:8090/report/status', data=Status(itemStatus['item_id'],
    #                                                                    itemStatus['status'],
    #                                                                    itemStatus['destination'],
    #                                                                    itemStatus['deliverydate']))

    client = KafkaClient(hosts="{}:{}".format(app_config["kafka"]["domain"], app_config["kafka"]["port"]))
    topic = client.topics[app_config["kafka"]["topic"]]
    producer = topic.get_sync_producer()
    msg = {
        "type": "status",
        "datetime": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": itemStatus
    }
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml")


if __name__ == "__main__":
    app.run(port=8080)
