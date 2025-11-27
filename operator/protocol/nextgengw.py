import paho.mqtt.client as paho
from paho.mqtt import client as mqtt_client
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes
import queue
from queue import Empty

import uuid
import json 
import time

client = mqtt_client.Client('far-edge-connection-provisioner', protocol=paho.MQTTv5)
queue = queue.Queue()

def on_message(client, userdata, message):
    userdata['message_queue'].put(message)

def connect(url, port):
    client.on_message = on_message
    client.user_data_set({
        'message_queue': queue
    })

    retries = 5
    while retries > 0:
        try:
            client.connect(url, int(port))
            break
        except ConnectionRefusedError:
            print("MQTT broker not ready. Retrying in 5 seconds...")
            time.sleep(5)

    if retries == 0:
        raise RuntimeError('Failed to connect')

    client.loop_start()
    print("Connected to NextGenGW")


def create_route(device, origin_uri, destination_uri, logger):
    route_id = 1000

    msg_queue = queue
    response_topic = str(uuid.uuid4().hex)

    publish_property = Properties(PacketTypes.PUBLISH)
    publish_property.ResponseTopic = response_topic
    client.subscribe(response_topic)
    
    try:
        # Get routes
        topic = device + "/Data_Route"
        data ='{"operation": "GET"}'
        client.publish(topic=topic, payload=data, properties=publish_property)
        try:
            response = json.loads(msg_queue.get(timeout=1).payload)
            if response['response_code'] == 69:
                route_ids = []

                for route in response['sdfObject']['Data_Route']:
                    route_ids.append(int(route['label']))
                    route_origin_uri = route['sdfProperty']['Origin_URI']
                    route_destination_uri = route['sdfProperty']['Destination_URI']

                    if route_origin_uri == origin_uri and route_destination_uri == destination_uri:
                        # Route already exists
                        logger.info("Route already exists")
                        return True

                # Increment ID if needed
                while route_id in route_ids:
                    route_id += 1
        except Empty:
            logger.error('Failed to get routes, empty?')
            pass

        # Create Instance
        data ='{"operation": "POST", "data": "{\\"label\\": \\"' + str(route_id) + '\\"}"}'
        client.publish(topic=topic, payload=data, properties=publish_property)
        response = json.loads(msg_queue.get(timeout=10).payload)
        if response['response_code'] != 65:
            raise RuntimeError('Failed to create instance')

        # Set data in Instance
        topic = device + "/Data_Route/" + str(route_id)
        data ='{"operation": "POST", "data": "{\\"sdfProperty\\":{\\"Origin_URI\\": \\"' + origin_uri + '\\",\\"Destination_URI\\": \\"' + destination_uri + '\\"}}"}'
        client.publish(topic=topic, payload=data, properties=publish_property)
        response = json.loads(msg_queue.get(timeout=10).payload)
        if response['response_code'] != 68:
            raise RuntimeError('Failed to create instance')

        return True

    except Exception as e:
        logger.error(e)
        return False

    finally:
        client.unsubscribe(response_topic)


def delete_route(device, origin_uri, destination_uri, logger):
    # Get routes
    response_topic = str(uuid.uuid4().hex)
    msg_queue = queue

    publish_property = Properties(PacketTypes.PUBLISH)
    publish_property.ResponseTopic = response_topic
    client.subscribe(response_topic)

    try:
        topic = device + "/Data_Route"
        data ='{"operation": "GET"}'
        client.publish(topic=topic, payload=data, properties=publish_property)
        response = json.loads(msg_queue.get(timeout=1).payload)
        if response['response_code'] != 69:
            raise RuntimeError('Failed to get routes')

        for route in response['sdfObject']['Data_Route']:
            route_id = route['label']
            route_origin_uri = route['sdfProperty']['Origin_URI']
            route_destination_uri = route['sdfProperty']['Destination_URI']

            if route_origin_uri == origin_uri and route_destination_uri == destination_uri:
                topic = device + "/Data_Route/" + str(route_id)
                data ='{"operation": "DELETE"}'
                client.publish(topic=topic, payload=data, properties=publish_property)
                response = json.loads(msg_queue.get(timeout=10).payload)
                if response['response_code'] != 66:
                    raise RuntimeError('Failed to delete route')
                
                return True
        
        raise RuntimeError('Failed to find route')

    except Exception as e:
        logger.error(e)
        return False

    finally:
        client.unsubscribe(response_topic)
