#!/usr/bin/env python3

"""A MQTT Broker
This script receives MQTT data and saves those to JSON file.
"""

import paho.mqtt.client as mqtt

MQTT_ADDRESS = '192.168.1.101'
MQTT_USER = 'rdalla'
MQTT_PASSWORD = 'vao1ca'
MQTT_TOPIC = '/home'
MQTT_CLIENT_ID = 'DallaValleMQTTBroker'

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    #print(msg.topic + ' ' + str(msg.payload))
    path = '/home/debian/data.json'
    #data_file = open(path,'a')
    with open(path, 'a', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    #data_file.write(msg.payload)
    #data_file.write(str(msg.payload))
    #print(str(msg.payload))
    #data_file.close()

def main():
    mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('DallaValle MQTT Broker')
    main()
