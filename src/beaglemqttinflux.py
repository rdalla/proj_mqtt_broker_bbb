#!/usr/bin/env python3

"""A MQTT Broker
This script receives MQTT data and saves those to InfluxDB.
"""
import re
from typing import NamedTuple

import datetime

import paho.mqtt.client as mqtt
from influxdb import InfluxDBClient


# mqtt configuration
MQTT_ADDRESS = '192.168.1.101'
MQTT_USER = 'rdalla'
MQTT_PASSWORD = 'vao1ca'
MQTT_TOPIC = 'home/+/+' # [sensor]/[irms|kwh|cost]
MQTT_REGEX = 'home/([^/]+)/([^/]+)'
MQTT_CLIENT_ID = 'DallaValleMQTTBroker'

# influx configuration
ifuser = "grafana"
ifpass = "dallavalle"
ifdb   = "home"
ifhost = "192.168.1.101"
ifport = 8086
measurement_name = "smart_meter"

# connect to influx
ifclient = InfluxDBClient(ifhost,ifport,ifuser,ifpass,ifdb)

class SensorData(NamedTuple):
    location: str
    measurement: str
    value: float

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    #print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + msg.payload.decode())
    sensor_data = _parse_mqtt_message(msg.topic, msg.payload.decode('utf-8'))
    if sensor_data is not None:
        _send_sensor_data_to_influxdb(sensor_data)

def _parse_mqtt_message(topic, payload):
    match = re.match(MQTT_REGEX, topic)
    if match:
        location = match.group(1)
        measurement = match.group(2)
        return SensorData(location, measurement, float(payload))
    else:
        return None

# format the data as a single measurement for influx
def _send_sensor_data_to_influxdb(sensor_data):
    time = datetime.datetime.utcnow()
    json_body = [
        {
            'measurement': measurement_name,
            'time':time,
            'fields': {
                sensor_data.measurement: sensor_data.value
            }
        }
    ]
    ifclient.write_points(json_body)


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
