"""
Echo device attribute changes via MQTT.

We use the paho library to show how to communicate with ThingsBoard via MQTT,
without the abstraction of the tb-mqtt-client library
(should be applicable to other coding languages).

```
python device/echo_attribute_updates.py --host demo.thingsboard.io --port 1883 --access-token YOUR_DEVICE_ACCESS_TOKEN
```

See also:
- https://pypi.org/project/paho-mqtt/#getting-started
"""

import json
from time import sleep
import argparse

import paho.mqtt.client as mqtt


# parse command line arguments
argparser = argparse.ArgumentParser()
argparser.add_argument("--host", type=str, help="MQTT broker host")
argparser.add_argument("--port", type=int, help="MQTT broker port")
argparser.add_argument("--access-token", type=str, help="Device access token")
args = argparser.parse_args()


# defaults
desired_attributes = ["test1", "test2", "test3"]


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("v1/devices/me/attributes")
    client.subscribe("v1/devices/me/attributes/response/+")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    global desired_attributes
    print("<<< " + msg.topic + " " + str(msg.payload))
    if msg.topic.startswith("v1/devices/me/attributes/response/"):
        desired_attributes = json.loads(msg.payload)["shared"]


def publish(client: mqtt.Client, topic: str, msg: str):
    print(">>> " + topic + " " + msg)
    client.publish(topic, msg)


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.enable_logger()
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.username_pw_set(args.access_token)

# simulate box
# requests must come from same mqtt session

sleep(1)
print()

print("--- test attribute changes: box offline ---")
print("changes some shared attributes on the device in the TB UI")
input("press any key to continue\n")

print("--- box connecting ---")
mqttc.connect(args.host, args.port, 60)
mqttc.loop_start()

sleep(2)
print()

print("--- startup: request box attributes ---")
keys = ",".join(desired_attributes)
publish(mqttc, "v1/devices/me/attributes/request/1", '{"sharedKeys": "' + keys + '"}')

sleep(2)
print()

print("--- test attribute changes: box online ---")
print("changes some shared attributes on the device in the TB UI")
input("press any key to continue\n")

print("--- done ---")
mqttc.loop_stop()
