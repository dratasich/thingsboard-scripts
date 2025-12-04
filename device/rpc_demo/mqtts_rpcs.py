"""Connect to ThingsBoard with MQTTS as device and use custom RPCs."""

import argparse
import json

import paho.mqtt.client as mqtt

# parse command line arguments
argparser = argparse.ArgumentParser()
argparser.add_argument("--host", type=str, help="MQTT broker host")
argparser.add_argument("--port", type=int, help="MQTT broker port")
argparser.add_argument("--access-token", type=str, help="Device access token")
argparser.add_argument(
    "--rpc",
    type=str,
    choices=["getJobs", "checkoutJobs", "abortJobs", "uploadMeasurements"],
    help="RPC to call",
)
args = argparser.parse_args()


# code from paho-mqtt examples
# https://pypi.org/project/paho-mqtt/


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, reason_code, properties):  # noqa: ANN001, ANN201, ARG001, D103
    print(f"Connected with result code {reason_code}")
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # https://thingsboard.io/docs/reference/mqtt-api/
    client.subscribe("v1/devices/me/rpc/request/+")  # receive rpc requests
    client.subscribe("v1/devices/me/attributes")  # client+shared attributes updates
    client.subscribe("v1/devices/me/attributes/response/+")  # firmware updates
    client.subscribe("v1/devices/me/rpc/response/+")  # rpc responses


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):  # noqa: ANN001, ANN201, ARG001, D103
    print(msg.topic + " " + str(msg.payload))


mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqttc.on_connect = on_connect
mqttc.on_message = on_message

# basic auth (with access token)
mqttc.tls_set()
mqttc.username_pw_set(args.access_token)

mqttc.connect(args.host, args.port, 60)


# simulate some RPCs

if args.rpc == "getJobs":
    mqttc.publish("v1/devices/me/rpc/request/1", '{"method": "getJobs", "params": {}}')
    print("Requested getJobs RPC")

if args.rpc == "checkoutJobs":
    mqttc.publish(
        "v1/devices/me/rpc/request/1",
        '{"method": "checkoutJobs", "params": {"ids": ["iwr_Sammelauftrag_59139"]}}',
    )
    print("Requested checkoutJobs RPC")

if args.rpc == "abortJobs":
    mqttc.publish(
        "v1/devices/me/rpc/request/1",
        '{"method": "abortJobs", "params": {"ids": ["iwr_Sammelauftrag_59128"]}}',
    )
    print("Requested abortJobs RPC")

if args.rpc == "uploadMeasurements":
    with open("job.json") as f:  # noqa: PTH123
        job = f.read()
    rpc = {
        "method": "uploadMeasurements",
        "params": {
            "jobs": {"iwr_Sammelauftrag_59139": job},
        },
    }
    mqttc.publish("v1/devices/me/rpc/request/1", json.dumps(rpc))
    print("Requested uploadMeasurements RPC")

if args.rpc is None:
    print("No RPC requested, just connecting to see incoming messages")


# wait and exit

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqttc.loop_forever()
# press Ctrl+C to stop the script
