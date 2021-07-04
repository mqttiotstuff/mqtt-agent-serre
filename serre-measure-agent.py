#
# Mqttagent that monitor growing system
#
#   more information about store : https://medium.com/@aroussi/fast-data-store-for-pandas-time-series-data-using-pystore-89d9caeef4e2
#

import paho.mqtt.client as mqtt
import random
import time
import re
import configparser
import os.path
import json

import traceback

import pystore
from dataclasses import make_dataclass
import datetime
import pandas as pd

import os
import os.path
import numpy as np

storepath = os.path.dirname(os.path.realpath(__file__)) + "/store"
print("using store at " + storepath) 
if not os.path.exists(storepath):
    os.mkdir(storepath)

SERRE_STORE_ITEM = "serre2"

pystore.set_path(storepath)
store = pystore.store('serre_store')
# Access a collection (create it if not exist)
collection = store.collection(SERRE_STORE_ITEM)

SerreData = make_dataclass("SerreData", [("TIME", datetime.datetime), ("TEMPERATURE", float), ("LIGHT", float), 
          ("HUMIDITY", float), ("SOIL", float), ("LIGHTCMD", int), ("PUMPCMD", int)])



df = None
try:
    df = collection.item(SERRE_STORE_ITEM)
except ValueError:
    pass


config = configparser.RawConfigParser()


SERRE_TOPIC = "home/agents/serre"
ESP_SENSING_TOPIC = "home/esp50/sensors/serre"
ESP_LIGHT_TOPIC = "home/esp50/actuators/light"
ESP_PUMP_TOPIC = "home/esp50/actuators/pump"



#############################################################
## MAIN

conffile = os.path.expanduser('~/.mqttagents.conf')
if not os.path.exists(conffile):
   raise Exception("config file " + conffile + " not found")

config.read(conffile)


username = config.get("agents","username")
password = config.get("agents","password")
mqttbroker = config.get("agents","mqttbroker")

measures = SerreData(datetime.datetime.now(), np.nan, np.nan, np.nan, np.nan, np.nan, np.nan)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):

    global LEDSTRIPPATH
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(ESP_SENSING_TOPIC)
    client.subscribe(ESP_LIGHT_TOPIC)
    client.subscribe(ESP_PUMP_TOPIC)


def on_message(client, userdata, msg):
    global measures
    global df
    try:
        storeValue = False
        if msg.topic == ESP_SENSING_TOPIC:
            # get metrics
            s = msg.payload.decode("utf-8")
            v = s.split('|')
            for e in v:
                t = e.split(':')
                first = t[0]
                value = float(t[len(t)-1])
                setattr(measures, first, value)
            measures.TIME = datetime.datetime.now()
            storeValue = True

        elif msg.topic == ESP_PUMP_TOPIC and not msg.payload == None:
            if msg.payload != "":
                measures.PUMPCMD = int(msg.payload.decode("utf-8"))
                measures.TIME = datetime.datetime.now()
                storeValue = True
        elif msg.topic == ESP_LIGHT_TOPIC and not msg.payload == None:
            if msg.payload != "":
                measures.LIGHTCMD = int(msg.payload.decode("utf-8"))
                measures.TIME = datetime.datetime.now()
                storeValue = True
        print(measures)
        if storeValue:
            r = pd.DataFrame([measures], index=[measures.TIME])
            if df is None:
                collection.write(SERRE_STORE_ITEM, r, overwrite=True)
                df = r
            else:
                collection.append(SERRE_STORE_ITEM, r)

            try:
            # send to mqtt
                for i in measures.__dict__.keys():
                    v = getattr(measures, i)
                    if v != np.nan :
                        client.publish(SERRE_TOPIC + "/metrics/" + str(i), str(v))
            except Exception as e:
                traceback.print_exc()

        print(collection.item(SERRE_STORE_ITEM).data.tail())


    except Exception as e:
        traceback.print_exc()


client2 = mqtt.Client()

client2.on_connect = on_connect
client2.on_message = on_message

# client2 is used to send events to wifi connection in the house 
client2.username_pw_set(username, password)
client2.connect(mqttbroker, 1883, 60)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client2.loop_start()


lastvalue = None

while True:
   try:
      time.sleep(2) 
      client2.publish(SERRE_TOPIC + "/watchdog", "1")

   except Exception:
        traceback.print_exc()



