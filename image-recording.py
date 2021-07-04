#
# Mqttagent that record an image of the growing plants every 2 mins, 
# the camera is a simple ESP32 CAM, with an http jpg end point
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

import urllib3
import PIL.Image
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True


from  io import BytesIO

imagepath = os.path.dirname(os.path.realpath(__file__)) + "/image"
print("using image store at " + imagepath) 
if not os.path.exists(imagepath):
    os.mkdir(imagepath)

SERRE_STORE_ITEM = "serreimage"

config = configparser.RawConfigParser()


SERRE_TOPIC = "home/agents/serreimage"
CAMERA_IP = "http://192.168.4.57/jpg"


#############################################################
## MAIN

conffile = os.path.expanduser('~/.mqttagents.conf')
if not os.path.exists(conffile):
   raise Exception("config file " + conffile + " not found")

config.read(conffile)


username = config.get("agents","username")
password = config.get("agents","password")
mqttbroker = config.get("agents","mqttbroker")


client2 = mqtt.Client()

# client2 is used to send events to wifi connection in the house 
client2.username_pw_set(username, password)
client2.connect(mqttbroker, 1883, 60)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.

client2.loop_start()

def record_image():
    d = datetime.datetime.now()
    string_date = d.strftime("%Y%m%d%H%M%S")
    http = urllib3.PoolManager()
    r = http.request('GET',CAMERA_IP) 
    b = bytearray()
    b.extend(r.data)
    image = PIL.Image.open(BytesIO(b))
    imagefilename = os.path.join(imagepath,string_date + ".jpg")
    image.save(imagefilename)
    print("image saved : %s\n" % imagefilename)


while True:
   try:
      record_image()
      time.sleep(120) 
      client2.publish(SERRE_TOPIC + "/watchdog", "1")

   except Exception:
        traceback.print_exc()

