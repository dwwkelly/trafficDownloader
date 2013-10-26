#!/usr/bin/env python

__author__ = "Devin Kelly"

import pymongo
import time
import json
from datetime import datetime
from tornado import httpclient, ioloop


def insertData(coll, data):

   try:
      if data['status'] != "OK":
         return
   except KeyError:
      print "corrupted data"
      return

   startAddr = data['destination_addresses']
   endAddr = data['origin_addresses']

   if coll.find({"startAddr": startAddr}).count() > 0:
      coll.insert({"startAddr": startAddr})

   if coll.find({"endAddr": endAddr}).count() > 0:
      coll.insert({"endAddr": endAddr})

   rows = data['rows']
   for row in rows:
      for element in row['elements']:
         commuteTime = element["duration"]["value"]
         timestamp = time.time()
         coll.insert({"commuteTime": commuteTime, "timestamp": timestamp})


def getWeekdayCommuteTimeFunction(coll, toAddr, fromAddr, startHour, endHour):
   toAddr = toAddr.replace(" ", "+")
   fromAddr = fromAddr.replace(" ", "+")
   url = "https://maps.googleapis.com/maps/api/distancematrix/json?origins={0}&destinations={1}&sensor=false&units=imperial".format(toAddr, fromAddr)

   def weekdayCommuteTime():
      now = time.time()
      dt = datetime.fromtimestamp(now)
      if dt.weekday() > 4:
         return

      if dt.hour < startHour or dt.hour > endHour:
         return

      http_client = httpclient.HTTPClient()
      print 'fetching'
      try:
         response = http_client.fetch(url)
         insertData(coll, json.loads(response.body))
      except httpclient.HTTPError as e:
         print "Error:", e
         http_client.close()

   return weekdayCommuteTime


def main():

   # Setup DB
   dbName = "traffic"
   cli = pymongo.MongoClient()
   db = cli[dbName]

   # Read Config File
   with open("trafficConfig.json") as fd:
      config = json.loads(fd.read())

   home = config["home"]
   work = config["work"]
   interval = config["interval_ms"]

   # Setup IO Loop
   callbacks = []
   io_loop = ioloop.IOLoop.instance()

   # morning commute
   startHour = 6
   endHour = 11
   coll = db["morning"]
   F1 = getWeekdayCommuteTimeFunction(coll, home, work, startHour, endHour)
   callbacks.append(ioloop.PeriodicCallback(F1, interval, io_loop))

   # afternoon commute
   startHour = 15
   endHour = 19
   coll = db["afternoon"]
   F2 = getWeekdayCommuteTimeFunction(coll, work, home, startHour, endHour)
   callbacks.append(ioloop.PeriodicCallback(F2, interval, io_loop))

   # Start callbacks
   [ii.start() for ii in callbacks]

   # Start IO Loop
   io_loop.start()

   return

if __name__ == "__main__":
   main()
