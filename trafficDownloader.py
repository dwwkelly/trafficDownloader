#!/usr/bin/env python

__author__ = "Devin Kelly"

import pymongo
import time
import json
import re
import daemon
from datetime import datetime
from tornado import httpclient, ioloop


def parseHTML(htmlData):

   expr = re.compile("In current traffic: [0-9]{0,2} mins")
   matches = re.finditer(expr, htmlData)

   trafficData = []
   for ii in matches:
      tmpData = {}
      s = re.sub('<[^<]+?>', '', htmlData[ii.start(0): ii.start(0) + 180])
      s = re.sub("<.*$", '', s)
      (travelTime, route) = s.split('mins')

      route = re.sub("^\s*", "", route)
      route = re.sub("\s*$", "", route)
      tmpData["route"] = route

      travelTime = re.sub("^.*:\s*", "", travelTime)
      tmpData["time"] = travelTime

      trafficData.append(tmpData)

   return trafficData


def insertData(coll, data):

   timestamp = time.time()
   for trip in data:
      coll.insert({"commuteTime": trip['time'], "timestamp": timestamp, "route": trip['route']})


def getWeekdayCommuteTimeFunction(coll, toAddr, fromAddr, startHour, endHour):
   toAddr = toAddr.replace(" ", "+")
   fromAddr = fromAddr.replace(" ", "+")
   url = "https://maps.google.com/maps?saddr={0}&daddr={1}&hl=en".format(toAddr, fromAddr)

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
         trafficData = parseHTML(response.body)
         print trafficData
         insertData(coll, trafficData)
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
   endHour = 23
   coll = db["afternoon"]
   F2 = getWeekdayCommuteTimeFunction(coll, work, home, startHour, endHour)
   callbacks.append(ioloop.PeriodicCallback(F2, interval, io_loop))

   # Start callbacks
   [ii.start() for ii in callbacks]

   # Start IO Loop
   io_loop.start()

   return

if __name__ == "__main__":
   with daemon.DaemonContext():
      main()
