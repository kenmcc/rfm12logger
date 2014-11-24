import struct
import urllib
import os
import datetime
from datalogger import *
import array, fcntl, sys

fd = os.open("/dev/rfm12b.0.1",  os.O_NONBLOCK|os.O_RDWR)
localURI="http://192.168.1.31/emoncms/input/post.json?apikey=d959950e0385107e37e2457db27b781e&node="
remoteURI="http://emoncms.org/input/post.json?apikey=a6958b2d85dfdfab9406e1e786e38249&node="
goatstownURI="http://goatstownweather.hostoi.com/emoncms/input/post.json?apikey=0f170b829035f4cde06637f953852333&node="

if len(sys.argv)== 2:
   if sys.argv[1] == "9600":
     print "Setting baudrate to 9600"
     buf = array.array('h',[0x23])
     fcntl.ioctl(fd, 1074033160, buf, 1)

     fcntl.ioctl(fd, -2147192317, buf, 1)
     if buf[0] == 35:
        print "Successfully"

logger = dataLogger("/data/weather.db")

run=True
while run == True:
   try:
	data=None
	jsonStr= ""
	data = os.read(fd, 66)
	node, len = struct.unpack("BB", data[:2])
	now=datetime.datetime.now().strftime("%Y%m%d_%H:%M")
	#print now, node, len
	if node == 2:
          temp, batt, pressure, humidity = struct.unpack("hhii", data[2:])
          fields = ({"field": "node", "value": str(node)}, 
                    {"field": "batt", "value": str(float(batt)/1000)}, 
                    {"field": "temp", "value": str(float(temp)/1000)},
                    {"field": "pressure", "value": str(float(pressure))},
                    {"field": "humidity", "value": str(float(humidity)/100)})
	  logger.insert(fields)
          print "Temp {0}, batt {1}, pressure {2}, humidity{3}".format(temp, batt, pressure, humidity)

	elif node == 3 and len == 10:
	  rain,batt,a,b,c = struct.unpack("hhhhh", data[2:])
          fields = ({"field": "node", "value": str(node)}, 
                    {"field": "batt", "value": str(float(batt)/1000)}, 
                    {"field": "rain", "value": str(float(rain)/100)})
	  logger.insert(fields)
          print "Rain {0}, batt {1}".format(rain, batt)

	elif node >= 10 and node < 20 and len == 4:
		temp,batt = struct.unpack("hh", data[2:])
		if temp > -2000 and temp < 4000:
		  print "Got node, temp, batt = ", node, temp, batt
                  fields = ({"field": "node", "value": str(node)}, 
                            {"field": "batt", "value": str(float(batt)/1000)}, 
                            {"field": "temp", "value": str(float(temp)/100)})
		  logger.insert(fields)
      	          jsonStr = "temp:"+str(float(temp/100.0))+",batt:"+str(float(batt/1000.0))
	elif node == 20 and len == 8: # this is a pressure sensor 
          print "pressure sensor"
	  temp, batt, pressure = struct.unpack("hhi", data[2:])
	  if temp >-2000 and temp < 40000 and pressure >900 and pressure < 1100:
	 	print "Got temp, battery, pressure = ", temp, batt, pressure
		jsonStr = "temp:"+str(float(temp/10.0))
	  	jsonStr += ",batt:"+str(float(batt/1000.0))
          	jsonStr += ",pressure:"+str(pressure)  
	elif node == 21 and len == 5:
	    temp, batt, other = struct.unpack("hhb", data[2:])
	    print node, temp, batt, other
            switchstat = ["'OFF'", "'ON'", "'STAY'"]
            fields = ({"field": "node", "value": str(node)}, 
                      {"field": "batt", "value": str(float(batt)/1000)}, 
                      {"field": "temp", "value": str(float(temp)/100)},
                      {"field": "switch", "value": str(switchstat[int(other)])})
            try:
	      logger.insert(fields)
            except:
              pass

	else:
	  print "don't know what to do with node, len", node, len
          if node == 3 and len == 10:
		rain, batt, a, b, c = struct.unpack("hhhhh", data[2:])
          	print "Batt = ", batt, "rain=", rain		

   except Exception, e:
    	print "Exception ", e
os.close(fd)
