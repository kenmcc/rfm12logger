import sqlite3
#from pywws import DataStore
from pywws.DataStore import *
from datetime import *
import time


sensors = {"2": "TempPressureHumidity", "3": "RainWind", "10": "temp_kitchen", "21": "temp_bedroom"}
class weatherdata:
    def __init__(self):
        self.timestamp = None
        self.temp_out=None
        self.temp_in =None
        self.hum_out =None
        self.hum_in  =None
        self.pressure=None
        self.rain=None
        self.wind_avg=None
        self.wind_gust=None
        self.wind_dir=None




class dataLogger:
    def __init__(self, db):
        self.liveData = {}
        self.dbName = db
        self.conn = sqlite3.connect(db)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS data (date text DEFAULT CURRENT_TIMESTAMP, 
                                                           node int  NOT NULL, 
                                                           batt float NOT NULL, 
                                                           temp float DEFAULT x, 
                                                           humidity float DEFAULT x, 
                                                           pressure float DEFAULT x, 
                                                           rain float DEFAULT x, 
                                                           wind_dir int DEFAULT x,  
                                                           wind_avg float DEFAULT x,  
                                                           wind_gust float DEFAULT x, 
                                                           switch text DEFAULT x)''')
        self.conn.commit()
        self.weatherdata = weatherdata()

        selectStr = "SELECT * from data where node < 10 group by node order by date desc limit 10"
        data = self.c.execute(selectStr)
        names = list(map(lambda x: x[0], data.description))
        for row in data.fetchall():
            date = row[names.index("date")]
            if self.weatherdata.timestamp == None:
                self.weatherdata.timestamp = date
            node = row[names.index("node")]
            if node == 2:
                #temp, pressure, humidity#
                temp_out = row[names.index("temp")]
                pressure = row[names.index("pressure")]
                hum_out  = row[names.index("humidity")]
                if self.weatherdata.temp_out == None:
                    self.weatherdata.temp_out = temp_out
                if self.weatherdata.pressure == None:
                    self.weatherdata.pressure = pressure
                if self.weatherdata.hum_out == None:
                    self.weatherdata.hum_out = hum_out
            elif node == 3:
                rain  = row[names.index("rain")]
                if self.weatherdata.rain == None:
                    self.weatherdata.rain = rain
        print self.weatherdata

    def updatelive(self, value_list):
        for x in value_list:
            if x["field"] == "humidity":
                self.weatherdata.humidity = x["value"]
            elif x["field"] == "temp":
                self.weatherdata.temp_out = x["value"]
            elif x["field"] == "pressure":
                self.weatherdata.pressure = x["value"]
            elif x["field"] == "rain":
                self.weatherdata.rain = x["value"]
            else:
                print "ignoring", x["field"]
            
    def insert(self, value_list):
        fields = []
        values = []
        for d in value_list:
           fields.append(d["field"])
           values.append(d["value"])
        insert_string =  "INSERT INTO data ({0}) VALUES({1})".format(",".join(fields), ",".join(values))
        self.c.execute(insert_string)
        self.conn.commit()
        self.updatelive(value_list)
        
        
class fileDataLogger:
    def __init__(self, weatherdataDir):
        self.raw_data = data_store(weatherdataDir)
        self.current_data = self.raw_data.last_entry()
        print self.current_data, "\n-------------------------------------------\n"
        
    def insert(self, value_list):
        fields = []
        values = []
        for d in value_list:
           fields.append(d["field"])
           values.append(d["value"])
           
        new_data = dict(self.current_data)
        if "node" in fields:
            nodeId = int(values[fields.index("node")])
            battery = values[fields.index("batt")]
            
            if str(nodeId) in sensors:
                if nodeId == 2: # temp_out, pressure, humidity
                    temp = float(values[fields.index("temp")]) if "temp" in fields else 999
                    pressure = values[fields.index("pressure")] if "pressure" in fields else 0
                    humidity = values[fields.index("humidity")] if "humidity" in fields else -1
                    if temp > -10 and temp < 40:
                        new_data["temp_in"] = new_data["temp_out"] = temp
                    if pressure > 900 and pressure < 1100:
                        new_data["abs_pressure"] = pressure
                    if humidity > 0 and humidity < 100:
                        new_data["hum_in"] = new_data["hum_out"] = humidity
                        
                elif nodeId == 3: # rain
                    rain = float(values[fields.index("rain")]) if "rain" in fields else 999
                    if rain >= 0 and rain <= 0.3:
                        new_data["rain"] = round(new_data["rain"]+rain,2)
                        
                elif nodeId >= 10:
                    temp = float(values[fields.index("temp")]) if "temp" in fields else 999
                    if temp > -10 and temp < 40:
                        new_data[sensors[str(nodeId)]] = temp
                    
            else:
                print "Ignoring unknown node ", nodeId
        if new_data != self.current_data:
            justnow = datetime.now().replace(microsecond=0)
            timedifference = (justnow - self.current_data["idx"]).seconds
            self.current_data = new_data # store the current stuff
            if timedifference < 48:
                pass # this might go away - we're trying to limit the data to every 48 seconds or so as the weather station does
            else:
                self.current_data["idx"] = justnow
            self.raw_data[self.current_data["idx"]] = self.current_data
            
            
                
         
        
        
if __name__ == "__main__":
    
 fileLogger = fileDataLogger("/tmp/tempweather")
 fields = ({"field": "node", "value": str(2)}, 
                    {"field": "batt", "value": str(float(3000)/1000)}, 
                    {"field": "temp", "value": str(float(12000)/1000)},
                    {"field": "pressure", "value": str(float(991.9))},
                    {"field": "humidity", "value": str(float(8500)/100)})
 fileLogger.insert(fields)
 time.sleep(4)
 
 fields = ({"field": "node", "value": str(3)}, 
                    {"field": "batt", "value": str(float(2900)/1000)}, 
                    {"field": "rain", "value": str(float(30)/100)})
 fileLogger.insert(fields)
  
 time.sleep(4)
 
 fields = ({"field": "node", "value": str(10)}, 
                    {"field": "batt", "value": str(float(2910)/1000)}, 
                    {"field": "temp", "value": str(float(18000)/1000)})
 
 fileLogger.insert(fields)
 time.sleep(1)
 
 fields = ({"field": "node", "value": str(21)}, 
                    {"field": "batt", "value": str(float(2910)/1000)}, 
                    {"field": "temp", "value": str(float(14000)/1000)})
 
 fileLogger.insert(fields)

 
  
  #logger = dataLogger("/data/test.db")
  #logger.insert(({"field": "node", "value": "3"}, {"field": "batt", "value": "3000"}, {"field": "rain", "value": "18.3"}))
  #logger.insert(({"field": "node", "value": "2"}, {"field": "batt", "value": "3000"}, {"field": "humidity", "value": "98.3"}, {"field": "temp", "value": "17.6"}))

