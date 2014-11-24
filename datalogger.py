import sqlite3

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
        
if __name__ == "__main__":
  logger = dataLogger("/data/test.db")
  logger.insert(({"field": "node", "value": "3"}, {"field": "batt", "value": "3000"}, {"field": "rain", "value": "18.3"}))
  logger.insert(({"field": "node", "value": "2"}, {"field": "batt", "value": "3000"}, {"field": "humidity", "value": "98.3"}, {"field": "temp", "value": "17.6"}))

