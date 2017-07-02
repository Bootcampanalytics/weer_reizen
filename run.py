import os
import json
import requests
from flask import Flask, render_template, request
from json2html import *
import ibm_db 
import pandas as pd
import time


app = Flask(__name__)

@app.route('/one_location/', methods = ['GET', 'POST'])
def serve_page():
	if request.method == 'GET':
		result=" "
		return render_template('one_location.html',result=result)
	if request.method == 'POST':
		loc = request.values.get('location')
		url='https://59760dd6-a040-4632-9482-0a822d6ca56b:R6E0gH4ydU@twcservice.eu-gb.mybluemix.net:443/api/weather'
		loc='/v3/location/search?query='+loc+'&locationType=city&countryCode=NL&language=en-US'
		loc=json.loads(requests.get(url+loc).text)
		loc=str(loc['location']['latitude'][0])+'/'+str(loc['location']['longitude'][0])
		query='/v1/geocode/'+loc+'/forecast/intraday/10day.json?units=m'
		result=json2html.convert(json.loads(requests.get(url+query).text))    
		return render_template('one_location.html',result=result)

@app.route('/collect_weather/', methods = ['GET', 'POST'])
def serve_page1():
	if request.method == 'GET':
		last_refresh=""
		return render_template('collect_weather.html',last_refresh=last_refresh)
	if request.method == 'POST':
		credentials_1={"host": "dashdb-entry-yp-lon02-01.services.eu-gb.bluemix.net","username": "dash8371" ,"password": "5tJ__CuJf2Li"}
		db2conn = ibm_db.connect("database=BLUDB;Hostname=" + credentials_1["host"] + ";Port=50000;PROTOCOL=TCPIP;UID=" + credentials_1["username"] + ";PWD=" + credentials_1["password"]+";","","")
		stmt = ibm_db.exec_immediate(db2conn,"TRUNCATE TABLE WEERDATA IMMEDIATE")
		
		locations = pd.DataFrame(columns=('GEMEENTE','PROVINCIE','LATITUDE','LONGITUDE','PC_COUNT'))
		stmt = ibm_db.exec_immediate(db2conn,"select * from LOCATIONS")
		result = ibm_db.fetch_assoc(stmt)
		
		row = ibm_db.fetch_tuple(stmt)
		while row != False:
		    locations.loc[len(locations)]=row
		    row = ibm_db.fetch_tuple(stmt)
		
		#locations=locations[:9]
		
		url='https://59760dd6-a040-4632-9482-0a822d6ca56b:R6E0gH4ydU@twcservice.eu-gb.mybluemix.net:443/api/weather'
		weather = pd.DataFrame(columns=('GEMEENTE','PROVINCIE','DAY_OF_WEEK', 'DAYPART_NAME', 'CLOUDPERC','RAINPROB','HUMIDITY','TEMP','WINDDIREC','WINDSPEED'))
		
		for index, row in locations.iterrows():
		    query='/v1/geocode/'+str(row['LATITUDE'])+'/'+str(row['LONGITUDE'])+'/forecast/intraday/10day.json?units=m'
		    result=json.loads(requests.get(url+query).text)
		    #time.sleep(1)
		    day1=result['forecasts'][0]['dow']
		    next_week=False
		    len_weather=len(weather)
		    for i in range(len(result['forecasts'])):
		        if (i>20 and result['forecasts'][i]['dow']==day1) or next_week:
		            dow=result['forecasts'][i]['dow']+'+1'
		            next_week=True
		        else:
		            dow=result['forecasts'][i]['dow']
		
		        weather.loc[i+len_weather]=[row['GEMEENTE'],
		                                    row['PROVINCIE'],
		                                    dow,
		                                    result['forecasts'][i]['daypart_name'],
		                                    result['forecasts'][i]['clds'],
		                                    result['forecasts'][i]['pop'],
		                                    result['forecasts'][i]['rh'],
		                                    result['forecasts'][i]['temp'],
		                                    result['forecasts'][i]['wdir_cardinal'],
		                                    result['forecasts'][i]['wspd']]
		          
		for index, row in weather.iterrows():
		    stmt = ibm_db.exec_immediate(db2conn,"INSERT INTO WEERDATA  VALUES ('"+ row[0]+"','"+ row[1]+"','"+row[2]+"','"+row[3]+"',"+str(row[4])+","+str(row[5])+","+str(row[6])+","+str(row[7])+",'"+row[8]+"',"+str(row[9])+")")
		ibm_db.close(db2conn)
		last_refresh='now'
		return render_template('collect_weather.html',last_refresh=last_refresh)

@app.route('/', methods = ['GET', 'POST'])

def serve_page_main():
	if request.method == 'GET':
		result1=""
		result2=""
		result3=""
		result4=""
		return render_template('index.html',result1=result1,result2=result2,result3=result3,result4=result4)
	if request.method == 'POST':	
		days = request.form.getlist('days') 
		time = request.form.getlist('time') 
		location = request.form.getlist('locatie')
		
		credentials_1={"host": "dashdb-entry-yp-lon02-01.services.eu-gb.bluemix.net","username": "dash8371" ,"password": "5tJ__CuJf2Li"}
		db2conn = ibm_db.connect("database=BLUDB;Hostname=" + credentials_1["host"] + ";Port=50000;PROTOCOL=TCPIP;UID=" + credentials_1["username"] + ";PWD=" + credentials_1["password"]+";","","")
		
		query1 ='SELECT T0.GEMEENTE AS "GEMEENTE",AVG(T0.CLOUDPERC) AS "CLOUD PERCENTAGE" FROM DASH8371.WEERDATA T0 WHERE ((T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 ASC FETCH FIRST 3 ROWS ONLY'
		query2 ='SELECT T0.GEMEENTE AS "GEMEENTE",AVG(T0.RAINPROB) AS "RAINPROB" FROM DASH8371.WEERDATA T0 WHERE ((T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 ASC FETCH FIRST 3 ROWS ONLY'
		query3 ='SELECT T0.GEMEENTE AS "GEMEENTE",AVG(T0.TEMP) AS "TEMP" FROM DASH8371.WEERDATA T0 WHERE ((T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 DESC FETCH FIRST 3 ROWS ONLY'
		query4 ='SELECT T0.GEMEENTE AS "GEMEENTE",AVG(T0.WINDSPEED) AS "WINDSPEED" FROM DASH8371.WEERDATA T0 WHERE ((T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 ASC FETCH FIRST 3 ROWS ONLY'
				
		stmt = ibm_db.exec_immediate(db2conn,query1)
		result1=[]
		row = ibm_db.fetch_tuple(stmt)
		while row != False:
		   result1.append(row)
		   row = ibm_db.fetch_tuple(stmt)

		stmt = ibm_db.exec_immediate(db2conn,query2)
		result2=[]
		row = ibm_db.fetch_tuple(stmt)
		while row != False:
		   result2.append(row)
		   row = ibm_db.fetch_tuple(stmt)
		   
		stmt = ibm_db.exec_immediate(db2conn,query3)
		result3=[]
		row = ibm_db.fetch_tuple(stmt)
		while row != False:
		   result3.append(row)
		   row = ibm_db.fetch_tuple(stmt)
		   
		stmt = ibm_db.exec_immediate(db2conn,query4)
		result4=[]
		row = ibm_db.fetch_tuple(stmt)
		while row != False:
		   result4.append(row)
		   row = ibm_db.fetch_tuple(stmt)
			
		ibm_db.close(db2conn)
		
		return render_template('index.html',result1=result1,result2=result2,result3=result3,result4=result4)
		
 
port = os.getenv('PORT', '5000')
if __name__ == "__main__":
	app.debug = True
	app.run(host='0.0.0.0', port=int(port))
