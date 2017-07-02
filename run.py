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


@app.route('/', methods = ['GET', 'POST'])
def serve_page_main():

	credentials_1={"host": "dashdb-entry-yp-lon02-01.services.eu-gb.bluemix.net","username": "dash8371" ,"password": "5tJ__CuJf2Li"}
	db2conn = ibm_db.connect("database=BLUDB;Hostname=" + credentials_1["host"] + ";Port=50000;PROTOCOL=TCPIP;UID=" + credentials_1["username"] + ";PWD=" + credentials_1["password"]+";","","")

	query = 'SELECT MAX(T0."index") AS "index_Max" FROM DASH8371.COUNTER T0'
	stmt = ibm_db.exec_immediate(db2conn,query)
	latest = ibm_db.fetch_tuple(stmt)[0]
	
	query='SELECT T0."DAY_OF_WEEK" AS "DAY_OF_WEEK",AVG(CAST(T0.NUM AS FLOAT)) AS "NUM_Mean" FROM DASH8371."WEERDATA_V3" T0 WHERE (T0.LATEST ='+str(latest)+') GROUP BY T0."DAY_OF_WEEK"  ORDER BY 2 ASC'
	stmt = ibm_db.exec_immediate(db2conn,query)
	days_list=[]
	row = ibm_db.fetch_tuple(stmt)
	while row != False:
	   days_list.append(row[0]) 
	   row = ibm_db.fetch_tuple(stmt)  	   
		   
	if request.method == 'GET':
	
		result1=""
		result2=""
		result3=""
		result4=""
		return render_template('index.html',days=days_list,result1=result1,result2=result2,result3=result3,result4=result4)
		
	if request.method == 'POST':	
		days = request.form.getlist('days') 
		time = request.form.getlist('time') 
		location = request.form.getlist('locatie')
			
		query1 ='''SELECT T0.GEMEENTE AS "GEMEENTE",CAST(DECIMAL(AVG(T0.CLOUDPERC),2,0) AS CHAR(2)) CONCAT '%' AS "CLOUD PERCENTAGE" FROM DASH8371.WEERDATA_V3 T0 WHERE ((T0."LATEST"='''+str(latest)+') AND (T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 ASC FETCH FIRST 3 ROWS ONLY'
		query2 ='''SELECT T0.GEMEENTE AS "GEMEENTE",CAST(DECIMAL(AVG(T0.RAINPROB),2,0) AS CHAR(2)) CONCAT '%' AS "RAINPROB" FROM DASH8371.WEERDATA_V3 T0 WHERE ((T0.LATEST='''+str(latest)+') AND (T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 ASC FETCH FIRST 3 ROWS ONLY'
		query3 ='''SELECT T0.GEMEENTE AS "GEMEENTE",CAST(DECIMAL(AVG(T0.TEMP),2,0) AS CHAR(2)) CONCAT 'C' AS "TEMP" FROM DASH8371.WEERDATA_V3 T0 WHERE ((''' +str(latest)+') AND (T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 DESC FETCH FIRST 3 ROWS ONLY'
		query4 ='SELECT T0.GEMEENTE AS "GEMEENTE",DECIMAL((((((-8.9e-04 * POWER(DOUBLE(AVG(T0.WINDSPEED)) / NULLIF(10, 0), 4)) + (2.64e-02 * POWER(DOUBLE(AVG(T0.WINDSPEED)) / NULLIF(10, 0), 3))) - (2.942e-01 * POWER(DOUBLE(AVG(T0.WINDSPEED)) / NULLIF(10, 0), 2))) + (2.194e+00 * (DOUBLE(AVG(T0.WINDSPEED)) / NULLIF(10, 0)))) + 1.6639e-01),2,1) AS "WINDSPEED" FROM DASH8371.WEERDATA_V3 T0 WHERE ((T0.LATEST='+str(latest)+') AND (T0."DAY_OF_WEEK" IN (' + ', '.join("'{0}'".format(d) for d in days) + ')) AND (T0."DAYPART_NAME" IN (' + ', '.join("'{0}'".format(t) for t in time) +' )) AND (T0."PROVINCIE" IN (' + ', '.join("'{0}'".format(l) for l in location) +' ))) GROUP BY T0.GEMEENTE ORDER BY 2 ASC FETCH FIRST 3 ROWS ONLY'
				
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
		
		return render_template('index.html',days=days_list,result1=result1,result2=result2,result3=result3,result4=result4)
		
 
port = os.getenv('PORT', '5000')
if __name__ == "__main__":
	app.debug = True
	app.run(host='0.0.0.0', port=int(port))
