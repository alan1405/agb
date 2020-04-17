#!/usr/bin/python
import psycopg2
import json
import requests
db_port = 5432
db_name = "wmca"
db_user = 
db_pass = 

def lambda_handler(event, context):
    con = psycopg2.connect(
            host = "wmcapostgres.cok4j10mlpxt.eu-west-1.rds.amazonaws.com",
            database = db_name,
            user = db_user,
            password = db_pass)
    r = requests.get('https://traffic.ls.hereapi.com/traffic/6.2/flow.json/?apiKey=vRbDFfFW5VMv9bBcbjKDgRvwtQZO77W1RPZqaPE_kBI&bbox=52.6625,-1.4209;52.3465,-2.2072&locationreferences=shp')
    theflow = json.loads(r.text)
    i = theflow['RWS'][0]['RW']
    for x in i:
    	#print(x)
    	geom = x['FIS'][0]['FI'][0]['SHP'][0]['value']
    	fc = x['FIS'][0]['FI'][0]['SHP'][0]['FC']
    	lid = x['FIS'][0]['FI'][0]['SHP'][0]['LID']
    	le = x['FIS'][0]['FI'][0]['SHP'][0]['LE']
    	fw = x['FIS'][0]['FI'][0]['SHP'][0]['FW']
    	ty = x['FIS'][0]['FI'][0]['CF'][0]['TY']
    	sp = x['FIS'][0]['FI'][0]['CF'][0]['SP']
    	#print(x['FIS'][0]['FI'][0]['CF'][0]['SU'])
    	ff = x['FIS'][0]['FI'][0]['CF'][0]['FF']
    	jf = x['FIS'][0]['FI'][0]['CF'][0]['JF']
    	cn = x['FIS'][0]['FI'][0]['CF'][0]['CN']
    	mid = x['mid']
    	pbt = x['PBT']
    	
    	cur = con.cursor()
    	cur.execute("INSERT INTO wmca_road.wmca_here_traffic_flow(cn, fc, ff, fw, geom, jf, le, lid, mid, pbt, sp, su, ty) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(cn, fc, ff, fw, geom, jf, le, lid, mid, pbt, sp, 0, ty))
    	con.commit()
    cur.close()
    con.close()
    return theflow
