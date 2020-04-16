#!/usr/bin/python
import psycopg2
import json
import requests
from datetime import datetime
from xml.etree import ElementTree

db_port = 5432
db_name = "wmca"
db_user = "fme_server_write"
db_pass = "tomcatFileScript43"
def lambda_handler(event, context):
    r = requests.get('https://publicdashacc.blob.core.windows.net/publicdata?restype=container&comp=list')
    # root address for the JSON GeoJSON etc https://c19pub.azureedge.net/
    #rtheflow = r.json()

    date_now = datetime.now()
    db_date = date_now.strftime("%d/%m/%Y %H:%M:%S")
    #print(db_date)

    root = ElementTree.fromstring(r.content)
    print(dir(root))
    
    jsonloc = []
    for child in root.findall('.//Name'):
        #print(child.text)
        jsonloc.append(child.text)
    jsonloc.sort(reverse=True)
    
    jsonloc.pop(0)
    jsonloc.pop(0)
    jsonloc.pop()
    
    jsonloc.sort(reverse=True)
    
    #get the lastest JSON file dataset
    
    latestcovid = jsonloc[0]
    
    #print(jsonloc)
    rooturl = "https://c19pub.azureedge.net/"
    regions = "regions.geojson"
    countries = "countries.geojson"
    utlas = "utlas.geojson"
    drequest = rooturl+countries
    data1 = requests.get(drequest)
    
    
    
    rdata = data1.json()
    print(rdata)

    	
    #cur = con.cursor()
    #cur.execute("INSERT INTO wmca_road.wmca_tomtom_traffic_flow(confidence, coordinates, currentspeed, currenttraveltime, date, frc, freeflowspeed, freeflowtraveltime, roadclosure, roadname) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(rconfidence, rcoordinates, rcurrentspeed, rcurrenttraveltime, rdate, rfrc, rfreeflowspeed, rfreeflowtraveltime, rroadclosure, rroadname))
    #con.commit()
    #cur.close()
    #con.close()
    return "DONE"
