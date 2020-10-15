#!/usr/bin/python
import psycopg2
import json
import requests
import datetime
from xml.etree import ElementTree

db_port = 5432
db_name = "wmca"
db_user = ""
db_pass = ""

def test():
    con = psycopg2.connect(
        host = "wmcapostgres.cok4j10mlpxt.eu-west-1.rds.amazonaws.com",
        database = db_name,
        user = db_user,
        password = db_pass)
    r = requests.get('https://publicdashacc.blob.core.windows.net/publicdata?restype=container&comp=list')
    # root address for the JSON GeoJSON etc https://c19pub.azureedge.net/
    #rtheflow = r.json()

    date_now = datetime.datetime.now()
    db_date = date_now.strftime("%d/%m/%Y %H:%M:%S")
    #print(db_date)

    root = ElementTree.fromstring(r.content)
    #print(dir(root))
    
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
    drequest = rooturl+latestcovid
    data1 = requests.get(drequest)
    
    
    updated = datetime.datetime.now()
    rdata = data1.json()
    #print(rdata)
    lupdated = rdata['lastUpdatedAt']
    #print(rdata['overview']['K02000001']['totalCases'])
    totalcases = rdata['overview']['K02000001']['totalCases']
    newcases = rdata['overview']['K02000001']['newCases']
    deaths = rdata['overview']['K02000001']['deaths']
    name = rdata['overview']['K02000001']['name']
    dailydeaths = rdata['overview']['K02000001']['dailyDeaths']
    dailytotaldeaths = rdata['overview']['K02000001']['dailyTotalDeaths']

    allcur = con.cursor()
    allcur.execute("INSERT INTO wmca_covid_dailies.uk_covid_dailies(name, totalcases, newcases, deaths, dailydeaths, dailytotaldeaths, updated) VALUES( %s, %s, %s, %s, %s, %s, %s)",(json.dumps(name), json.dumps(totalcases), json.dumps(newcases), json.dumps(deaths), json.dumps(dailydeaths), json.dumps(dailytotaldeaths), lupdated ))
    con.commit()
    #print(rdata['utlas']['E08000025']['name'])
    #print(rdata['utlas']['E08000025']['totalCases'])
    #print(rdata['utlas']['E08000025']['dailyConfirmedCases'])
    #print(rdata['utlas']['E08000025']['dailyTotalConfirmedCases'])
    auths = ['E08000025','E08000026','E08000027','E08000028','E08000029','E08000030','E08000031']
    
    for auth in auths:
        print(rdata['utlas'][auth]['name'])
        print(rdata['utlas'][auth]['totalCases'])
        authority = rdata['utlas'][auth]['name']
        dailyconfirmedcases = rdata['utlas'][auth]['dailyConfirmedCases'] 
        dailytotalconfirmedcases = rdata['utlas'][auth]['dailyTotalConfirmedCases'] 
        totalcases = rdata['utlas'][auth]['totalCases']
        cur = con.cursor()
        cur.execute("INSERT INTO wmca_covid_dailies.covid_dailies_cases(authority, dailyconfirmedcases, dailytotalconfirmedcases, totalcases, updated) VALUES( %s, %s, %s, %s, %s)",(json.dumps(authority), json.dumps(dailyconfirmedcases), json.dumps(dailytotalconfirmedcases), json.dumps(totalcases), updated ))
        con.commit()

    cur.close()
    con.close()
    return "DONE"

print(test())
