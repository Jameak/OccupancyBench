import json
import subprocess
import collections
import os

idMapFileName = "idMap.csv"

username = ""
password = ""
serverUrl = ""
with open("secrets.txt", 'r') as info:
    lines = info.readlines()
    username = lines[0].replace("\n","")
    password = lines[1].replace("\n","")
    serverUrl = lines[2].replace("\n","")

idMap = {} # AP-name -> assigned id
lastId = 0
if os.path.isfile(idMapFileName):
    mapping_file = open(idMapFileName, 'r')
    line = mapping_file.readline()
    while line:
        line = line.replace("\n","")
        contents = line.split(';')
        idMap[contents[1]] = contents[0]
        lastId = int(contents[0])
        line = mapping_file.readline()

    mapping_file.close()

if not os.path.isdir("map"):
    os.mkdir("map")

def extractSeedData(timestamp, output):
    global lastId
    query = "SELECT AP,clients FROM occtest1 WHERE time < '" + timestamp + "' + 30s AND time > '" + timestamp + "' - 30s"
    command = "curl --silent -G -u " + username + ":" + password + " '" + serverUrl + "/query' --data-urlencode 'db=occupancy' --data-urlencode \"q=" + query + "\""

    # subprocess.run requires python 3.5 or newer.
    result = subprocess.run(command, stdout=subprocess.PIPE, shell=True).stdout.decode('utf-8')
    #with open('raw.json', 'w+') as raw:
    #    raw.write(json.dumps(result))
    dict = json.loads(result)
    try:
        list = dict["results"][0]["series"][0]["values"]
    except:
        output.append("Time;" + timestamp + "\n")
        output.append("Total clients;0\n")
        output.append("NO DATA\n")
        return -1;

    totalClients = 0
    idMapKeys = idMap.keys()
    newList = []
    mapping_file = open(idMapFileName, 'a')
    for entry in list:
        id = -1
        APname = entry[1].upper()
        if APname in idMapKeys:
            id = idMap[APname]
        else:
            mapping_file.write(str(lastId) + ";" + APname + "\n")
            idMap[APname] = lastId
            id = lastId
            lastId += 1
        newList.append([id, entry[2]])
            
        totalClients += entry[2] 

    mapping_file.close()

    probList = []
    for entry in newList:
        probability = 0 if totalClients == 0 else entry[1] / totalClients
        probList.append([entry[0], probability])

    output.append("Time;" + timestamp + "\n")
    output.append("Total clients;" + str(totalClients) + "\n")
    for entry in probList:
        output.append(str(entry[0]) + ";" + str(entry[1]) + "\n")

def downloadMonth(year, monthNumber, endDay):
    for day in range(endDay):
        downloadDay(str(year) + "-" + str(monthNumber).zfill(2) + "-" + str(day+1).zfill(2))

def downloadDay(day):
    output = []
    for hour in range(24):
        for minute in range(60):
            #Example format (RFC3339): "2019-09-11T09:30:00Z"
            timestamp = day + "T" + str(hour).zfill(2) + ":" + str(minute).zfill(2) + ":00Z"
            print("Getting map for " + timestamp)
            extractSeedData(timestamp, output)
    with open(os.path.join("map", day + '.csv'), 'w+') as probabilityMap:
        for entry in output:
            probabilityMap.write(entry)

#downloadMonth(2018,  7, 31)
#downloadMonth(2018,  8, 31)
#downloadMonth(2018,  9, 30)
#downloadMonth(2018, 10, 31)
#downloadMonth(2018, 11, 30)
#downloadMonth(2018, 12, 31)
#downloadMonth(2019,  1, 31)
#downloadMonth(2019,  2, 28)
#downloadMonth(2019,  3, 31)
#downloadMonth(2019,  4, 30)
#downloadMonth(2019,  5, 31)
#downloadMonth(2019,  6, 30)
