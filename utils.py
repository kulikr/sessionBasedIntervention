import json as jsn
import csv

def convertDateToString(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()

def writeDictToFile(dict, fileName):
    json = jsn.dumps(dict, default=convertDateToString, separators=['\n',':'])
    f = open( fileName , "w")
    f.write(json)
    f.close()


def writeMonthDataToFile(sessions, accumulated_data, month):
    sessionsToDf(sessions,month)
    writeDictToFile(accumulated_data, "accumulated_"+month)

def sessionsToDf(sessions,fileEnding):
    with open('sessions_'+fileEnding+'.csv', 'w') as f:  # Just use 'w' mode in 3.x
        dict_writer = csv.DictWriter(f, sessions[0].keys())
        dict_writer.writeheader()
        dict_writer.writerows(sessions)
