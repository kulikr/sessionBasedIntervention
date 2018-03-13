import re as re
import datetime as dt
import time as time


monthDict={"Jan": 1 ,"Feb": 2,"Mar": 3,"Apr": 4,"May": 5,"Jun": 6,"Jul": 7,"Aug":8,"Sep": 9,"Oct": 10, "Nov": 11 ,"Dec": 12}

# Returns a list of dictionaries contains for each event the 'timestamp', 'eventType', 'userId', 'itemId'
def extractEvents(filePath,numOfEvents):
    events = []
    eventsList=getEventsList(filePath, numOfEvents)
    check=time.time()
    splittedEvents = [re.split(";", clickEvent) for clickEvent in eventsList]
    print("split time stamps from rest of the event with re.split:" + str(time.time()-check))
    check=time.time()
    timeStamps=extractTimeStamps(splittedEvents)
    print("time to extract all time stamps - splitting and creating time and date object" + str(time.time() - check))

    eventData =[re.split("/|\?",splitEvent[1]) for splitEvent in splittedEvents]

    #iterate the splitted lines of the file and extract the event features
    for i in range(len(eventData)):
        events.append(dict())
        eventType=eventData[i][3]

        events[i]["eventType"]=eventType
        events[i]["timestamp"]=timeStamps[i]

        if(eventType=='transfer'):
            events[i]["userId"] = eventData[i][4]
            events[i]["newUserId"] = eventData[i][5]
        else:
            events[i]['userId'] = eventData[i][4]
            events[i]['itemId'] = eventData[i][6]

            # in case there is extra information related to the event
            if (len(eventData[i]) > 7):
                events[i]['extraInfo'] = eventData[i][7]

    return events



#Read the events from the given path and returns list of events(seperated by the new line)
def getEventsList(filePath,numOfLines):
    read_data=[]
    with open(filePath,'r') as f:
        for line in f:
            splittedLine=line.rstrip('\n')
            splittedLine=splittedLine.strip('[')
            read_data.append(splittedLine)

    print("read data length is: " + str(len(read_data)))
    return read_data

# Turns the time stamp into a dateTime object with the event time and return list of dateTime objects
def extractTimeStamps(splittedEvents):
    timeStamps = [re.split(':|\/' ,splitEvent[0]) for splitEvent in splittedEvents]
    timeStampList=[]
    for i in range(len(timeStamps)):
        eventTime=dt.datetime(int(timeStamps[i][2]),monthDict[timeStamps[i][1]],int(timeStamps[i][0]),int(timeStamps[i][3]),int(timeStamps[i][4]),int(timeStamps[i][5]))
        timeStampList.append(eventTime)
    return timeStampList
