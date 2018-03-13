import time
import re as re


### need to take care of the transfer event

# Dictionary that contains the batch events and a threshold for the length of a batch
def extractSessions(eventDict, session_idle_threshold):
    openSessions = {}
    closedSessions = []
    transferDict = {}
    accumulatedData = {}
    numOfBuys = 0
    accumulatedData['numOfBuys'] = 0
    accumulatedData['averageDwell'] = 0
    accumulatedData['numOfDwells'] = 0

    # iterate the log events
    for event in eventDict:
        currentUser = event['userId']

        # get the oldest id from the transfer dict for the current user(in case user logged in during session)
        currentUser = updateCurrentUser(currentUser, transferDict)

        # Cases the event is a transfer there is a need to map the new id to the old id
        if event['eventType'] == 'transfer':
            if (event['userId'] != event['newUserId']):
                transferDict[event['newUserId']] = event['userId']

        isBuyEvent = (event['eventType'] == 'buy')
        if isBuyEvent:
            accumulatedData['numOfBuys'] += 1

        # If the user has an active session
        if currentUser in openSessions:
            idleTime = (event['timestamp'] - openSessions[currentUser]['lastEvent']).seconds

            # If the user was idle for to long add the event to new session and add the last session to the close session
            if (idleTime > session_idle_threshold):

                ####### TO BE FIXED (length always zero) #####
                if (len(openSessions[currentUser]['boughtItems']) > 0):
                    openSessions[currentUser]['numOfPurchasedItems'] = len(openSessions[currentUser]['boughtItems'])
                #######
                # add to the closed sessions(finished)
                closedSessions.append(
                    openSessions[currentUser])  ## maybe it will be better to write 'closed sessions' to file here
                del openSessions[currentUser]
                if (currentUser in transferDict):
                    del transferDict[currentUser]
                addNewSession(event, currentUser, openSessions, isBuyEvent)
            # If the event is part of the session append it in the user session events
            else:
                updateCurrentSessionWithEvent(event, currentUser, openSessions, isBuyEvent)

                # update accumulated( num of dwells,average dwell,min dwell, max dwell)
                updateDwell(accumulatedData, openSessions, idleTime, currentUser)
        else:
            addNewSession(event, currentUser, openSessions, isBuyEvent)

    # add the remain sessions to closed sessions list
    for user in openSessions:
        closedSessions.append(openSessions[user])
    return closedSessions, accumulatedData


# Function that creates new session and appends it to the open session list
def addNewSession(event, currentUser, openSessions, isBuyEvent):
    openSessions[currentUser] = {}
    openSessions[currentUser]['isBuySession'] = isBuyEvent
    openSessions[currentUser]['numOfEvents'] = 1
    openSessions[currentUser]['maxDwell'] = 0
    openSessions[currentUser]['minDwell'] = 1000000000000
    openSessions[currentUser]['avgDwell'] = 0
    openSessions[currentUser]['totalDwell'] = 0
    openSessions[currentUser]['maximalItemPrice'] = 0
    openSessions[currentUser]['boughtItems'] = {}  # create {key:item , value:quantity}
    openSessions[currentUser]['numOfPurchasedItems'] = 0  # update the number when session is saved
    openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented
    openSessions[currentUser]['totalAmountOfPayment'] = 0  # amount paid by the user in the session
    openSessions[currentUser]['lastEvent'] = event['timestamp']
    if (isBuyEvent):
        addToBoughtItemsDict(event, openSessions, currentUser)
    openSessions[currentUser]['events'] = []


# Updates both the global dwell time as well as the sessions dwell time
def updateDwell(accumulatedData, openSessions, currentDwell, currentUser):
    updateSessionDwell(openSessions, currentDwell, currentUser)
    updateGlobalDwell(accumulatedData, currentDwell)


# Updates the global dwell time stored in 'accumulatedData' (avg dwell, total number of dwells)
def updateGlobalDwell(accumulatedData, currentDwell):
    accumulatedData['numOfDwells'] += 1
    numOfDwells = accumulatedData['numOfDwells']
    averageDwell = accumulatedData['averageDwell']
    if (accumulatedData['numOfDwells'] > 1):
        accumulatedData['averageDwell'] = averageDwell * (
                numOfDwells - 1) / numOfDwells + 1 / numOfDwells * currentDwell
    else:
        accumulatedData['averageDwell'] = currentDwell
    return accumulatedData


# Updates the current session dwell data stored at 'openSessions' (min dwell,max dwell)
def updateSessionDwell(openSessions, currentDwell, currentUser):
    if (currentDwell > openSessions[currentUser]['maxDwell']):
        openSessions[currentUser]['maxDwell'] = currentDwell
    if (currentDwell < openSessions[currentUser]['minDwell']):
        openSessions[currentUser]['minDwell'] = currentDwell
    openSessions[currentUser]['totalDwell'] += currentDwell


def updateCurrentUser(currentUser, transferDict):
    # Check whether the current user as an older ID (before login a temp id in must cases)
    if currentUser in transferDict:
        return transferDict[currentUser]
    else:
        return currentUser


def addToBoughtItemsDict(event, openSessions, currentUser):
    splittedInfo = re.split("=|&|E", event["extraInfo"])

    quantity = int(splittedInfo[1])
    price = float(splittedInfo[3])

    if (event['itemId'] in openSessions[currentUser]['boughtItems']):
        openSessions[currentUser]['boughtItems'][event['itemId']] += quantity
    else:
        openSessions[currentUser]['boughtItems'][event['itemId']] = quantity

    if (price > openSessions[currentUser]['maximalItemPrice']):
        openSessions[currentUser]['maximalItemPrice'] = float(splittedInfo[3])

    openSessions[currentUser]['numOfSaleItemsBought'] = 0  # not yet implemented (need to preProcess items catalogue)
    openSessions[currentUser]['totalAmountOfPayment'] += price * quantity


def updateCurrentSessionWithEvent(event, currentUser, openSessions, isBuyEvent):
    openSessions[currentUser]['numOfEvents'] += 1
    openSessions[currentUser]['isBuySession'] = (openSessions[currentUser][
                                                     'isBuySession'] or isBuyEvent)
    openSessions[currentUser]['lastEvent'] = event['timestamp']
    # openSessions[currentUser]['events'].append(event)
    if (isBuyEvent):
        addToBoughtItemsDict(event, openSessions, currentUser)
