import PreProcess_ExtractEvents as pre_events
import PreProcess_ExtractSessions as pre_sessions
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import pylab
import plotly.plotly as py
import seaborn as sns
import numpy as np
import json as jsn
import utils as utl
from sklearn import preprocessing as pre




def save_dist_plot(title, feature, filepath):
    plt.title(title)
    sns.distplot(feature)
    plt.legend(loc='upper right')
    plt.savefig("./" + title + ".png")
    plt.close()

def plot_results(title, x_label, y_label, x, y):
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    plt.bar(x, y)
    plt.legend(loc='upper right')
    plt.savefig("./" + title + ".png")
    plt.close()

def plotDictDistribution(dict,title, x_label,y_label):
    x = []
    y = []

    for key in sorted(dict, key=number_map.__getitem__):
        x.append(int(key))
        y.append(dict[key])
    plot_results(title, x_label, y_label, x, y)

def plotBuyRate(dict1,dict2,title, x_label,y_label):
    x=[]
    y=[]
    tmp_y=[]

    for key in sorted(dict1, key=number_map.__getitem__):
        x.append(int(key))
        value= dict1[key]/dict2[key]
        y.append(value)

    y=pre.normalize(np.reshape(y,(1,-1)))
    ben=2

    plot_results(title, x_label,y_label,x,y)


ABS_EVENTS_PATH = "G:\\My Drive\\data\\lidl-usage-exports"
ABS_CATALOG_PATH = "G:\\My Drive\\data\\lidl-exports"

number_map={}
for i in range(0,3000):
    number_map[str(i)]=i

eventDict = pre_events.extractEvents("C:\\sample_data.txt", 1000000)
sessions , accumulated_data = pre_sessions.extractSessions(eventDict, 5000)


sessionsLength = {}
sessionsBuy = []
buyPerSessionLength = {}
noBuyPerSessionLength={}




for session in sessions:
    session_length = str(len(session['events']))

    if(session_length not in sessionsLength):
        sessionsLength[session_length]=1
    else:
        sessionsLength[session_length]+=1

    if(session['isBuySession']):
        if (session_length not in buyPerSessionLength):
            buyPerSessionLength[session_length] = 1
        else:
            buyPerSessionLength[session_length] += 1

utl.writeMonthDataToFile(sessions,accumulated_data, "04")
#plotDictDistribution(sessionsLength,"session length distribution", "session length", "number of session")
#plotBuyRate(buyPerSessionLength,sessionsLength,"buy rate","session length","buy rate")

print("num of buys : " + str(accumulated_data['numOfBuys']))
print("num of sessions : " + str(len(sessions)))

# print(buyPerSessionLength)




