#!/usr/bin/python
# -*- coding: utf-8 -*-
#########################################################
# createdResolved_incremental.py
# 7/9/2015 created; 8/8/2015 updated
#
########################################################

'''
   read in an RT dump pickle file, and generate "win/lose" create/resolve
        plots.

   USAGE: $ python createdResolved_incremental.py
          c:\Users\bh\RT-reports\data\pickled-merged_2015-07-10T145241.pkl
   (MS Windows example) where picklefilepath is a pointer to the pickled file.

   INPUTS: Pickle of Pandas dataframe of RT dumpfiles. From aggregate_data.py

   OUTPUT: a created/resolved win/loss plot of xxxxxx
        plus an index.html page in the same directory, for ease of browsing.

   EXAMPLES OF OUTPUT:
        http://www.slac.stanford.edu/~bhecker/RT_metrics/2015-02-13_analyses/
'''
################################################################
# FIXME AND IMPROVEMENTS
#
# * output the date of the extract / date of last data point -
#     analysis won't always run in the same time frame as the extract
################################################################

def readPickle(infile):
    import cPickle
    newDF = cPickle.load(open(infile, 'rb'))
    return newDF


def generate_index_page(starttime, queuelist):
    pageCode = '<html><head><title>RT created/resolved graphs generated %s</title> </head>\n' % starttime
    pageCode += '<body><h1>RT Created/Resolved graphs (weekly)</h1>\n'
    pageCode += '<body><h3>generated %s </h3>\n' % starttime
    pageCode += '<ol>'
    q = ''
    for q in queuelist:
        pageCode += '<li><a href="cr_'+q+'.png">'+q+'</a></li>\n'
    pageCode += '</ol></body></html>'
    return pageCode

import pandas as pd
#import numpy as np

#TO RUN INTERACTIVE, THIS COMMAND MUST RUN  before this program
# %matplotlib inline
# symptom is kernel hang

import matplotlib.pyplot as plt
import datetime, os, sys

print "Backend:", plt.matplotlib.rcParams['backend']
print "Interactive:", plt.isinteractive()

STATUSES = ['new', 'open', 'stalled', 'resolved', 'rejected', 'deleted']

STARTTIME = datetime.datetime.today().isoformat()[0:-7]
print "Start time:", STARTTIME
print "Display mode:", plt.matplotlib.rcParams['backend']

##queues of interest

QueueList = [ 'Authors','AUTHORS_add_user', 'AUTHORS_claim_manual',\
              'AUTHORS_cor_user',\
              'CONF_add+cor', 'CONF_add_user', 'Feedback',\
              'HEP', 'HEP_add_user', 'HEP_cor_user', 'HEP_ref_user', \
              'HEP_curation', 'Inspire-References', 'INST_add+cor', \
              'HEP_publishing', 'JOBS']

#QueueList = ['AUTHORS_cor_user']

## directory for data file  input and output
PICKLEFILEPATH = sys.argv[1]
# pickfile may be Unix or Windows
if "\\" in PICKLEFILEPATH:
    #windows
    FILEPATH, PICKLEFILE = PICKLEFILEPATH.split('\\data\\')
else:
    #unix
    FILEPATH, PICKLEFILE = PICKLEFILEPATH.split('/data/')

### create output directory for graphs
pathx = FILEPATH + '\\graphs\\' + 'create_resolve-'+STARTTIME.replace(':', '')

print "FILEPATH=", FILEPATH
print "PICKLEFILE=", PICKLEFILE



## unpickle entireSheetQI
entireSheetQI = readPickle(PICKLEFILEPATH)
#EntireSheet = EntireSheetQI.reset_index()
#entireSheetQI = EntireSheet.set_index('QueueName')

## modern epoch, i.e., point at which analysis starts being meaningful
BIN_START_EPOCH = pd.datetime(2013, 5, 1)  

#define weekly range to be used in defining binning dataframes.
rng = pd.date_range(start=BIN_START_EPOCH, end=datetime.date.today(), freq='w')

## odd structure, but avoids race condition and avoids file/dir confusion
try:
    os.makedirs(pathx)
except OSError:
    if not os.path.isdir(pathx):
        raise

####################################
## do the counting

for thisQueueName in QueueList:
    # extract only rows that belong to the selected queue
    ThisQueueFrame = entireSheetQI.loc[thisQueueName]
    print "===> ", thisQueueName, "<========="
    ## I didn't see a way to extract the ID column if it was the index
    qFrame = ThisQueueFrame.reset_index()
    idKeys = qFrame['id']
    # set up fresh bin (Pandas dataframe)
    tsbin = pd.DataFrame(0, index=rng, columns=['Created', 'Resolved'])
    qFrame = qFrame.set_index('id')

    for k in idKeys:
        c = qFrame.loc[k]['Created']
        y, m, d = c[:10].split('-')
        c = pd.datetime(int(y), int(m), int(d))
        r = qFrame.loc[k]['Resolved'] 
        if r == "Not set": 
            r = pd.datetime(2020, 12, 31)
        else:
            y, m, d = r[:10].split('-')
            r = pd.datetime(int(y), int(m), int(d))


        #######################################################################
        ## inconsistency between RT and binner: RT counts tickets which are
        ##    open, but have a Resolved date, as "open". Binner would natively
        ##    count these as resolved. So binner current counts would be < RT.
        ##    This check makes binner counting more like RT, by setting the
        ##    Resolved date to the "not set" date if status is open|new|stalled.
        if qFrame.loc[k]['Status'] in ['open', 'new', 'stalled']:
            r = pd.datetime(2020, 12, 31)

        for beginBin in tsbin.index:
            ## bin is an entire week -- careful comparison
            ## ticket starts before or equal to "endBin" date,
            ## and ends on or after the beginBin date.

            if c >= beginBin and c <= beginBin+datetime.timedelta(days=6):
                tsbin.loc[beginBin]['Created'] += 1
            if r >= beginBin and r <= beginBin+datetime.timedelta(days=6):
                tsbin.loc[beginBin]['Resolved'] += 1

    ## plot ##############
    ## #get_ipython().magic(u'matplotlib inline')
    #print plt.matplotlib.rcParams['backend']

    # http://matplotlib.org/api/pyplot_api.html#matplotlib.pyplot.bar
    #next line forces the y axis to originate at zero
    #plt.ylim(0,)
    plt.figure(figsize=[12, 8])
    plt.ylabel("Created and Resolved tickets, per week")
    plt.title(thisQueueName + " tickets created and resolved by week")
    wx = 6   # bar width
    pCreated = plt.bar(tsbin.index, tsbin['Created'], \
                       width=wx, color='k', linewidth=0)
    pResolved = plt.bar(tsbin.index, -tsbin['Resolved'], \
                        width=wx, color='g', bottom=0, linewidth=0)

    # http://matplotlib.org/api/axes_api.html#matplotlib.axes.Axes.legend
    #plt.legend((pCreated, pResolved), ('Created', 'Resolved') )
    plt.legend((pCreated, pResolved), ('Created', 'Resolved'), loc=3)


    ## if interactive, uncomment
    #plt.show()
    ## if hardcopy, uncomment following 5 lines
    plt.savefig(pathx+'\\cr_'+thisQueueName+'.png', dpi=140,\
                facecolor='w', format='png')
    plt.close()

OUTX = open(pathx+'\\cr-index.html', 'w')
OUTX.write(generate_index_page(STARTTIME, QueueList))
OUTX.close()

print "End time:", datetime.datetime.today().isoformat()

