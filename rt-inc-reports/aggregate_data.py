#/usr/bin/python
#!/usr/bin/python
#########################################################
# aggregate_data.py
# 7/9/2015 created; 9/2/2015 updated;
#
########################################################

'''
   Concatenates an arbitrary collection of RT dump files.
   creates an intermediary data structure that then serves
   as input for the graphing routines.
   This enables us to add new data incrementally, rather than running
   the entire, slow-to-download complete data dump every time.
   Be sure to specify the date range, and use a ".tsv" extension on the files.

   USAGE: python aggregate_data.py directory_path
   where directory_path is directory with /data and /graphs subdirectories. Ex.,
       'c:\Users\bhecker\Documents\python\pandas\RT\rt-inc-reports'
   (MS Windows example.)

   TO RUN: cd to the location of the *.tsv files (i.e., the /data subdir),
   as given by FILEPATH variable. then run
        python ../aggregate_data.py

   INPUTS: output of RT saved search (BH: "manual queues, non-antique
        w/Org"), then should be downloaded by the RT
        "Feed/Spreadsheet" feature.  (Complete data set takes a long
        time to download.)  Files must have "*.tsv" extension.  Files
        must reside in FILEPATH/data directory.

   OUTPUT: aggregated dataset, in Pandas data frame format, pickled.
        Into /data subdir.  This serves as input when you run the
        "binner_incremental.py" and "createdResolvled"_incremental.py"
        reporting scripts.

   EXAMPLES OF OUTPUT: http://www.slac.stanford.edu/~bhecker/RT_metrics/2015-02-13_analyses/ 
 
'''

## TO DO
## header skipping doesn't work (it only skips the first character of the stream)
##

#######################################################
import sys, glob, datetime;
import pandas as pd;
import numpy as np;
import cPickle

STARTTIME = datetime.datetime.today().isoformat()[0:-7]
print "Start time:", STARTTIME


## directory for data file  input and output
## example of filepath on a Windows box:
#  'c:\\Users\\bhecker\\Documents\\python\\pandas\\RT\\rt-inc-reports'
FILEPATH = sys.argv[1]
#FILEPATH =  'c:\Users\bhecker\Documents\python\pandas\RT\rt-inc-reports'
print "FILEPATH=", FILEPATH

tsvFiles = glob.glob(FILEPATH + '\\data\\*.tsv')
tsvFiles.sort();  ## not sure if glob reliably returns sorted file names, which is
                  ## important for the deduping later on

outfile = FILEPATH + '\\data\\' + 'merged-tsv_' + STARTTIME.replace(':','')
print"Combining tsv files into intermediate output file: ", outfile

noHeaderYet = True

with open(outfile, "wb") as OUTX:
    for f in tsvFiles:
        print "Reading input file:", f
        with open(f, "rb") as infile:
            if noHeaderYet:
                OUTX.write(infile.read())
                noHeaderYet = False
            else:
                # skip the header row in all but the first file
                OUTX.write(infile.read()[1:])                
OUTX.close()
infile.close()

## use Pandas to read the CSV file into a Pandas dataframe
print "Reading intermediate file into Pandas dataframe..."
EntireSheet=pd.read_csv(outfile, delimiter='\t', index_col=None, na_values=['NA'] )


## now need to DEDUPE, saving the most recently updated
##   but careful, have to select the *most recent*, not just dedupe. "take_last" might do
## NOTE: take_last deprecated, use keep='last'
##  see http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.drop_duplicates.html?highlight=deduplicate
##  http://pandas.pydata.org/pandas-docs/stable/indexing.html?highlight=take_last
##    "last" defined in order of loading, unless the frame is reordered?

## modified 8/24/2015
### NOTE: Input files names MUST sort into time order. So name w/time stamp first.
###
### Note: Drop_duplicates deals with fact that updated records will show up
### in the currentd search, and may correspond to records that were previously
### loaded (ex., when created, or previously modified)
entireSheetQI = EntireSheet.drop_duplicates(subset='id',keep='last').set_index('QueueName')

#########################################################
## OUTPUT PICKLED FILE ##################################
##  create output directory for graphs
print "Saving pickled dataframe..."
pickleFile = FILEPATH + '\\data\\' + 'pickled-merged_' + STARTTIME.replace(':','') + '.pkl'
outpickle = open(pickleFile, 'wb')
cPickle.dump(entireSheetQI, outpickle)
outpickle.close()

print "\nMerged pickled file created. Record this name to use \n    as input for the graphing runs:"
print pickleFile

# ex.,
# 'c:\Users\bhecker\Documents\python\pandas\RT\rt-inc-reports\data\pickled-merged_2015-07-10T144803.pkl'
#=================================

def toReadPickle():
    # example only on how to use the pickle
    import cPickle
    newDF = cPickle.load(open(FILENAME, 'rb'))
    qf = entireSheetQI.reset_index()
    ik = qf.set_index('id')
    ik.loc['497611']
    #etc

