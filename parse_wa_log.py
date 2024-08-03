
# Parses a Web Adaptor 11.x log file into a csv
# "C:\Program Files\ArcGIS\Server\framework\runtime\ArcGIS\bin\Python\Scripts\propy.bat" "parse_wa_log.py"
import sys 
import os
import datetime
import csv
from urllib.parse import urlparse
import re

def main(argv=None):
    
    # create output csv with same name
    logFileName = r'C:\temp\scdnr\22July\webadaptor20240722.log'
    csvFileName = createCsvFileName(logFileName)

    # do the work
    parseWebAdaptorLogFile(logFileName, csvFileName)
    print('File output: ' + csvFileName)
    
    
# create a matching csv name for the logfile
def createCsvFileName(logFileName):
    shortLogFileName = os.path.basename(logFileName)
    csvFileBaseName = shortLogFileName.replace('.log','.csv')
    thePath = os.path.dirname(logFileName)
    csvFileName = os.path.join(thePath,csvFileBaseName)
    return csvFileName

# this is the status code that Web Adaptor returns to IIS    
def extractFrontEndStatusCodeFromFinishedRecord(message):
    statusCode = ''
    try:
        matchList = re.findall('- \d{3}', message)
        if len(matchList) == 1:
            matchList = re.findall('\d{3}',matchList[0])
            if len(matchList) == 1:        
                statusCode = matchList[0]
    except:
        statusCode = ''
    return statusCode

# this is the status code that Web Adaptor receives from the backend
def extractBackEndStatusCodeFromFinishedRecord(message):
    statusCode = ''
    try:
        
        if 'End processing HTTP request after' in message:
            endOfTiming = message.find('ms -')
            statusCode = message[(endOfTiming + len('ms –')):len(message)]
    except:
        statusCode = ''
        
    return statusCode

# pulls out any url (received from the front or sent to the back)
def extractUrlFromFinishedRecord(message):
    parseResult = None
    try:
        if 'https://' in message:
            startOfUrl = message.find('https://')
            endOfUrl = message.find(' ',startOfUrl)
            url = message[startOfUrl:endOfUrl]
            parseResult = urlparse(url)
    except:
        parseResult = None
    return parseResult
    
# parses information from the "message" portion so it can be normalized for querying purposes
def finalProcessingOfRecord(lastProcessedRecord):
    
    message = lastProcessedRecord['message']
    
    # find the frontendstatuscode (if present) and add it to the dictionary
    statusCode = extractFrontEndStatusCodeFromFinishedRecord(message)
    lastProcessedRecord['frontstatuscode'] = statusCode
    
    # find the backendstatuscode (if present) and add it to the dictionary
    backStatusCode = extractBackEndStatusCodeFromFinishedRecord(message)
    lastProcessedRecord['backstatuscode'] = backStatusCode

    # url components
    parseResult = extractUrlFromFinishedRecord(message)
    if (parseResult != None):
        lastProcessedRecord['targethost'] = parseResult.netloc
        lastProcessedRecord['targetpath'] = parseResult.path
        lastProcessedRecord['targetquery'] = parseResult.query
    else:
        lastProcessedRecord['targethost'] = ''
        lastProcessedRecord['targetpath'] = ''
        lastProcessedRecord['targetquery'] = ''
    
    return lastProcessedRecord
    
# do the log reading, parsing, and csv writing
def parseWebAdaptorLogFile(logFileName, csvFileName):
    
    # open csv for output
    with open (csvFileName, 'w', newline="") as csvFile:
        bKeysWritten = False

        # open log file for reading
        with open (logFileName,'r') as logFile:
            # read all lines
            lines = logFile.readlines()
            count = 0
            lastProcessedRecord = {} # a dictionary for one line
            
            #iterate through lines
            for line in lines:
                count += 1
                endOfDate = line.find('T',0)
                theDate = line[0:endOfDate]
                bIsDate = False
                try:
                    d = datetime.datetime.strptime(theDate,'%Y-%m-%d')
                    bIsDate = True
                except:
                    bIsDate = False

                # if the line begins with a date, we have a new record to parse
                if bIsDate:
                    
                    # if we have an accumulated lastProcessedRecord, write it and re-initialize (because the start of this line indicates there is a new record to process.
                    if bool(lastProcessedRecord):
                        
                        # extract any goodies
                        lastProcessedRecord = finalProcessingOfRecord(lastProcessedRecord)
                        
                        # record the record
                        if bKeysWritten == False:
                            # if we've not yet written the header, do that and the first record
                            writer = csv.DictWriter(csvFile, lastProcessedRecord.keys())
                            writer.writeheader()
                            writer.writerow(lastProcessedRecord)
                            bKeysWritten = True
                        else:
                            # if the header exists, write the record
                            writer.writerow(lastProcessedRecord)
                            # note progress to stdout
                            if (count % 1000 == 0):
                                print('Record: ' + str(count) + ' written')
                        # re-initialize it
                        lastProcessedRecord = {}
                    
                    # parse the new log line ...
                    #endOfTime = line.find('-',endOfDate) # probably only works west of greenwich (otherwise it might be '+')
                    endOfTime = (endOfDate + 17) # getting time based on observed length of time information.
                    theTime = line[(endOfDate + 1):(endOfTime)]
                    
                    startOfType = line.find(' [',endOfTime)
                    theZone = line[endOfTime:startOfType]
                    endOfType = line.find('] ',startOfType)
                    theType = line[(startOfType + 2):endOfType]
                    startOfModule = line.find(' (',endOfType)
                    endOfModule = line.find(') ', startOfModule)
                    theModule = line[(startOfModule + 2):endOfModule]
                    theMessage = line[(endOfModule + 1):len(line)]
                    
                    # start the dictionary for this line, it will either be appended to or written on the next iteration of the loop ...
                    lastProcessedRecord['lineNumber'] = str(count)
                    lastProcessedRecord['date'] = theDate
                    lastProcessedRecord['time'] = theTime
                    lastProcessedRecord['datetime'] = theDate + ' ' + theTime
                    lastProcessedRecord['zone'] = theZone
                    lastProcessedRecord['type'] = theType
                    lastProcessedRecord['module'] = theModule
                    lastProcessedRecord['message'] = theMessage.replace('\n',' ').replace('\r',' ')
                
                # if the line doesn't begin with a date, we want to append this line's content to the lastProcessedRecord.message entity
                else:
                    lastProcessedRecord['message'] = lastProcessedRecord['message'] + ' ' + line.replace('\n',' ').replace('\r',' ')
                
                
                #if count > 300:
                #    break                     

            # write the last dictionary, if it has not already been written
            if bool(lastProcessedRecord):
                writer.writerow(lastProcessedRecord)
                print('Final record: ' + str(count) + ' written')
   

if __name__ == "__main__":
	sys.exit(main(sys.argv))