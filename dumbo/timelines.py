import sys
import re
import urllib, httplib
import dumbo

ATTRIBUTES_PATTERN = re.compile('(?P<name>[^=]+)="(?P<value>[^"]*)" *')
INT_PROPS = frozenset(('SUBMIT_TIME','START_TIME','FINISH_TIME','SHUFFLE_FINISHED','SORT_FINISHED'))

scale = 1000

def mapper(key, line):
  event, rest = line.split(" ",1)
  attrs = {}
  for name, value in re.findall(ATTRIBUTES_PATTERN, rest):
    attrs.setdefault(name, [])
    attrs[name].append(int(value)/scale if name in INT_PROPS else value)

  if 'JOBNAME' in attrs:
    if event == 'Job':
      # Job has multiple JOBNAME, taking the longest for now. lame.
      names = sorted(attrs['JOBNAME'],lambda x,y: cmp(len(y), len(x)))
      yield names[0], (event, attrs)
    else:
      yield attrs['JOBNAME'][0], (event, attrs)
    
def reducer(key, values):
  
  mapStartTime = {}
  mapEndTime = {}
  reduceStartTime = {}
  reduceShuffleTime = {}
  reduceSortTime = {}
  reduceEndTime = {}
  finalAttempt = {}
  wastedAttempts = []
  submitTime = None
  finishTime = None

  for event, attrs in values:
    attrs = dict((k,v[0]) for k,v in attrs.items())
    if event == 'Job':
      #print >> sys.stderr, 'reduce', key, attrs.keys()
      if "SUBMIT_TIME" in attrs:
        submitTime = attrs["SUBMIT_TIME"]
      if "FINISH_TIME" in attrs:
        finishTime = attrs["FINISH_TIME"]
    elif event == 'MapAttempt':
      attempt = attrs["TASK_ATTEMPT_ID"]
      time = attrs.get("START_TIME", 0)
      if time != 0:
          mapStartTime[attempt] = time
      elif "FINISH_TIME" in attrs:
        mapEndTime[attempt] = attrs["FINISH_TIME"]
        if attrs.get("TASK_STATUS", "") == "SUCCESS":
          task = attrs["TASKID"]
          if task in finalAttempt:
            wastedAttempts.append(finalAttempt[task])
          finalAttempt[task] = attempt
        else:
          wastedAttempts.append(attempt)
    elif event == 'ReduceAttempt':
      attempt = attrs["TASK_ATTEMPT_ID"]
      time = attrs.get("START_TIME", 0)
      if time != 0:
          reduceStartTime[attempt] = time
      elif "FINISH_TIME" in attrs:
        task = attrs["TASKID"]
        if attrs.get("TASK_STATUS", "") == "SUCCESS":
          if task in finalAttempt:
            wastedAttempts.append(finalAttempt[task])
          finalAttempt[task] = attempt
        else:
          wastedAttempts.append(attempt)
        reduceEndTime[attempt] = attrs["FINISH_TIME"]
        if "SHUFFLE_FINISHED" in attrs:
          reduceShuffleTime[attempt] = attrs["SHUFFLE_FINISHED"]
        if "SORT_FINISHED" in attrs:
          reduceSortTime[attempt] = attrs["SORT_FINISHED"]

  final = frozenset(finalAttempt.values())

  runningMaps = []
  shufflingReduces = []
  sortingReduces = []
  runningReduces = []
  waste = []

  if not submitTime or not finishTime:
    dumbo.core.incrcounter('Timelines', 'Incomplete Jobs', 1)
    return

  for t in range(submitTime, finishTime):
    runningMaps.append(0)
    shufflingReduces.append(0)
    sortingReduces.append(0)
    runningReduces.append(0)
    waste.append(0)

  for task in mapEndTime.keys():
    if task in mapStartTime:
      for t in range(mapStartTime[task]-submitTime, mapEndTime[task]-submitTime):
        if task in final:
          runningMaps[t] += 1
        else:
          waste[t] += 1

  for task in reduceEndTime.keys():
    if task in reduceStartTime:
      if task in final:
        for t in range(reduceStartTime[task]-submitTime, reduceShuffleTime[task]-submitTime):
          shufflingReduces[t] += 1
        for t in range(reduceShuffleTime[task]-submitTime, reduceSortTime[task]-submitTime):
          sortingReduces[t] += 1
        for t in range(reduceSortTime[task]-submitTime, reduceEndTime[task]-submitTime):
          runningReduces[t] += 1
      else:
        for t in range(reduceStartTime[task]-submitTime, reduceEndTime[task]-submitTime):
          waste[t] += 1

  params = {'maps': runningMaps, 'shuffles': shufflingReduces,
    'merges': sortingReduces, 'reducers': runningReduces,
    'waste': waste }
    
  params = dict([(k,",".join(str(c) for c in v)) for k,v in params.items()])
  
  params['start'] = submitTime
  params['end'] = finishTime
  
  params['mapcount'] = len([k for k in mapEndTime.keys() if k in mapStartTime and k in final])
  params['redcount'] = len([k for k in reduceEndTime.keys() if k in reduceStartTime and k in final])
  
  conn = httplib.HTTPConnection("hadoop-timelines.appspot.com:80")
  conn.request("POST", "/timelines", urllib.urlencode(params))
  response = conn.getresponse()
  yield key, response.getheader('location')
  
if __name__ == "__main__":
  dumbo.run(mapper, reducer)
