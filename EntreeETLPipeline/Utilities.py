import glob
import pprint


LINE_SIZE = 300
def getSessionData(filename):

    with open(filename, 'r') as fo:
         blob = []
         for line in fo.readlines():
              line = line.strip().split()
              temp = {}
              temp['datetime'] = line[0]
              temp['ip'] = line[1]
              temp['entrypoint'] = line[2]
              temp['navigations'] = line[3:-1]
              temp['endpoint'] = line[-1]

              blob.append(temp)
         return blob


def mapRestIDName(filename):
    maptable = {}

    with open(filename,'r') as fo:
        for line in fo.readlines():
            line = line.split("\t")
            id = int(line[0])
            name = line[1]
            maptable[id] = name

    return maptable