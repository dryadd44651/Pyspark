# python commonFriend.py soc-LiveJournal1Adj.txt

from __future__ import print_function
import sys
from pyspark import SparkContext
import shutil
import os
from os import path

def mapper(line):
    user = line[0]
    friends = line[1].split(',')
    out = []

    for friend in friends:
        if len(friend) > 0:
            #key = user+", "+friend if int(user)<int(friend) else friend+", "+user
            key = ','.join(sorted([user, friend]))
            val = set(friends)
            out.append((key, val))
    return out

def reducer(val1,val2):#val1 and val2 have same key
    return val1.intersection(val2)


if __name__ == "__main__":
    if path.exists("Q2_Output"):
        shutil.rmtree("Q2_Output")
        print("deleted")
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.argv = [0,0]
        sys.argv[1] = "soc-LiveJournal1Adj.txt"
        # sys.exit(-1)
    sc = SparkContext.getOrCreate()
    topTen = sc.textFile(sys.argv[1]).map(lambda x: x.split('\t'))\
            .flatMap(mapper)\
            .reduceByKey(reducer)\
            .map(lambda x: [x[0],len(x[1])]).top(10, key=lambda k: k[1])
    #print(topTen)
    topUser = sc.parallelize(topTen)
    userData = sc.textFile("userdata.txt").map(lambda x: x.split(',')).map(lambda x: (x[0],x[1]+" "+x[2]+" "+x[3]))
    #a = tmpRes.map(lambda x:(x[1],x[0].split(',')[0]))
    #print(a.collect())
    a = topUser.map(lambda x:(x[0].split(',')[0],(x[0],x[1])))#A, (AB ,friend #)
    b = topUser.map(lambda x:(x[0].split(',')[1],x[0]))#B, AB
    userA = a.join(userData)#A, ((AB ,friend #),dataA)
    userB = b.join(userData)#B, (AB,dataB)
    tmpA = userA.map(lambda x:(x[1][0][0],(x[1][0][1],x[1][1])))#AB, (friend #,dataA)
    tmpB = userB.map(lambda x:(x[1][0],(x[1][1])))#AB, dataB
    tmpC = tmpA.join(tmpB)#AB, ((friend #,dataA),dataB)
    result = tmpC.map(lambda x:(x[1][0][0],x[1][0][1],x[1][1]))#friend#, (dataA,dataB)
    result.repartition(1).saveAsTextFile("Q2_Output")
    