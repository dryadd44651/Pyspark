from __future__ import print_function
import sys
from pyspark import SparkContext

def mapper(line):
    user = line[0]
    friends = line[1].split(',')
    out = []

    for friend in friends:
        if len(friend) > 0:
            key = ','.join(sorted([user, friend]))
            val = set(friends)
            out.append((key, val))
    return out

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.argv[1] = "soc-LiveJournal1Adj.txt"
        # sys.exit(-1)
    sc = SparkContext.getOrCreate()
    result = sc.textFile(sys.argv[1]).map(lambda x: x.split('\t'))\
        .flatMap(mapper)\
        .reduceByKey(lambda l1, l2: len(l1.intersection(l2))).sortByKey()
            
    #print(result.collect()[0:10])
    result.repartition(1).saveAsTextFile("Q1_Output")