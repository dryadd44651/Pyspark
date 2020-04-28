# python yelpBusinessRate.py business.csv review.csv

from __future__ import print_function
import sys
from pyspark import SparkContext
import shutil
import os
from os import path


def mapper(line):
    key = line[1][1][0]
    value = line[1][1][1]
    return key, value

if __name__ == "__main__":
    if path.exists("Q4_Output"):
        shutil.rmtree("Q4_Output")
        print("deleted")
    if len(sys.argv) != 3:
        print("Usage: wordcount <business.csv> <review.csv>", file=sys.stderr)
        sys.argv = [0,0,0]
        sys.argv[1] = "business.csv"
        sys.argv[2] = "review.csv"
        
        
        #sys.exit(-1)

    sys.argv[1] = "business.csv"
    sys.argv[2] = "review.csv"
    sc = SparkContext.getOrCreate();
    #"business_id"::"full_address"::"categories"
    # (business_id, (full address, categorie))
    bussiness = sc.textFile(sys.argv[1]).map(lambda x: (x.split('::'))).map(lambda x:(x[0],(x[1],x[2])))
    #"review_id"::"user_id"::"business_id"::"stars"
    #(business_id,(stars,1))
    review = sc.textFile(sys.argv[2]).map(lambda x: (x.split('::'))).map(lambda x: (x[2], (x[3],1)))
    #(business_id,(alld stars,rate #))
    sumReview = review.reduceByKey(lambda x, y: (float(x[0])+float(y[0]),x[1]+y[1]))
    #(business_id,(average rate))
    avgReview = sumReview.map(lambda x:(x[0],(float(x[1][0])/float(x[1][1]))))
    #topReviewList is list
    topReviewList = avgReview.top(10, key=lambda x: x[1])
    #(business_id,(average rate))
    topReview = sc.parallelize(topReviewList)
    #print(bussiness.collect()[0:10])
    #print(review.collect()[0:10])
    
    #business_id,((full address,,stars)
    join = bussiness.join(topReview)
    #remove the duplicated record
    join = join.reduceByKey(lambda x, y: x)
    #print(join.collect())
    #business id, full address, categories, avg rating
    result = join.map(lambda x: x[0]+'\t'+x[1][0][0]+'\t'+x[1][0][1]+'\t'+str(x[1][1]))
    result.repartition(1).saveAsTextFile("Q4_output")   
    
     
    

    