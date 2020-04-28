from __future__ import print_function
import sys
from pyspark import SparkContext


if __name__ == "__main__":
    if path.exists("Q3_output"):
        shutil.rmtree("Q3_output")
        print("deleted")
    if len(sys.argv) != 4:
        print("Usage: wordcount <business.csv> <review.csv> <user.csv>", file=sys.stderr)
        sys.argv = [0,0,0,0]
        sys.argv[1] = "business.csv"
        sys.argv[2] = "review.csv"
        sys.argv[3] = "user.csv"
        #sys.exit(-1)


    sc = SparkContext.getOrCreate();
    #"business_id"::"full_address"::"categories"
    business = sc.textFile(sys.argv[1]).map(lambda x: (x.split('::')[0], x.split('::')[1])).filter(lambda out: "Stanford" in out[1])
    #business_id, full_address
    business = business.reduceByKey(lambda x, y: x)
    print(len(business.collect()))
    #"review_id"::"user_id"::"business_id"::"stars"
    #business_id, user_id, stars
    review = sc.textFile(sys.argv[2]).map(lambda x: (x.split('::')[2], (x.split('::')[1], x.split('::')[3])))
    print(len(review.collect()))
    
    
    #"user_id"::"name"::"url"
    #user = sc.textFile(sys.argv[3])
    
    
    #business_id, (full_address, (user_id, stars))
    join = business.join(review)#leftOuterJoin
    print(len(join.collect()))
    #print(join.collect()[0:1])
    #user_id stars
    result = join.map(lambda x: x[0]+'\t'+x[1][0]+'\t'+x[1][1][0]+'\t'+str(x[1][1][1]))
    #print(result.collect()[0:1])
    result.repartition(1).saveAsTextFile("Q3_output")
    
    