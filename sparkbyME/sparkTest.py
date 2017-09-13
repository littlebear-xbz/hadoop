# -*- coding:utf-8 -*-
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

conf = SparkConf()
conf = conf.setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)

# distData = sc.parallelize(data,numSlices=10)

# read file as rdd
rdd = sc.textFile("./test/data.txt")

data = [1,2,3,4,5]
distData = sc.parallelize(data)
result = distData.map(lambda x:x+1)
print result.collect()

def gl(x):
    result = False
    if x > 2:
        result = True
    return result
result = distData.filter(gl)
print result.collect()


x = sc.parallelize(range(1,5))
y = sc.parallelize(range(6,10))
print x.zip(y).collect()

m = sc.parallelize([("a",1),(3,5)]).collectAsMap()
print m['a']
print str(m[3]) + "__________________collectAsMaptest"

# group
rdd = sc.parallelize([1,1,2,3,5,8])
# def groupTest(i):
#     return i % 3
re = rdd.groupBy(lambda x: x%3).collect()
print re
print [(x,sorted(y)) for (x,y) in re]

# reduce
data = sc.parallelize(range(1,11))
result = data.reduce(lambda a,b: a*b)
print result