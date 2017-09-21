# -*- coding:utf-8 -*-
from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext
from operator import add

conf = SparkConf()
conf = conf.setAppName("test")
sc = SparkContext(conf=conf)

# distData = sc.parallelize(data,numSlices=10)

# read file as rdd
rdd = sc.textFile("file:///home/l/project/hadoop/sparkbyME/test/data.txt")
newrdd = rdd.map(lambda x:x.split("|")[0])
print len(newrdd.collect())
newrdd = newrdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print len(newrdd.collect())
# num = 1
# nums = 0
# for name,count in newrdd.collect():
#     if count == 2:
#         nums = nums + 1
# print nums
values = [v for v in dict(newrdd.collect()).values()]
distinctList = list(set(values))
print distinctList


data = [1,2,3,4,5]
distData = sc.parallelize(data)
result = distData.map(lambda x:x+1)
# print result.collect()

def gl(x):
    result = False
    if x > 2:
        result = True
    return result
result = distData.filter(gl)
# print result.collect()


x = sc.parallelize(range(1,5))
y = sc.parallelize(range(6,10))
# print x.zip(y).collect()

m = sc.parallelize([("a",1),(3,5)]).collectAsMap()
# print m['a']
# print str(m[3]) + "__________________collectAsMaptest"

# group by
rdd = sc.parallelize([1,1,2,3,5,8])
# def groupTest(i):
#     return i % 3
re = rdd.groupBy(lambda x: x%3).collect()
# print re
# print [(x,sorted(y)) for (x,y) in re]

# reduce
data = sc.parallelize(range(1,11))
result = data.reduce(lambda a,b: a*b)
# print result

list = [("a",1),("b",2),("c",3)]
rdd = sc.parallelize(list)
listValues = rdd.mapValues(lambda x:x+2)
# print listValues.collect()

x = sc.parallelize([1,2,3,4,5,6],3)
def f(i):
    yield sum(i)
y = x.mapPartitions(f)
# print y.collect()

x = sc.parallelize(range(10))
ylist = [x.sample(withReplacement=False,fraction=0.9) for i in range(5)]
# for y in ylist: print y.collect()

