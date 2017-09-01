#coding = utf-8
import sys

a = lambda a,b,c : a+b*c

print 'a---lambda   ' + str(a(1,2,3))

c = reduce(lambda x,y : x*y ,[1,2,3,4,5],1)

print 'reduce----c  ' + str(c)

b=range(1,6)
e=range(13,16)
d = map(lambda x:str(x)+"_mapped",b)

print b

f = map(lambda x,y:(str(x)+'_'+str(y) , x+y , x*y) , [1,2,3],[7,8,9])
print f