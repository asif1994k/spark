# spark
import pyspark
from pyspark.sql import sparkSession
spark = sparksession.builder.appName("word_count").getOrCreate()
sc = spark.sparkContext
rdd1 = sc.textFile("file.txt")
rdd2 = rdd1.flatMap(lambda x: x.split(" "))
rdd3 = rdd2.map(lambda x: (x,1))
rdd4 = rdd3.reduceBykey(lambda x,y: x+y)
result =rdd4.collect()
for a in result:
print(a)


