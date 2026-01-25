from pyspark.sql import SparkSession

s = SparkSession.builder.master("local[*]").getOrCreate()
print("Spark version:", s.sparkContext.version)
print("Hadoop version:", s.sparkContext._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
s.stop()