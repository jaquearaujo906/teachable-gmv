import os
import sys
from pyspark.sql import SparkSession

# garante python da venv
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# aponta para os binários nativos do Hadoop no Windows
os.environ['HADOOP_HOME'] = r'C:/hadoop'
os.environ['HADOOP_COMMON_LIB_NATIVE_DIR'] = r'C:/hadoop/bin'
os.environ['JAVA_LIBRARY_PATH'] = r'C:/hadoop/bin'

spark = (
    SparkSession.builder
    .appName("smoke-test")
    .master("local[*]")
    .config("spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
    # garante java.library.path para driver/executors
    .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
    .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
    # propaga variáveis de ambiente para executores (importante em local[*])
    .config("spark.executorEnv.HADOOP_HOME", "C:/hadoop")
    .config("spark.executorEnv.HADOOP_COMMON_LIB_NATIVE_DIR", "C:/hadoop/bin")
    .config("spark.executorEnv.PATH", os.environ.get("PATH", "") + ";C:\\hadoop\\bin")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "BR", 10.0), (2, "US", 20.0)],
    ["purchase_id", "subsidiary", "amount"]
)

output_path = "data_lake/bronze/_smoke_test_csv"
df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

print(f"Ok - wrote csv to {output_path}")
spark.stop()