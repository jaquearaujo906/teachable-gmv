import os
import sys
import argparse
from datetime import datetime, timedelta, date

from pyspark.sql import SparkSession, functions as F, Window


def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def iso(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def make_spark() -> SparkSession:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    os.environ["HADOOP_HOME"] = r"C:/hadoop"
    os.environ["HADOOP_COMMON_LIB_NATIVE_DIR"] = r"C:/hadoop/bin"
    os.environ["JAVA_LIBRARY_PATH"] = r"C:/hadoop/bin"

    spark = (
        SparkSession.builder
        .appName("teachable-gmv-etl")
        .master("local[*]")
        .config(
            "spark.sql.sources.commitProtocolClass",
            "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol",
        )
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped", "true")
        .config("spark.driver.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
        .config("spark.executor.extraJavaOptions", "-Djava.library.path=C:/hadoop/bin")
        .config("spark.executorEnv.HADOOP_HOME", "C:/hadoop")
        .config("spark.executorEnv.HADOOP_COMMON_LIB_NATIVE_DIR", "C:/hadoop/bin")
        .config("spark.executorEnv.PATH", os.environ.get("PATH", "") + ";C:\\hadoop\\bin")
        .getOrCreate()
    )

    # overwrite só das partições presentes no DF
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def main():
    parser = argparse.ArgumentParser(description="Teachable GMV ETL (CDC + late arrivals + append-only gold)")
    parser.add_argument("--bronze-base", default="data_lake/bronze")
    parser.add_argument("--silver-base", default="data_lake/silver")
    parser.add_argument("--gold-base", default="data_lake/gold")

    # janela
    parser.add_argument("--days-back", type=int, default=30, help="Reprocess last N days ending yesterday")
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD (optional)")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD (optional, inclusive)")

    parser.add_argument("--write-silver", action="store_true", help="Persist silver outputs to disk")

    args = parser.parse_args()
    spark = make_spark()

    BRONZE_BASE = args.bronze_base
    SILVER_BASE = args.silver_base
    GOLD_BASE = args.gold_base

    # -----------------------------
    # 1) Read Bronze (folders)
    # -----------------------------
    purchase_df = spark.read.option("header", True).csv(f"{BRONZE_BASE}/purchase/")
    product_item_df = spark.read.option("header", True).csv(f"{BRONZE_BASE}/product_item/")
    extra_df = spark.read.option("header", True).csv(f"{BRONZE_BASE}/product_extra_info/")

    # -----------------------------
    # 2) Type conversions
    # -----------------------------
    purchase_df = purchase_df.withColumn("purchase_id", F.col("purchase_id").cast("string"))
    product_item_df = product_item_df.withColumn("purchase_id", F.col("purchase_id").cast("string"))
    extra_df = extra_df.withColumn("purchase_id", F.col("purchase_id").cast("string"))

    purchase_df = purchase_df.withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))
    product_item_df = product_item_df.withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))
    extra_df = extra_df.withColumn("transaction_datetime", F.to_timestamp("transaction_datetime"))

    purchase_df = purchase_df.withColumn("transaction_date", F.to_date("transaction_date"))
    product_item_df = product_item_df.withColumn("transaction_date", F.to_date("transaction_date"))
    extra_df = extra_df.withColumn("transaction_date", F.to_date("transaction_date"))

    purchase_df = purchase_df.withColumn("release_date", F.to_date("release_date"))
    purchase_df = purchase_df.withColumn("order_date", F.to_date("order_date"))

    product_item_df = product_item_df.withColumn("purchase_value", F.col("purchase_value").cast("double"))
    product_item_df = product_item_df.withColumn("item_quantity", F.col("item_quantity").cast("int"))

    # -----------------------------
    # 3) Define window
    # -----------------------------
    # Se usuário passar start/end, usa.
    # Se não passar, usa min/max do dataset (melhor para mock com datas antigas).
    if args.start_date or args.end_date:
        today = date.today()
        default_end = today - timedelta(days=1)

        if args.start_date and args.end_date:
            start_d = parse_date(args.start_date)
            end_d = parse_date(args.end_date)
        elif args.start_date and not args.end_date:
            start_d = parse_date(args.start_date)
            end_d = default_end
        else:  # end_date sem start_date
            end_d = parse_date(args.end_date)
            start_d = end_d - timedelta(days=args.days_back)
    else:
        # pega min/max do bronze (purchase) para default window
        mm = (
            purchase_df
            .select(F.min("transaction_date").alias("min_d"), F.max("transaction_date").alias("max_d"))
            .collect()[0]
        )
        if mm["min_d"] is None or mm["max_d"] is None:
            print("[WARN] No transaction_date found in purchase data. Exiting.")
            spark.stop()
            return
        start_d = mm["min_d"]
        end_d = mm["max_d"]

    if start_d > end_d:
        raise ValueError(f"Invalid window: start_date ({start_d}) > end_date ({end_d})")

    print(f"\n[INFO] Processing window (inclusive): {iso(start_d)} -> {iso(end_d)}")

    start_lit = F.lit(iso(start_d)).cast("date")
    end_lit = F.lit(iso(end_d)).cast("date")

    purchase_df_w = purchase_df.filter((F.col("transaction_date") >= start_lit) & (F.col("transaction_date") <= end_lit))
    product_item_df_w = product_item_df.filter((F.col("transaction_date") >= start_lit) & (F.col("transaction_date") <= end_lit))
    extra_df_w = extra_df.filter((F.col("transaction_date") >= start_lit) & (F.col("transaction_date") <= end_lit))

    # Se a janela não tem dados, encerra de forma limpa
    if purchase_df_w.rdd.isEmpty():
        print("[WARN] No purchase data found for the selected window. Nothing to process.")
        spark.stop()
        return

    # -----------------------------
    # 4) CDC -> Silver
    # -----------------------------
    # para cada purchase_id, pega o evento mais recente (maior transaction_datetime)
    w_purchase = Window.partitionBy("purchase_id").orderBy(F.col("transaction_datetime").desc())
    purchase_silver = (
        purchase_df_w
        .withColumn("rn", F.row_number().over(w_purchase))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    w_item = Window.partitionBy("purchase_id").orderBy(F.col("transaction_datetime").desc())
    product_item_silver = (
        product_item_df_w
        .withColumn("rn", F.row_number().over(w_item))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    w_extra = Window.partitionBy("purchase_id").orderBy(F.col("transaction_datetime").desc())
    extra_silver = (
        extra_df_w
        .withColumn("rn", F.row_number().over(w_extra))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    if args.write_silver:
        purchase_silver.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{SILVER_BASE}/purchase")
        product_item_silver.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{SILVER_BASE}/product_item")
        extra_silver.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{SILVER_BASE}/purchase_extra_info")
        print("[INFO] Silver written to disk.")

    # -----------------------------
    # 5) Avoid ambiguous columns
    # -----------------------------
    purchase_silver = purchase_silver.withColumnRenamed("transaction_date", "purchase_transaction_date")
    product_item_silver = product_item_silver.withColumnRenamed("transaction_date", "item_transaction_date")
    extra_silver = extra_silver.withColumnRenamed("transaction_date", "extra_transaction_date")

    # -----------------------------
    # 6) Join + business rule
    # -----------------------------
    df_joined = (
        purchase_silver
        .join(product_item_silver, on="purchase_id", how="left")
        .join(extra_silver, on="purchase_id", how="left")
    )

    df_valid = df_joined.filter(F.col("release_date").isNotNull())

    # Se ninguém estiver pago na janela, encerra limpo
    if df_valid.rdd.isEmpty():
        print("[WARN] No paid purchases (release_date is null for all) in this window. Nothing to aggregate.")
        spark.stop()
        return

    # -----------------------------
    # 7) GMV
    # -----------------------------
    df_valid = df_valid.withColumn("gmv_value", F.col("purchase_value") * F.col("item_quantity"))

    daily_gmv = (
        df_valid
        .groupBy("purchase_transaction_date", "subsidiary")
        .agg(F.round(F.sum("gmv_value"), 2).alias("gmv"))
        .orderBy("purchase_transaction_date", "subsidiary")
    ).withColumnRenamed("purchase_transaction_date", "transaction_date")

    print("\n[INFO] Preview daily_gmv:")
    daily_gmv.show(200, truncate=False)

    # -----------------------------
    # 8) Gold write (append-only by day partitions)
    # -----------------------------
    (
        daily_gmv
        .write
        .mode("overwrite")  # com dynamic partition overwrite, só substitui os dias presentes no DF
        .partitionBy("transaction_date")
        .option("header", True)
        .csv(f"{GOLD_BASE}/daily_gmv")
    )

    print(f"\n[OK] Gold written to: {GOLD_BASE}/daily_gmv (partitioned by transaction_date)")
    print("[OK] ETL GMV completed successfully.\n")

    spark.stop()


if __name__ == "__main__":
    main()
