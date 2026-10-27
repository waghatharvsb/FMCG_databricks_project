# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

catalog = "fmcg"
bronze_table     = f"{catalog}.bronze.gross_price"
silver_table     = f"{catalog}.silver.gross_price"
gold_child_table = f"{catalog}.gold.sb_dim_gross_price"
parent_table     = f"{catalog}.gold.dim_gross_price"
products_silver  = f"{catalog}.silver.products"

# COMMAND ----------

volume_path = "/Volumes/fmcg/bronze/landing/child/gross_price/"

# COMMAND ----------

if spark.catalog.tableExists(bronze_table):
    print(f"{bronze_table} already exists - skipping load")
else:
    df_raw = (
        spark.read.format("csv")
            .option("header", True)
            .option("inferSchema", False)
            .load(volume_path)
            .withColumn("read_timestamp", F.current_timestamp())
            .select("*", "_metadata.file_name", "_metadata.file_size")
    )
    (df_raw.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .saveAsTable(bronze_table))
    print(f"Loaded {df_raw.count()} rows into {bronze_table}")

# COMMAND ----------

df_bronze = spark.table(bronze_table)
df_bronze.printSchema()
print("Bronze rows:", df_bronze.count())       
display(df_bronze.limit(10))

# COMMAND ----------

df_bronze.select("month").distinct().show(30, truncate=False)

# COMMAND ----------

df_silver = df_bronze.select(
    F.trim(F.col("product_id").cast("string")).alias("product_id"),
    F.trim(F.col("month").cast("string")).alias("month_raw"),
    F.trim(F.col("gross_price").cast("string")).alias("gross_price_raw"),
)
 
df_silver = df_silver.withColumn(
    "month",
    F.coalesce(
        F.try_to_date("month_raw", "yyyy/MM/dd"),
        F.try_to_date("month_raw", "dd/MM/yyyy"),
        F.try_to_date("month_raw", "yyyy-MM-dd"),
        F.try_to_date("month_raw", "dd-MM-yyyy"),
    ),
)

display(df_silver.limit(10))

# COMMAND ----------

print("Unparsed months (expect 0):", df_silver.filter(F.col("month").isNull()).count())
df_silver.select("month").distinct().orderBy("month").show() 

# COMMAND ----------

# Handling the gross_price column
is_numeric = F.col("gross_price_raw").rlike(r"^-?\d+(\.\d+)?$")
 
df_silver = df_silver.withColumn(
    "gross_price",
    F.when(is_numeric, F.abs(F.col("gross_price_raw").cast("double")))
)

# COMMAND ----------

(df_silver.filter(F.col("gross_price").isNull())
    .groupBy("gross_price_raw").count().show())

# COMMAND ----------

df_products = (
    spark.table(products_silver)
        .select(F.col("product_id").cast("string").alias("product_id"), "product_code")
)

# COMMAND ----------

df_unmatched = df_silver.join(df_products, on="product_id", how="left_anti")
df_unmatched.groupBy("product_id").count().orderBy("product_id").show()

# COMMAND ----------

df_silver = (
    df_silver
        .join(df_products, on="product_id", how="inner")
        .select("product_id", "product_code", "month", "gross_price")
        .withColumn("processed_timestamp", F.current_timestamp())
)
 
print("Silver rows:", df_silver.count())

# COMMAND ----------

dups = df_silver.groupBy("product_code", "month").count().filter("count > 1")
print("Duplicate product_code + month:", dups.count())

# COMMAND ----------

(df_silver.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable(silver_table))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Gold data processing 
# MAGIC

# COMMAND ----------

df_silver = spark.table(silver_table)
df_gold = df_silver.select("product_code", "month", "gross_price")
df_gold.show(5)


# COMMAND ----------

(df_gold.write
    .format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .option("overwriteSchema", "true")
    .mode("overwrite")
    .saveAsTable(gold_child_table))

# COMMAND ----------

df_price = spark.table(gold_child_table).withColumn("year", F.year("month"))
display(df_price.limit(5))

# COMMAND ----------

w = Window.partitionBy("product_code", "year").orderBy(F.col("month").desc())
 
df_latest_price = (
    df_price
        .filter(F.col("gross_price").isNotNull())      # months with no valid price can't be used
        .withColumn("rnk", F.row_number().over(w))
        .filter(F.col("rnk") == 1)
        .select(
            "product_code",
            F.col("gross_price").alias("price_inr"),
            F.col("year").cast("string").alias("year"),   # parent stores year as string
        )
)

# COMMAND ----------

df_latest_price.show(5)
df_latest_price.printSchema()
print("Rows to merge:", df_latest_price.count())

# COMMAND ----------

spark.sql(f"DESCRIBE {parent_table}").show()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, parent_table)
 
(delta_table.alias("target")
    .merge(
        source=df_latest_price.alias("source"),
        # match on product AND year, so one year's price never overwrites another year's
        condition="target.product_code = source.product_code AND target.year = source.year"
    )
    .whenMatchedUpdate(set={"price_inr": "source.price_inr"})
    .whenNotMatchedInsert(values={
        "product_code": "source.product_code",
        "price_inr":    "source.price_inr",
        "year":         "source.year",
    })
    .execute())

# COMMAND ----------

display(delta_table.history(1).select("operation", "operationMetrics"))

# COMMAND ----------

spark.sql(f"SELECT year, COUNT(*) AS products FROM {parent_table} GROUP BY year ORDER BY year").show()
 
# Every child product should now have a row in the parent
child_codes = spark.table(gold_child_table).select("product_code").distinct()
in_parent = child_codes.join(spark.table(parent_table).select("product_code").distinct(), "product_code")
print(f"Child products found in parent: {in_parent.count()} of {child_codes.count()}")