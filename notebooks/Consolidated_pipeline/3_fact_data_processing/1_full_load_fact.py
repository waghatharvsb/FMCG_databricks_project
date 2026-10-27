# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

bronze_schema = "bronze"
silver_schema = "silver"
gold_schema   = "gold"

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog     = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

volume_name    = "raw"
base_path      = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/{data_source}"
landing_path   = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

bronze_table      = f"{catalog}.{bronze_schema}.{data_source}"
silver_table      = f"{catalog}.{silver_schema}.{data_source}"
gold_table        = f"{catalog}.{gold_schema}.sb_fact_{data_source}"
parent_gold_table = f"{catalog}.{gold_schema}.fact_orders"   

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{bronze_schema}.{volume_name}")
dbutils.fs.mkdirs(landing_path)
dbutils.fs.mkdirs(processed_path)
display(dbutils.fs.ls(landing_path))

# COMMAND ----------

for f in dbutils.fs.ls(processed_path):
    if f.name.endswith(".csv"):
        dbutils.fs.mv(f.path, f"{landing_path}{f.name}", True)

display(dbutils.fs.ls(landing_path))

# COMMAND ----------

landing_files = [f for f in dbutils.fs.ls(landing_path) if f.name.endswith(".csv")]
assert len(landing_files) > 0, "No CSV files in landing folder"

df = (
    spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}*.csv")
    .withColumn("read_timestamp", F.current_timestamp())
    .select("*", "_metadata.file_name", "_metadata.file_size")
)

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

spark.sql(f"TRUNCATE TABLE {bronze_table}")

# COMMAND ----------

df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .mode("append") \
    .saveAsTable(bronze_table)

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
for file_info in files:
    dbutils.fs.mv(file_info.path, f"{processed_path}{file_info.name}", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Silver Processing

# COMMAND ----------

df_orders = spark.sql(f"SELECT * FROM {bronze_table}")
df_orders.show(2)

# COMMAND ----------

# 1. keep only rows where order_qty is present
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. clean customer_id: keep numeric, else 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
     .otherwise("999999")
     .cast("string")
)

# 3. remove weekday name: "Tuesday, July 01, 2025" -> "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. parse date using multiple formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. drop duplicates
df_orders = df_orders.dropDuplicates(
    ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]
)

# 6. product_id as string
df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date"),
    F.sum(F.when(F.col("order_placement_date").isNull(), 1).otherwise(0)).alias("unparsed_dates")
).show()

# COMMAND ----------

df_products = spark.table(f"{catalog}.{silver_schema}.products")
df_joined = (
    df_orders.join(df_products, on="product_id", how="inner")
             .select(df_orders["*"], df_products["product_code"])
)
df_joined.show(5)

# COMMAND ----------

key_cols = ["order_placement_date", "order_id", "product_code", "customer_id"]

# sum quantity; keep one value for every other column (product_id, file_name, read_timestamp, ...)
agg_exprs = [F.sum("order_qty").alias("order_qty")] + [
    F.first(c).alias(c) for c in df_joined.columns if c not in key_cols + ["order_qty"]
]

df_silver_src = df_joined.groupBy(*key_cols).agg(*agg_exprs)

print("rows before:", df_joined.count(), "| after:", df_silver_src.count())

# must show 0 rows
df_silver_src.groupBy(*key_cols).count().filter("count > 1").show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Write to Silver
# MAGIC

# COMMAND ----------

spark.sql(f"""
    SELECT order_id, order_placement_date, product_id, customer_id,
           order_qty, file_name, read_timestamp
    FROM {bronze_table}
    WHERE order_id IN ('FDEC844501603', 'FDEC844221503', 'FDEC844422402')
    ORDER BY order_id, file_name
""").show(truncate=False)

# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
    df_silver_src.write.format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(
        df_silver_src.alias("bronze"),
        "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Next to Gold

# COMMAND ----------

df_gold = spark.sql(f"""
    SELECT order_id,
           order_placement_date AS date,
           customer_id AS customer_code,
           product_code,
           product_id,
           order_qty AS sold_quantity
    FROM {silver_table}
""")
df_gold.show(2)

# COMMAND ----------

if not spark.catalog.tableExists(gold_table):
    df_gold.write.format("delta") \
        .option("delta.enableChangeDataFeed", "true") \
        .option("mergeSchema", "true") \
        .mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("gold").merge(
        df_gold.alias("source"),
        "gold.date = source.date AND gold.order_id = source.order_id AND gold.product_code = source.product_code AND gold.customer_code = source.customer_code"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Merge with Parent Company

# COMMAND ----------

df_child = spark.sql(f"SELECT date, product_code, customer_code, sold_quantity FROM {gold_table}")
df_child.show(10)
print(df_child.count())

# COMMAND ----------

df_monthly = (
    df_child
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)
df_monthly.show(5, truncate=False)
print(df_monthly.count())

# COMMAND ----------

spark.table(parent_gold_table).printSchema()
df_monthly.printSchema()

# COMMAND ----------

gold_parent_delta = DeltaTable.forName(spark, parent_gold_table)
gold_parent_delta.alias("parent_gold").merge(
    df_monthly.alias("child_gold"),
    "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()