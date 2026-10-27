# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")
dbutils.widgets.text("volume_name", "raw", "Volume Name")

catalog       = dbutils.widgets.get("catalog")
data_source   = dbutils.widgets.get("data_source")
volume_name   = dbutils.widgets.get("volume_name")

bronze_schema, silver_schema, gold_schema = "bronze", "silver", "gold"

base_path      = f"/Volumes/{catalog}/{bronze_schema}/{volume_name}/{data_source}"
landing_path   = f"{base_path}/landing/"
processed_path = f"{base_path}/processed/"

bronze_table   = f"{catalog}.{bronze_schema}.{data_source}"
silver_table   = f"{catalog}.{silver_schema}.{data_source}"
gold_table     = f"{catalog}.{gold_schema}.sb_fact_{data_source}"      # child daily fact
parent_table   = f"{catalog}.{gold_schema}.fact_{data_source}"         # parent monthly fact

staging_bronze = f"{catalog}.{bronze_schema}.staging_{data_source}"
staging_silver = f"{catalog}.{silver_schema}.staging_{data_source}"

print(landing_path, processed_path, sep="\n")

# COMMAND ----------

files = dbutils.fs.ls(landing_path)
csv_files = [f for f in files if f.name.endswith(".csv")]
print("New files:", len(csv_files))

if len(csv_files) == 0:
    dbutils.notebook.exit("No new files in landing. Nothing to process.")

# COMMAND ----------

df = (spark.read.options(header=True, inferSchema=True).csv(f"{landing_path}*.csv")
      .withColumn("read_timestamp", F.current_timestamp())
      .select("*", "_metadata.file_name", "_metadata.file_size"))

print("Total Rows:", df.count())
df.show(5)

# COMMAND ----------

(df.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .mode("append")
   .saveAsTable(bronze_table))

# COMMAND ----------

(df.write.format("delta")
   .option("delta.enableChangeDataFeed", "true")
   .mode("overwrite")
   .saveAsTable(staging_bronze))

# COMMAND ----------

for f in csv_files:
    dbutils.fs.mv(f.path, f"{processed_path}{f.name}", True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Reads Staging Data

# COMMAND ----------

df_orders = spark.table(staging_bronze)
df_orders.show(25)

# COMMAND ----------

# 1. Keep rows where order_qty is present
df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. customer_id: keep numeric, else 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").cast("string").rlike("^[0-9]+$"), F.col("customer_id").cast("string"))
     .otherwise("999999")
)

# 3. Remove weekday name: "Tuesday, July 01, 2025" -> "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date").cast("string"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse date from multiple formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(
    ["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"]
)

# 6. product_id as string
df_orders = df_orders.withColumn("product_id", F.col("product_id").cast("string"))

# COMMAND ----------

df_orders.agg(F.min("order_placement_date").alias("min_date"),
              F.max("order_placement_date").alias("max_date")).show()

# COMMAND ----------

df_products = spark.table(f"{catalog}.{silver_schema}.products")

df_joined = (df_orders.join(df_products, on="product_id", how="inner")
             .select(df_orders["*"], df_products["product_code"]))

df_joined.show(5)

# COMMAND ----------

from pyspark.sql.window import Window

merge_keys = ["order_placement_date", "order_id", "product_code", "customer_id"]

w = Window.partitionBy(*merge_keys).orderBy(F.col("file_name").desc())

df_joined = (df_joined
             .withColumn("rn", F.row_number().over(w))
             .filter("rn = 1")
             .drop("rn"))

print("Rows after dedupe:", df_joined.count())

# COMMAND ----------

if not spark.catalog.tableExists(silver_table):
    (df_joined.write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("mergeSchema", "true")
        .mode("overwrite").saveAsTable(silver_table))
else:
    (DeltaTable.forName(spark, silver_table).alias("silver")
        .merge(df_joined.alias("new"),
               """silver.order_placement_date = new.order_placement_date
                  AND silver.order_id = new.order_id
                  AND silver.product_code = new.product_code
                  AND silver.customer_id = new.customer_id""")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Staging table

# COMMAND ----------

(df_joined.write.format("delta")
    .option("delta.enableChangeDataFeed", "true")
    .mode("overwrite")
    .saveAsTable(staging_silver))

# COMMAND ----------

df_gold = spark.sql(f"""
    SELECT order_id,
           order_placement_date AS date,
           customer_id          AS customer_code,
           product_code,
           product_id,
           order_qty            AS sold_quantity
    FROM {staging_silver}
""")
print(df_gold.count())
df_gold.show(2)

# COMMAND ----------

if not spark.catalog.tableExists(gold_table):
    print("Creating new table")
    (df_gold.write.format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .option("mergeSchema", "true")
        .mode("overwrite").saveAsTable(gold_table))
else:
    (DeltaTable.forName(spark, gold_table).alias("gold")
        .merge(df_gold.alias("new"),
               """gold.date = new.date
                  AND gold.order_id = new.order_id
                  AND gold.product_code = new.product_code
                  AND gold.customer_code = new.customer_code""")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge into the parent (monthly)

# COMMAND ----------

df_child = spark.sql(f"SELECT order_placement_date AS date FROM {staging_silver}")

incremental_month_df = df_child.select(F.trunc("date", "MM").alias("start_month")).distinct()
incremental_month_df.show()
incremental_month_df.createOrReplaceTempView("incremental_months")

# COMMAND ----------

monthly_table = spark.sql(f"""
    SELECT sbf.date, sbf.product_code, sbf.customer_code, sbf.sold_quantity
    FROM {gold_table} sbf
    INNER JOIN incremental_months m
        ON trunc(sbf.date, 'MM') = m.start_month
""")

print("Total Rows:", monthly_table.count())
monthly_table.select("date").distinct().orderBy("date").show()

# COMMAND ----------

df_monthly_recalc = (
    monthly_table
    .withColumn("month_start", F.trunc("date", "MM"))
    .groupBy("month_start", "product_code", "customer_code")
    .agg(F.sum("sold_quantity").alias("sold_quantity"))
    .withColumnRenamed("month_start", "date")
)

df_monthly_recalc.show(10, truncate=False)
print(df_monthly_recalc.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merge into Parent

# COMMAND ----------

(DeltaTable.forName(spark, parent_table).alias("parent")
    .merge(df_monthly_recalc.alias("child"),
           """parent.date = child.date
              AND parent.product_code = child.product_code
              AND parent.customer_code = child.customer_code""")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {staging_bronze}")
spark.sql(f"DROP TABLE IF EXISTS {staging_silver}")