import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, sum as _sum, col, round as _round, lit

# ─────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RideShareAnalytics")
    .config("spark.sql.shuffle.partitions", "4")   # keep it small for local runs
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("✅ Spark session started\n")


# ─────────────────────────────────────────────
# STEP 2: Load Data
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 2: Load Data")
print("=" * 55)
rides   = spark.read.csv("rides.csv",   header=True, inferSchema=True)
drivers = spark.read.csv("drivers.csv", header=True, inferSchema=True)

print("rides schema:")
rides.printSchema()
print("drivers schema:")
drivers.printSchema()


# ─────────────────────────────────────────────
# STEP 3: Unoptimized Join (Baseline)
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 3: Unoptimized Join (Baseline — causes shuffle)")
print("=" * 55)
t0 = time.time()
joined_df = rides.join(drivers, "driver_id")
joined_df.show()
print(f"⏱  Unoptimized join time: {time.time() - t0:.3f}s")
print("❌ Shuffles data across all partitions — slow on large data\n")


# ─────────────────────────────────────────────
# STEP 4: Broadcast Join (Optimized)
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 4: Broadcast Join (Optimized)")
print("=" * 55)
t0 = time.time()
optimized_df = rides.join(broadcast(drivers), "driver_id")
optimized_df.show()
print(f"⏱  Broadcast join time: {time.time() - t0:.3f}s")
print("✅ Small table (drivers) broadcast to all nodes — no shuffle\n")


# ─────────────────────────────────────────────
# STEP 5: Filter Before Join
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 5: Filter Before Join (Reduce data early)")
print("=" * 55)
rides_blr = rides.filter(rides.city == "Bangalore")
optimized_blr = rides_blr.join(broadcast(drivers), "driver_id")
optimized_blr.show()
print("✅ Filter pushdown reduces data size before join\n")


# ─────────────────────────────────────────────
# STEP 6: Aggregation
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 6: Revenue Aggregation per City")
print("=" * 55)
revenue = (
    optimized_df
    .groupBy("city")
    .agg(_sum("fare").alias("total_revenue"),
         _sum(lit(1)).alias("total_rides"))
)
revenue.show()


# ─────────────────────────────────────────────
# STEP 7: Partitioning Strategy
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 7: Partitioning Strategy")
print("=" * 55)

import shutil, os
for p in ["output/no_partition", "output/partitioned", "output/final"]:
    shutil.rmtree(p, ignore_errors=True)

# Without partitioning
optimized_df.write.parquet("output/no_partition")
print("❌ Written without partitioning → full scan on every read")

# With partitioning
optimized_df.write.partitionBy("city").parquet("output/partitioned")
print("✅ Written with partitionBy(city) → faster city-level reads\n")


# ─────────────────────────────────────────────
# STEP 8: Repartition vs Coalesce
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 8: Repartition vs Coalesce")
print("=" * 55)
print(f"Original partitions : {rides.rdd.getNumPartitions()}")

rides_rep = rides.repartition(4)
print(f"After repartition(4): {rides_rep.rdd.getNumPartitions()} — more parallelism")

rides_coal = rides.coalesce(2)
print(f"After coalesce(2)   : {rides_coal.rdd.getNumPartitions()} — fewer partitions, no shuffle")

print("""
| Method      | Use Case                          |
|-------------|-----------------------------------|
| repartition | Large data, need more parallelism |
| coalesce    | Reduce partitions, avoid shuffle  |
""")


# ─────────────────────────────────────────────
# STEP 10: Full Optimized Pipeline
# ─────────────────────────────────────────────
print("=" * 55)
print("STEP 10: Full Optimized Pipeline")
print("=" * 55)
shutil.rmtree("output/final", ignore_errors=True)

rides_f   = spark.read.csv("rides.csv",   header=True, inferSchema=True)
drivers_f = spark.read.csv("drivers.csv", header=True, inferSchema=True)

rides_filtered = rides_f.filter(rides_f.city == "Bangalore")
df = rides_filtered.join(broadcast(drivers_f), "driver_id")
result = df.groupBy("city").agg(_sum("fare").alias("total_revenue"))
result.write.partitionBy("city").parquet("output/final")
result.show()
print("✅ Full pipeline complete — output/final written\n")


# ─────────────────────────────────────────────
# EXERCISE 1: Remove broadcast → performance diff
# ─────────────────────────────────────────────
print("=" * 55)
print("EXERCISE 1: Broadcast vs No-Broadcast comparison")
print("=" * 55)

t0 = time.time()
rides.join(drivers, "driver_id").count()
t_no_bc = time.time() - t0

t0 = time.time()
rides.join(broadcast(drivers), "driver_id").count()
t_bc = time.time() - t0

print(f"  No broadcast : {t_no_bc:.3f}s")
print(f"  Broadcast    : {t_bc:.3f}s")
print(f"  {'✅ Broadcast faster!' if t_bc < t_no_bc else 'ℹ️  Difference minimal on small data (expected on local mode)'}\n")


# ─────────────────────────────────────────────
# EXERCISE 2: Add trip_distance column & optimize
# ─────────────────────────────────────────────
print("=" * 55)
print("EXERCISE 2: Add trip_distance column & optimize")
print("=" * 55)
import random
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Simulate trip_distance from fare (fare / 10 as proxy)
rides_ex2 = rides.withColumn("trip_distance", _round(col("fare") / lit(10.0), 2))
df_ex2 = rides_ex2.join(broadcast(drivers), "driver_id")

revenue_ex2 = (
    df_ex2
    .groupBy("city", "driver_name")
    .agg(
        _sum("fare").alias("total_fare"),
        _sum("trip_distance").alias("total_km")
    )
    .orderBy("city")
)
revenue_ex2.show()
print("✅ trip_distance added and aggregated per city & driver\n")


# ─────────────────────────────────────────────
# EXERCISE 3: Large dataset + repartition test
# ─────────────────────────────────────────────
print("=" * 55)
print("EXERCISE 3: Large Dataset — Repartition Performance")
print("=" * 55)
rides_large = spark.read.csv("rides_large.csv", header=True, inferSchema=True)
print(f"Loaded {rides_large.count()} rows")

t0 = time.time()
rides_large.groupBy("city").agg(_sum("fare")).collect()
t_default = time.time() - t0

rides_rep4 = rides_large.repartition(4)
t0 = time.time()
rides_rep4.groupBy("city").agg(_sum("fare")).collect()
t_rep4 = time.time() - t0

print(f"  Default partitions ({rides_large.rdd.getNumPartitions()}): {t_default:.3f}s")
print(f"  Repartition(4)    ({rides_rep4.rdd.getNumPartitions()}): {t_rep4:.3f}s")
print("✅ Repartition test complete\n")

spark.stop()
print("✅ Spark session stopped")
