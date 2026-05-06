# Lab 7 – PySpark Optimization | Ride-Sharing Analytics

## Objective
Build and optimize a PySpark pipeline to process ride-sharing data using:
- **Broadcast joins** to avoid shuffle
- **Filter pushdown** to reduce data early
- **Partitioning strategies** for faster reads
- **Repartition vs Coalesce** comparison

## Datasets
| File | Description |
|------|-------------|
| `rides.csv` | Core ride records (ride_id, driver_id, city, fare, ride_date) |
| `drivers.csv` | Small driver lookup table |
| `rides_large.csv` | 1000-row dataset for Exercise 3 repartition test |

## Steps Covered
| Step | Description |
|------|-------------|
| 1 | Spark session setup |
| 2 | Load CSV data with inferSchema |
| 3 | Unoptimized join (baseline — causes shuffle) |
| 4 | Broadcast join (optimized — no shuffle) |
| 5 | Filter before join (reduce data early) |
| 6 | Aggregation — revenue & ride count per city |
| 7 | Partitioning: no_partition vs partitionBy(city) |
| 8 | Repartition vs Coalesce |
| 10 | Full optimized pipeline end-to-end |

## Performance Summary
| Approach | Performance |
|----------|-------------|
| Normal join | Slow — full shuffle |
| Broadcast join | Fast — no shuffle |
| Partitioned data | Faster reads — reduced I/O |

## Exercises
- **Exercise 1:** Remove broadcast → compare timing
- **Exercise 2:** Add `trip_distance` column and optimize aggregation
- **Exercise 3:** Load 1000-row dataset → test repartition(4)

## Run
```bash
pip install pyspark
python pipeline.py
```

## Output
- `output/no_partition/` — Parquet without partitioning
- `output/partitioned/`  — Parquet partitioned by city
- `output/final/`        — Final optimized pipeline output
