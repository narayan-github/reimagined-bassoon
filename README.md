# PySpark Optimization Pipeline — Ride-Sharing Analytics

## Overview
A PySpark pipeline that processes ride-sharing data and demonstrates key Spark optimization techniques with measured comparisons:
- **Broadcast joins** to avoid shuffle
- **Filter pushdown** to reduce data early
- **Partitioning strategies** for faster reads
- **Repartition vs Coalesce** comparison

## Datasets
| File | Description |
|------|-------------|
| `rides.csv` | Core ride records (ride_id, driver_id, city, fare, ride_date) |
| `drivers.csv` | Small driver lookup table |
| `rides_large.csv` | Larger (1,000-row) dataset for the repartition benchmark |

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

## Benchmarks & variations
- Broadcast vs normal join — timing comparison
- `trip_distance` column added with optimized aggregation
- repartition(4) tested on the larger dataset

## Run
```bash
pip install pyspark
python pipeline.py
```

## Output
- `output/no_partition/` — Parquet without partitioning
- `output/partitioned/`  — Parquet partitioned by city
- `output/final/`        — Final optimized pipeline output
