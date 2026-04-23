import os
import time
import ray
import ray.data
import pandas as pd

HEAD_ADDRESS = "ray://MASTER_IP:10001"
DATA_PATH = "/Users/YOUR_USERNAME/a8_project/data/nyc_taxi/"
LOCATION_PATH = "/Users/YOUR_USERNAME/a8_project/data/taxi_zone_lookup.csv"
OUTPUT_PATH = "/Users/YOUR_USERNAME/a8_project/data/output/ray_result"

ray.init(address=HEAD_ADDRESS)


# 1. Ingest 
t0 = time.time()
trips_ds = ray.data.read_parquet(DATA_PATH)
locations_df = pd.read_csv(LOCATION_PATH)
t_ingest = time.time() - t0
print(f"[TIMER] Ingest: {t_ingest:.2f}s")



# 2. Cleanse 
t1 = time.time()

def cleanse_batch(batch: pd.DataFrame) -> pd.DataFrame:
    batch = batch.dropna().drop_duplicates()
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        batch[col] = pd.to_datetime(batch[col], errors="coerce")
    return batch.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

trips_ds = trips_ds.map_batches(cleanse_batch, batch_format="pandas")
t_cleanse = time.time() - t1
print(f"[TIMER] Cleanse (lazy): {t_cleanse:.4f}s")



# 3. Heavy Join + UDF 
t2 = time.time()
locations_ref = ray.put(locations_df)

def join_and_transform(batch: pd.DataFrame) -> pd.DataFrame:
    locs = ray.get(locations_ref)
    locs = locs.rename(columns={"LocationID": "PULocationID"})
    batch = batch.merge(locs, on="PULocationID", how="left")

    def avg_speed(row):
        try:
            hours = (row["tpep_dropoff_datetime"] - row["tpep_pickup_datetime"]).total_seconds() / 3600
            return row["trip_distance"] / hours if hours > 0 else 0.0
        except:
            return 0.0

    batch["avg_speed_mph"] = batch.apply(avg_speed, axis=1)
    return batch

trips_ds = trips_ds.map_batches(join_and_transform, batch_format="pandas")
trips_ds = trips_ds.materialize()
t_transform = time.time() - t2
print(f"[TIMER] Join + UDF (materialised): {t_transform:.2f}s")



# 4. Export 
t3 = time.time()
trips_ds.write_parquet(OUTPUT_PATH)
t_export = time.time() - t3
print(f"[TIMER] Export: {t_export:.2f}s")

total = time.time() - t0
print(f"\n[TOTAL WALL CLOCK] {total:.2f}s")
ray.shutdown()

