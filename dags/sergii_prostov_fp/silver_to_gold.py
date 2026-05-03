"""silver_to_gold.py — Stage 3 of the multi-hop datalake.

Reads silver/athlete_bio and silver/athlete_event_results, casts numeric columns
that ended up as strings (height, weight) into doubles, joins on athlete_id,
aggregates avg height/weight per (sport, medal, sex, country_noc), and writes
gold/avg_stats/.

Both source tables happen to share `country_noc`. The bio's `country_noc` is the
athlete's country of origin; the event's is the country they represented at the
event. The brief calls out country of origin as the relevant feature, so we keep
the bio's value and drop the event's `country_noc` before the join collision.

Usage:
    spark-submit silver_to_gold.py
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("silver_to_gold")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bio = spark.read.parquet("silver/athlete_bio")
    events = spark.read.parquet("silver/athlete_event_results")

    bio_clean = (
        bio
        .withColumn("height", F.col("height").cast("double"))
        .withColumn("weight", F.col("weight").cast("double"))
        .filter(F.col("height").isNotNull() & F.col("weight").isNotNull())
        .filter((F.col("height") > 0) & (F.col("weight") > 0))
        .select("athlete_id", "sex", "country_noc", "height", "weight")
    )

    # Drop event.country_noc so the join doesn't produce ambiguous columns;
    # bio.country_noc (origin) is the one used in the aggregation.
    events_slim = events.select("athlete_id", "sport", "medal")

    joined = events_slim.join(bio_clean, on="athlete_id", how="inner")

    gold = (
        joined
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            F.avg("height").alias("avg_height"),
            F.avg("weight").alias("avg_weight"),
            F.count(F.lit(1)).alias("athletes"),
        )
        .withColumn("timestamp", F.current_timestamp())
    )

    gold_path = "gold/avg_stats"
    print(f"writing {gold.count()} rows -> {gold_path}")
    gold.write.mode("overwrite").parquet(gold_path)

    gold.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
