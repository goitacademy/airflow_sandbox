from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp

def main():
    spark = SparkSession.builder.appName('Landing To Gold').getOrCreate()

    df_bio = spark.read.parquet('silver/athlete_bio')
    df_aer = spark.read.parquet('silver/athlete_event_results')

    df_bio = df_bio.drop('country_noc')

    joined_df = df_aer.join(df_bio, on='athlete_id', how='inner')

    agg_df = joined_df.groupby('sport', 'medal', 'sex', 'country_noc').agg(
        avg(col('weight')).alias('avg_weight'),
        avg(col('height')).alias('avg_height'),
    ).withColumn(
        'timestamp',
        current_timestamp()
    )

    (agg_df.write
    .mode('overwrite')
    .parquet('gold/avg_stats')
    )


if __name__ == '__main__':
    main()
