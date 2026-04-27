import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))

def main():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import avg, col, current_timestamp

    spark = SparkSession.builder.appName('Landing To Gold').getOrCreate()

    df_bio = spark.read.parquet(f'{DAG_DIR}/silver/athlete_bio')
    df_aer = spark.read.parquet(f'{DAG_DIR}/silver/athlete_event_results')

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
    .parquet(f'{DAG_DIR}/gold/avg_stats')
    )

    spark.stop()

if __name__ == '__main__':
    main()
