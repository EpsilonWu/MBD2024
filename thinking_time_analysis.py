from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, stddev, percentile_approx
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Preprocess").getOrCreate()

# Read data from
hdfs_pgn_file = f"hdfs:///user/s2706172/lichess_per_move.csv"
data = spark.read.csv(hdfs_pgn_file, header=True, inferSchema=True)

# Compute the Average Elo
# data = data.withColumn("EloRange", F.floor(data["WhiteElo"] / 200) * 200)

# Calculate statistics for thinking times per move number
stats_df = data.groupBy("MoveNumber", "MoveColor", "StartingTime").agg(
    avg("ThinkingTime").alias("AverageThinkingTime"),
    percentile_approx("ThinkingTime", 0.9).alias("90thPercentileThinkingTime"),
    percentile_approx("ThinkingTime", 0.1).alias("10thPercentileThinkingTime"),
    # max("ThinkingTime").alias("MaxThinkingTime"),
    # min("ThinkingTime").alias("MinThinkingTime"),
    stddev("ThinkingTime").alias("StdDevThinkingTime")
).orderBy("MoveNumber", "MoveColor", "StartingTime")

# save to csv
stats_df.write.csv(f"hdfs:///user/s2706172/thinking_time_stats.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()