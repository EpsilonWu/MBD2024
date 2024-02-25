from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType

# Initialize Spark Session
spark = SparkSession.builder.appName("Preprocess").getOrCreate()

hdfs_pgn_file = f"hdfs:///user/s3262642/lichess_db_standard_rated_2023-05.pgn"
data = spark.read.option("delimiter", "[Event").text(hdfs_pgn_file)


# Define the schema
def init_schema():
    return {
        "ID": "",
        "MarkAsDeleted": False,
        "White": "",
        "Black": "",
        "Result": "",
        "WhiteEloRange": 0,
        "BlackEloRange": 0,
        "WhiteRatingDiff": 0,
        "BlackRatingDiff": 0,
    }


def create_schema(partition):
    schema = init_schema()
    for row in partition:
        value = row.value
        if value.startswith('[Event'):
            schema = init_schema()
        elif value.startswith('[Site'):
            schema['ID'] = value.split('"')[1].split('/')[-1]
        elif value.startswith('[White '):
            schema['White'] = value.split('"')[1]
        elif value.startswith('[Black '):
            schema['Black'] = value.split('"')[1]
        elif value.startswith('[Result'):
            schema['Result'] = value.split('"')[1]
        elif value.startswith('[WhiteElo'):
            schema['WhiteEloRange'] = int(value.split('"')[1]) // 100
        elif value.startswith('[BlackElo'):
            schema['BlackEloRange'] = int(value.split('"')[1]) // 100
        elif value.startswith('[WhiteRatingDiff'):
            schema['WhiteRatingDiff'] = int(value.split('"')[1])
        elif value.startswith('[BlackRatingDiff'):
            schema['BlackRatingDiff'] = int(value.split('"')[1])
        elif value.startswith('1.'):
            if abs(schema['WhiteEloRange'] - schema['BlackEloRange']) > 2:
                schema['MarkAsDeleted'] = True
            average_elo = (schema['WhiteEloRange'] + schema['BlackEloRange']) // 2
            if average_elo < 8 or average_elo > 22:
                schema['MarkAsDeleted'] = True
            yield list(schema.values())



def spark_schema():
    return StructType([
        StructField("ID", StringType(), True),
        StructField("MarkAsDeleted", BooleanType(), True),
        StructField("White", StringType(), True),
        StructField("Black", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("WhiteEloRange", IntegerType(), True),
        StructField("BlackEloRange", IntegerType(), True),
        StructField("WhiteRatingDiff", IntegerType(), True),
        StructField("BlackRatingDiff", IntegerType(), True),
    ])


move_df = data.rdd.mapPartitions(lambda partition: create_schema(partition)).toDF(schema=spark_schema())

filter_df = move_df.filter(move_df.MarkAsDeleted == False)

filter_df.persist()

white_plays = filter_df.select(F.col("White").alias("player"),
                               F.col("WhiteEloRange").alias("eloRange"),
                               F.col("WhiteRatingDiff").alias("ratingDiff"),
                               F.lit("White").alias("color"),
                               F.when(F.col("Result") == "1-0", 1).otherwise(0).alias("win"))
black_plays = filter_df.select(F.col("Black").alias("player"),
                               F.col("BlackEloRange").alias("eloRange"),
                               F.col("BlackRatingDiff").alias("ratingDiff"),
                               F.lit("Black").alias("color"),
                               F.when(F.col("Result") == "0-1", 1).otherwise(0).alias("win"))

# Combine the dataframes
all_plays = white_plays.union(black_plays)

all_plays.persist()

winrate_stats = all_plays.groupBy("eloRange").agg(
    F.count("*").alias("total_games"),
    F.sum("win").alias("total_wins"),
    (F.sum("win") / F.count("*")).alias("winRate"),
).orderBy("eloRange")

winrate_stats.show(50)

player_stats = all_plays.groupBy("player").agg(
    F.floor(F.avg("eloRange")).alias("average_elo_range"),
    F.sum("ratingDiff").alias("total_rating_diff")
)

progress_stats = player_stats.groupBy("average_elo_range").agg(
    F.avg("total_rating_diff").alias("average_rating_diff"),
    F.stddev("total_rating_diff").alias("stddev_rating_diff"),
    F.percentile_approx("total_rating_diff", 0.95).alias("95th_percentile_rating_diff"),
    F.percentile_approx("total_rating_diff", 0.05).alias("5th_percentile_rating_diff"),
).orderBy("average_elo_range")

progress_stats.show(30)

# Stop the Spark Session
spark.stop()
