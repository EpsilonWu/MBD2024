import re

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, FloatType

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
        "WhiteElo": 0,
        "BlackElo": 0,
        "Type": "",
        "StartingTime": 0,
        "Increment": 0,
        "ThinkingTime": 0,
        "MoveNumber": 0,
        "Move": "",
        "MoveColor": "",
        "Centipawns": 0.0
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
            schema['WhiteElo'] = int(value.split('"')[1])
        elif value.startswith('[BlackElo'):
            schema['BlackElo'] = int(value.split('"')[1])
        elif value.startswith('[TimeControl'):
            time_control = value.split('"')[1]
            # If the time control is not specified, mark the game as deleted
            if time_control == '-':
                schema['MarkAsDeleted'] = True
            else:
                schema['StartingTime'] = int(time_control.split('+')[0])
                schema['Increment'] = int(time_control.split('+')[1])
        elif value.startswith('1.'):
            if abs(schema['WhiteElo'] - schema['BlackElo']) > 200:
                schema['MarkAsDeleted'] = True
                yield list(schema.values())
                continue
            average_elo = (schema['WhiteElo'] + schema['BlackElo']) // 2
            if average_elo < 1000 or average_elo > 2000:
                schema['MarkAsDeleted'] = True
                yield list(schema.values())
                continue
            if schema['StartingTime'] < 600:
                schema['MarkAsDeleted'] = True
                yield list(schema.values())
                continue
            if schema['StartingTime'] == 600 or schema['StartingTime'] == 900:
                schema['Type'] = "Rapid"
            elif schema['StartingTime'] == 1800:
                schema['Type'] = "Classical"
            # Regex pattern to match move number, eval score, and clock time
            pattern = r"(\d+)(\.+)\s(\S+)\s{\s\[%eval (-?\d+(\.\d+)?)\]\s\[%clk (\d+):(\d+):(\d+)\]\s}"

            # Find all matches
            matches = re.findall(pattern, value)

            if len(matches) == 0:
                schema['MarkAsDeleted'] = True
                yield list(schema.values())
                continue

            white_prev_time = schema['StartingTime']
            black_prev_time = schema['StartingTime']
            increment = schema['Increment']
            for match in matches:
                move_number, dots, move, eval_score, _, hours, minutes, seconds = match
                is_white_move = dots == "."  # True for white moves, False for black moves
                move_color = "White" if is_white_move else "Black"
                total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)  # Convert clock time to seconds
                schema['MoveNumber'] = int(move_number)
                schema['Move'] = move
                schema['MoveColor'] = move_color
                if is_white_move:
                    schema['ThinkingTime'] = white_prev_time + increment - total_seconds
                    white_prev_time = total_seconds
                else:
                    schema['ThinkingTime'] = black_prev_time + increment - total_seconds
                    black_prev_time = total_seconds
                schema['Centipawns'] = float(eval_score)
                yield list(schema.values())


def spark_schema():
    return StructType([
        StructField("ID", StringType(), True),
        StructField("MarkAsDeleted", BooleanType(), True),
        StructField("White", StringType(), True),
        StructField("Black", StringType(), True),
        StructField("Result", StringType(), True),
        StructField("WhiteElo", IntegerType(), True),
        StructField("BlackElo", IntegerType(), True),
        StructField("Type", StringType(), True),
        StructField("StartingTime", IntegerType(), True),
        StructField("Increment", IntegerType(), True),
        StructField("ThinkingTime", IntegerType(), True),
        StructField("MoveNumber", IntegerType(), True),
        StructField("Move", StringType(), True),
        StructField("MoveColor", StringType(), True),
        StructField("Centipawns", FloatType(), True)
    ])


move_df = data.rdd.mapPartitions(lambda partition: create_schema(partition)).toDF(schema=spark_schema())

filter_df = move_df.filter(move_df.MarkAsDeleted == False)

filter_df.write.csv(f"hdfs:///user/s2706172/lichess_per_move.csv", mode="overwrite", header=True)

# Stop the Spark Session
spark.stop()
