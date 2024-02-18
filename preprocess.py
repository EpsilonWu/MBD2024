import re

from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder.appName("Preprocess").getOrCreate()

hdfs_pgn_file = f"hdfs:///user/s2706172/lichess-dataset/lichess_db_standard_rated_2023-01.pgn"
data = spark.read.option("delimiter", "[Event").text(hdfs_pgn_file)

# Define the schema
move_schema = {
    "MarkAsDeleted": False,
    "White": "",
    "Black": "",
    "Result": "",
    "WhiteElo": 0,
    "BlackElo": 0,
    "StartingTime": 0,
    "Increment": 0,
    "ThinkingTime": 0,
    "MoveNumber": 0,
    "Move": "",
    "MoveColor": "",
    "Centipawns": 0.0,
}


def create_schema(partition, schema):
    for row in partition:
        value = row.value
        if value.startswith('[White'):
            schema['White'] = value.split('"')[1]
        elif value.startswith('[Black'):
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
            # Regex pattern to match move number, eval score, and clock time
            pattern = r"(\d+)(\.+)\s(\S+)\s{\s\[%eval (-?\d+(\.\d+)?)\]\s\[%clk (\d+):(\d+):(\d+)\]\s}"

            # Find all matches
            matches = re.findall(pattern, value)

            if len(matches) == 0:
                schema['MarkAsDeleted'] = True
                yield list(schema.values())

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


move_df = data.rdd.mapPartitions(lambda partition: create_schema(partition, move_schema)).toDF(list(move_schema.keys()))
move_df.persist()
move_df.take(10)

# Filter out games that are marked as deleted
move_df.filter(move_df.MarkAsDeleted == False).show(10, False)

# Print total number of moves
print(f"Total number of moves: {move_df.count()}")

# Stop the Spark Session
spark.stop()
