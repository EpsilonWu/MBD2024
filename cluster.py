from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

# Initialize Spark Session
spark = SparkSession.builder.appName("ClusterNormalized").getOrCreate()

# Load data
hdfs_pgn_file = "hdfs:///user/s2706172/lichess_per_move.csv"
data = spark.read.csv(hdfs_pgn_file, header=True, inferSchema=True)

# Feature Engineering: Assemble features into a vector
assembler = VectorAssembler(
    inputCols=["WhiteElo", "Centipawns", "ThinkingTime"],
    outputCol="features_unscaled"
)

# Normalize each dimension using MinMaxScaler
scaler = MinMaxScaler(inputCol="features_unscaled", outputCol="features")

# Clustering with KMeans
k = 10  # Number of clusters
kmeans = KMeans().setK(k).setSeed(1)

# Create a Pipeline
pipeline = Pipeline(stages=[assembler, scaler, kmeans])

# Fit the pipeline to perform the operations
model = pipeline.fit(data)

# Make predictions
predictions = model.transform(data)


# Define UDFs to extract each element of the feature vector
def extract_feature_value(index):
    # This function returns another function that will extract the index-th element of a vector
    return udf(lambda vector: float(vector[index]), FloatType())


# Now use these UDFs to create new columns in your DataFrame
predictions = predictions.withColumn("scaled_WhiteElo", extract_feature_value(0)("features")) \
                         .withColumn("scaled_Centipawns", extract_feature_value(1)("features")) \
                         .withColumn("scaled_ThinkingTime", extract_feature_value(2)("features"))

scaled_predictions = predictions.select(
    "scaled_WhiteElo",
    "scaled_Centipawns",
    "scaled_ThinkingTime",
    "prediction"
)

scaled_predictions.show()

# Save the scaled predictions
scaled_predictions.sample(False, 0.001).write.csv(
    "hdfs:///user/s2706172/scaled_cluster_results.csv", header=True, mode="overwrite"
)

# Extract cluster centers (from the KMeans model within the Pipeline)
kmeansModel = model.stages[-1]  # KMeansModel is the last stage in the pipeline
centers = kmeansModel.clusterCenters()
centers_df = spark.createDataFrame([(i, float(center[0]), float(center[1]), float(center[2])) for i, center in enumerate(centers)], ["cluster", "WhiteElo", "Centipawns", "ThinkingTime"])

centers_df.show()

# Save the cluster centers
centers_df.write.csv("hdfs:///user/s2706172/cluster_centers_normalized.csv", header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
