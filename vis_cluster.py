import glob

import matplotlib.pyplot as plt
import pandas as pd


# Function to safely read and concatenate CSV files
def safe_concat(files):
    dfs = []  # Initialize an empty list for DataFrames
    for f in files:
        if 'part' in f:  # Check for 'part' in filename to avoid temporary files
            df = pd.read_csv(f, header=0)
            if not df.empty:  # Add to the list only if DataFrame is not empty
                dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else None  # Concatenate all non-empty DataFrames

# Read the cluster results and centers, making sure they exist and are not empty
files = glob.glob('new_result/scaled_cluster_results.csv/*.csv')
cluster_results = safe_concat(files)
#print data size
print(cluster_results.shape)

files_centers = glob.glob('new_result/cluster_centers_normalized.csv/*.csv')
cluster_centers = safe_concat(files_centers)

# Check if data is loaded before plotting
if cluster_results is not None and cluster_centers is not None:
    fig = plt.figure(figsize=(12, 8))
    ax = fig.add_subplot(111, projection='3d')  # Create a 3D subplot

    # Plot each cluster with a loop
    for i in range(cluster_centers.shape[0]):
        cluster_data = cluster_results[cluster_results['prediction'] == i]
        ax.scatter(cluster_data['scaled_Centipawns'], cluster_data['scaled_ThinkingTime'], cluster_data['scaled_WhiteElo'], label=f'Cluster {i}')

    # Plot the cluster centers
    ax.scatter(cluster_centers['Centipawns'], cluster_centers['ThinkingTime'], cluster_centers['WhiteElo'], c='red', marker='x', s=100, label='Centers')

    # Set labels and limits according to your dataset dimensions
    ax.set_xlabel('Centipawns')
    ax.set_ylabel('Thinking Time')
    ax.set_zlabel('EloRange')

    # Manually change the range of the 'Thinking Time' axis
    ax.set_ylim(0.45, 0.55)

    ax.set_title('3D K-Means Clustering of Chess Data')
    ax.legend()

    plt.show()
else:
    print("No data available for plotting.")

if cluster_results is not None and cluster_centers is not None:
    # Set up the matplotlib figure
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))  # 1 row, 3 columns

    # Scatter plot for ThinkingTime vs EloRange
    for i in range(cluster_centers.shape[0]):
        cluster_data = cluster_results[cluster_results['prediction'] == i]
        axes[0].scatter(cluster_data['scaled_Centipawns'], cluster_data['scaled_WhiteElo'], label=f'Cluster {i}')
    axes[0].set_xlabel('Scaled Centipawns')
    axes[0].set_ylabel('Scaled WhiteElo')
    axes[0].set_title('Centipawns vs WhiteElo')
    axes[0].legend()

    # Scatter plot for Centipawns vs ThinkingTime
    for i in range(cluster_centers.shape[0]):
        cluster_data = cluster_results[cluster_results['prediction'] == i]
        axes[1].scatter(cluster_data['scaled_Centipawns'], cluster_data['scaled_ThinkingTime'], label=f'Cluster {i}')
    axes[1].set_xlabel('Scaled Centipawns')
    axes[1].set_ylabel('Scaled ThinkingTime')
    axes[1].set_title('Centipawns vs ThinkingTime')
    axes[1].legend()

    # Scatter plot for ThinkingTime vs WhiteElo
    for i in range(cluster_centers.shape[0]):
        cluster_data = cluster_results[cluster_results['prediction'] == i]
        axes[2].scatter(cluster_data['scaled_ThinkingTime'], cluster_data['scaled_WhiteElo'], label=f'Cluster {i}')
    axes[2].set_xlabel('Scaled ThinkingTime')
    axes[2].set_ylabel('Scaled WhiteElo')
    axes[2].set_title('ThinkingTime vs WhiteElo')
    axes[2].legend()

    # Show plot
    plt.tight_layout()  # Adjusts subplot params for better layout
    plt.show()
else:
    print("No data available for plotting.")

if cluster_centers is not None:
    # Set up the matplotlib figure
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))  # 1 row, 3 columns

    # Plot for Centipawns vs WhiteElo
    axes[0].scatter(cluster_centers['Centipawns'], cluster_centers['WhiteElo'], c='red', marker='x', s=100)
    axes[0].set_xlabel('Centipawns (Center)')
    axes[0].set_ylabel('WhiteElo (Center)')
    axes[0].set_title('Cluster Centers: Centipawns vs WhiteElo')

    # Plot for Centipawns vs ThinkingTime
    axes[1].scatter(cluster_centers['Centipawns'], cluster_centers['ThinkingTime'], c='red', marker='x', s=100)
    axes[1].set_xlabel('Centipawns (Center)')
    axes[1].set_ylabel('ThinkingTime (Center)')
    axes[1].set_title('Cluster Centers: Centipawns vs ThinkingTime')

    # Plot for ThinkingTime vs WhiteElo
    axes[2].scatter(cluster_centers['ThinkingTime'], cluster_centers['WhiteElo'], c='red', marker='x', s=100)
    axes[2].set_xlabel('ThinkingTime (Center)')
    axes[2].set_ylabel('WhiteElo (Center)')
    axes[2].set_title('Cluster Centers: ThinkingTime vs WhiteElo')

    # Show plot
    plt.tight_layout()  # Adjusts subplot params for better layout
    plt.show()
else:
    print("No cluster center data available for plotting.")

