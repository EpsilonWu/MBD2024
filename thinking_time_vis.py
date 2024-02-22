import matplotlib.pyplot as plt
import pandas as pd

# Load data
stats_data = pd.read_csv("result/thinking_time_stats.csv")

# Separate data for White and Black based on MoveColor and StartingTime
white_data = stats_data[(stats_data['MoveColor'] == 'White') & (stats_data['StartingTime'] == 900) & (stats_data['MoveNumber'] <= 80)]
black_data = stats_data[(stats_data['MoveColor'] == 'Black') & (stats_data['StartingTime'] == 900) & (stats_data['MoveNumber'] <= 80)]

# Prepare data for plotting
move_numbers_white = white_data['MoveNumber'].tolist()
average_times_white = white_data['AverageThinkingTime'].tolist()
std_white = white_data['StdDevThinkingTime'].tolist()
max_times_white = white_data['90thPercentileThinkingTime'].tolist()
min_times_white = white_data['10thPercentileThinkingTime'].tolist()

move_numbers_black = black_data['MoveNumber'].tolist()
average_times_black = black_data['AverageThinkingTime'].tolist()
std_black = black_data['StdDevThinkingTime'].tolist()
max_times_black = black_data['90thPercentileThinkingTime'].tolist()
min_times_black = black_data['10thPercentileThinkingTime'].tolist()

# Plotting
plt.figure(figsize=(15, 8))

# Plot for White
plt.plot(move_numbers_white, average_times_white, label='Average White', marker='o', color='blue')

plt.fill_between(move_numbers_white, min_times_white, max_times_white, color='blue', alpha=0.1, label='Percentage Range White')

# Plot for Black
plt.plot(move_numbers_black, average_times_black, label='Average Black', marker='o', color='black')
plt.fill_between(move_numbers_black, min_times_black, max_times_black, color='grey', alpha=0.1, label='Percentage Range Black')

plt.title('Thinking Time Variation per Move by Color')
plt.xlabel('Move Number')
plt.ylabel('Thinking Time (seconds)')
plt.legend()
plt.grid(True)
plt.show()

# Plotting
plt.figure(figsize=(15, 8))
plt.plot(move_numbers_white, std_white, label='Std Dev White', marker='o', color='green')
plt.plot(move_numbers_black, std_black, label='Std Dev Black', marker='o', color='red')

plt.title('Standard Deviation of Thinking Time per Move by Color')
plt.xlabel('Move Number')
plt.ylabel('Standard Deviation (seconds)')
plt.legend()
plt.grid(True)
plt.show()
