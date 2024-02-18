import re

# Sample text with moves, eval scores, and clock times
text = """
1. e4 { [%eval 0.17] [%clk 0:05:00] } 1... c5 { [%eval -0.19] [%clk 0:05:00] }
2. Nf3 { [%eval 0.25] [%clk 0:04:59] } 2... Nc6 { [%eval 0.33] [%clk 0:04:59] }
"""

# Regex pattern to match move number, eval score, and clock time
pattern = r"(\d+)(\.+)\s(\S+)\s{\s\[%eval (-?\d+(\.\d+)?)\]\s\[%clk (\d+):(\d+):(\d+)\]\s}"

# Find all matches
matches = re.findall(pattern, text)

print(type(matches))

# Process matches
for match in matches:
    move_number, dots, move, eval_score, _, hours, minutes, seconds = match
    is_white_move = dots == "."  # True for white moves, False for black moves
    move_color = "White" if is_white_move else "Black"
    total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)  # Convert clock time to seconds

    # Print extracted information
    print(f"Move {move_number} ({move_color}): Eval Score = {eval_score}, Clock Time = {total_seconds} seconds, Move = {move}")

