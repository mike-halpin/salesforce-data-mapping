import os
import pandas as pd
from datetime import datetime

def save_test_fixture(df, base_filename):
    # Ensure the directory exists
    directory = "tests/fixtures"
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Construct the filenames with the timestamp
    json_filename = f"{base_filename}_{timestamp}.json"
    pickle_filename = f"{base_filename}_{timestamp}.pkl"

    json_path = os.path.join(directory, json_filename)
    pickle_path = os.path.join(directory, pickle_filename)

    # Save to JSON (append if file exists)
    if os.path.exists(json_path):
        # Load existing data
        existing_data = pd.read_json(json_path, lines=True)
        df = existing_data.append(df, ignore_index=True)

    df.to_json(json_path, orient="records", lines=True)

    # Save to pickle
    df.to_pickle(pickle_path)

# Example usage
# df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
# save_dataframe(df, "sample")
