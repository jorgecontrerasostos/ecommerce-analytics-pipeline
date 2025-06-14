import pandas as pd
import os


def validate_data_files():
    data_dir = "data"

    # Check if data directory exists
    if not os.path.exists(data_dir):
        print(f"Error: {data_dir} directory does not exist")
        return

    # Check if data directory is empty
    files = os.listdir(data_dir)
    if not files:
        print(f"Error: {data_dir} directory is empty")
        return

    print(f"Found {len(files)} files in {data_dir} directory")

    # Validate each CSV file
    for file in files:
        file_path = os.path.join(data_dir, file)

        # Skip directories
        if os.path.isdir(file_path):
            continue

        # Only process CSV files
        if not file.endswith(".csv"):
            print(f"Skipping {file}: not a CSV file")
            continue

        try:
            df = pd.read_csv(file_path)
            print(f"\nValidating {file}:")
            print(f"- Shape: {df.shape}")
            print(f"- Columns: {', '.join(df.columns)}")
            print(f"- First 2 rows:")
            print(df.head(2))
            print("-" * 50)
        except Exception as e:
            print(f"Error reading {file}: {e}")


if __name__ == "__main__":
    validate_data_files()
