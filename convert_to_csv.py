import sys
import pandas as pd

def main():
    if len(sys.argv) < 2:
        print("Please provide a file name as an argument.")
        return

    parquet_file = sys.argv[1]
    print(f"Reading the file '{parquet_file}' as a parquet file...")
    df = pd.read_parquet(parquet_file)
    csv_file = parquet_file.replace(".parquet", ".csv")
    df.to_csv(csv_file, index=False)
    print("Done!")

if __name__ == '__main__':
    main()