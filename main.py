import os
import pandas as pd
from pprint import pprint

def main():
    file_dir = "data/"
    files = sorted(get_files_recursively(f"{file_dir}"))
    
    all_block_dfs = {}
    all_transaction_dfs = {}
    for file in files:
        print(file)
        date = file.split("/")[2][5:]
        print(date)
        df = read_snappy_parquet(file)
        if "blocks" in file:
            all_block_dfs[date] = df
        if "transactions" in file:
            all_transaction_dfs[date] = df
            
    pprint(all_block_dfs)
    
        


def read_snappy_parquet(file_path):
    try:
        df = pd.read_parquet(file_path, engine='pyarrow')  # Read the .snappy.parquet file into DataFrame
        return df
    except Exception as e:
        print("Error reading the file:", e)
        return None

def get_files_recursively(directory):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_list.append(os.path.join(root, file))
    return file_list

if __name__ == "__main__":
    main()