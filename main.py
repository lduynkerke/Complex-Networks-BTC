import os
import pandas as pd
from pprint import pprint

from correlation import format_data, analyse_correlation
from regression import prepare_data, fit_regression
from network import create_network, print_network_data

from connected_components import analyze_and_visualize_connected_components, read_parquet_file


def main():
    file_dir = "data/"
    files = sorted(get_files_recursively(f"{file_dir}"))

    all_block_dfs = {}
    all_transaction_dfs = {}
    all_price_dfs = {}
    for file in files:
        print(file)
        # date = file.split('/')[2][5:] # Linux
        date = file.split('\\')[1][5:]  # Windows
        print(date)
        df = read_snappy_parquet(file)
        if "blocks" in file:
            all_block_dfs[date] = df
        if "price" in file:
            all_price_dfs[date] = df
        # Uncomment if you want to create network
        if "transactions" in file:
            all_transaction_dfs[date] = df

    pprint(all_block_dfs['2024-04-01'].columns)
    pprint(all_transaction_dfs['2024-04-01'].columns)
    pprint(all_price_dfs['2024-04-01'].columns)

    # Create network and print properties
    # btc = create_network(all_transaction_dfs)
    # print_network_data(btc)

    # Analyse correlation
    correlation_df = format_data(all_block_dfs, all_price_dfs['2024-04-01'])
    analyse_correlation(correlation_df, False)

    # Fit regression models
    x0, x1, y0, y1, cols = prepare_data(correlation_df)
    fit_regression(x0, x1, y0, y1, cols)


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

# if __name__ == "__main__":
#     # Define the file path for the transaction data
#     filepath = 'data/transactions/date=2024-04-01/part-00000-6ef63408-eab9-44d3-9497-cf8652666c86-c000.snappy.parquet'
#
#     # Read the transaction data using the function imported from connected_components
#     transaction_data = read_parquet_file(filepath)
#
#     # Assuming the Parquet file has columns 'source' and 'target' representing edges
#     # Update the column names as per the actual names in your Parquet file
#     analyze_and_visualize_connected_components(transaction_data)



