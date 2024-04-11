import os
import pandas as pd
from pprint import pprint
import numpy as np
import matplotlib.pyplot as plt


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
        if "transactions" in file:
            all_transaction_dfs[date] = df
        if "price" in file:
            all_price_dfs[date] = df

    pprint(all_block_dfs['2024-04-01'].columns)
    pprint(all_price_dfs['2024-03-01'].columns)

    # Analyse correlation
    correlation_df = format_data(all_block_dfs, all_price_dfs)
    analyse_correlation(correlation_df)


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


def format_data(block_dict, price_dict):
    df_price = price_dict['2024-03-01']
    df_price['date'] = pd.to_datetime(df_price['date'].dt.tz_localize(None))
    df_price['volatility'] = np.log(df_price['close']).diff().rolling(window=30).std()

    df_network = pd.concat(
        [df[['timestamp', 'size', 'transaction_count']] for df in block_dict.values()],
        ignore_index=True
    )
    df_network['timestamp'] = pd.to_datetime(df_network['timestamp'].dt.floor('T'))

    df = pd.merge(df_network, df_price[['close', 'volume', 'date', 'volatility']],
                  left_on='timestamp', right_on='date', how='inner')
    df.drop('date', axis=1, inplace=True)

    df = df.sort_values(by='timestamp', ascending=True)

    return df


def analyse_correlation(df):
    correlation_matrix = df.corr()

    print("Correlation Matrix:", correlation_matrix)

    # Plot price, volume
    plt.figure()
    plt.plot(df['timestamp'], df['close'], label='close')
    plt.plot(df['timestamp'], df['volume'], label='volume')
    plt.plot(df['timestamp'], df['transaction_count'], label='transaction_count')
    plt.plot(df['timestamp'], df['size'], label='size')
    plt.legend()
    plt.show()


if __name__ == "__main__":
    main()
