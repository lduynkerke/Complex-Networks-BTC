import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def format_data(block_dict, df_price):
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


def analyse_correlation(df, plot_df=False):
    correlation_matrix = df.corr()

    print("Correlation Matrix:", correlation_matrix)

    # Plot price, volume
    if plot_df:
        plt.figure()
        plt.plot(df['timestamp'], df['close'], label='close')
        plt.plot(df['timestamp'], df['volume'], label='volume')
        plt.plot(df['timestamp'], df['transaction_count'], label='transaction_count')
        plt.plot(df['timestamp'], df['size'], label='size')
        plt.legend()
        plt.show()
