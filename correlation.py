import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


def format_data(block_dict, df_price):
    df_price['date'] = pd.to_datetime(df_price['date'].dt.tz_localize(None))
    df_price['volatility'] = np.log(df_price['close']).diff().rolling(window=30).std()
    df_price['price'] = df_price['close']

    df_network = pd.concat(
        [df[['timestamp', 'size', 'stripped_size', 'transaction_count',
             'difficulty', 'chainwork']] for df in block_dict.values()],
        ignore_index=True
    )
    df_network['timestamp'] = pd.to_datetime(df_network['timestamp'].dt.floor('T'))
    df_network['chainwork'] = df_network['chainwork'].apply(lambda x: int(x, 16))
    df_network['witness'] = df_network['size'] - df_network['stripped_size']
    del df_network['stripped_size']

    df = pd.merge(df_network, df_price[['price', 'volume', 'date', 'volatility']],
                  left_on='timestamp', right_on='date', how='inner')
    df.drop('date', axis=1, inplace=True)

    df = df.sort_values(by='timestamp', ascending=True)

    return df


def analyse_correlation(df, plot_df=False):
    if plot_df:
        plt.figure()
        plt.plot(df['timestamp'], df['close']/df['close'].max(), label='close')
        plt.plot(df['timestamp'], df['volume']/df['volume'].max(), label='volume')
        plt.plot(df['timestamp'], df['volatility']/df['volatility'].max(), label='volatility')
        plt.plot(df['timestamp'], df['transaction_count']/df['transaction_count'].max(), label='transaction_count')
        plt.plot(df['timestamp'], df['size']/df['size'].max(), label='size')
        plt.plot(df['timestamp'], df['witness']/df['witness'].max(), label='witness')
        plt.plot(df['timestamp'], df['chainwork']/df['chainwork'].max(), label='chainwork')
        plt.legend()
        plt.show()

    correlation_matrix = df.drop('timestamp', axis=1).corr()

    # Plot heatmap
    plt.figure(figsize=(16, 12))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=.5)
    plt.title('Correlation Matrix')
    # plt.savefig('correlation_matrix.png')
    plt.show()
