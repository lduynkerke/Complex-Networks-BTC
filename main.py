import os
import pandas as pd
from pprint import pprint
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

from correlation import format_data, analyse_correlation



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

    # Create network and print properties
    btc = create_network(all_transaction_dfs)
    print_network_data(btc)

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


def create_network(dict_df):
    G = nx.DiGraph()  # Directed graph since transactions are one way only
    df = dict_df['2024-03-01']

    for i in range(df.shape[0]):
        cell1 = df['inputs'].iloc[i]
        cell2 = df['outputs'].iloc[i]

        if cell1 is not None and cell2 is not None:
            address1 = cell1[0]['address']
            address2 = cell2[0]['address']

            # Add nodes (BTC addresses) and edges (BTC transactions)
            if address1 is not None and address2 is not None:
                G.add_node(address1)
                G.add_node(address2)
                G.add_edge(address1, address2, weight=0.5)

    return G


def print_network_data(G):
    # Calculate and print network properties
    num_nodes = G.number_of_nodes()
    num_edges = G.number_of_edges()
    avg_degree = np.mean([deg for node, deg in G.degree()])
    std_dev_degree = np.std([deg for node, deg in G.degree()])

    print(num_nodes, num_edges, avg_degree, std_dev_degree)

    # Select top nodes based on centrality scores
    centrality = nx.degree_centrality(G)
    top_nodes = sorted(centrality, key=centrality.get, reverse=True)[:5000]  # Adjust the number of nodes as needed
    subgraph = G.subgraph(top_nodes)

    # Draw the subgraph
    pos = nx.spring_layout(subgraph)  # Or any other layout algorithm
    nx.draw_networkx_nodes(subgraph, pos, node_size=10)
    nx.draw_networkx_edges(subgraph, pos, alpha=0.5)

    plt.axis('off')
    plt.show()


if __name__ == "__main__":
    main()
