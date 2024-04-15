import networkx as nx
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import random
import numpy as np
# import random

def load_data(filepath):
    return pd.read_parquet(filepath)


def build_graph(transactions_df):
    G = nx.DiGraph()
    for _, transaction in transactions_df.iterrows():
        inputs = transaction['inputs'] if transaction['inputs'] is not None else []
        outputs = transaction['outputs'] if transaction['outputs'] is not None else []
        for input_item in inputs:
            input_address = input_item.get('address')

            for output_item in outputs:
                output_address = output_item.get('address')

                if input_address and output_address and output_item.get('value', 0) > 0:
                    G.add_edge(input_address, output_address, weight=output_item['value'])
    return G


def compute_centrality(G, num_nodes=50000):

    if len(G) > num_nodes:
        all_nodes = list(G.nodes())
        sampled_nodes = random.sample(all_nodes, num_nodes)
        H = G.subgraph(sampled_nodes).copy()
        print(f"Computing centrality measures on a graph with {len(H)} nodes and {H.size()} edges...")

        degree_centrality = nx.degree_centrality(H)
        closeness_centrality = nx.closeness_centrality(H)
        betweenness_centrality = nx.betweenness_centrality(H, normalized=True, endpoints=False)

        return H, degree_centrality, closeness_centrality, betweenness_centrality
    else:
        print(
            f"Graph has fewer than or equal to the specified number of nodes to sample ({num_nodes}). Computing centrality on the full graph.")
        return G, nx.degree_centrality(G), nx.closeness_centrality(G), nx.betweenness_centrality(G, normalized=True,
                                                                                                 endpoints=False)


def visualize_centrality(H, degree_centrality, closeness_centrality, betweenness_centrality):
    nodes = list(H.nodes())
    degree_values = np.array([H.degree(n) for n in nodes])
    closeness_values = np.array([closeness_centrality[n] for n in nodes])
    betweenness_values = np.array([betweenness_centrality[n] for n in nodes])


    fig = plt.figure(figsize=(10, 7))
    ax = fig.add_subplot(111, projection='3d')


    scatter = ax.scatter(degree_values, closeness_values, betweenness_values, c=degree_values, cmap='viridis')

    cbar = plt.colorbar(scatter, ax=ax, orientation='vertical')
    cbar.set_label('Node Degree')

    ax.set_xlabel('Degree')
    ax.set_ylabel('Closeness')
    ax.set_zlabel('Betweenness')

    plt.title('Closeness and Betweenness vs Node Degree')
    plt.show()


def main(filepath):
    print("Loading data...")
    transactions_df = load_data(filepath)

    print("Building graph...")
    G = build_graph(transactions_df)
    print(f"Built a graph with {G.number_of_nodes()} nodes and {G.number_of_edges()} edges.")


    if G.number_of_edges() == 0:
        print("No edges in the graph. Exiting...")
        return

    print("Computing centrality measures...")
    H, degree_centrality, closeness_centrality, betweenness_centrality = compute_centrality(G)

    visualize_centrality(H, degree_centrality, closeness_centrality, betweenness_centrality)



if __name__ == '__main__':
    filepath = 'tranaction/part-00000-357d1c05-5aae-461a-b4b1-27a9376291bb-c000.snappy.parquet'
    main(filepath)