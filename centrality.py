import networkx as nx
import pandas as pd

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

def compute_centrality(G, num_nodes=10000):


    if G.number_of_nodes() > num_nodes:
        all_nodes = list(G.nodes())
        sampled_nodes = set(pd.Series(all_nodes).sample(n=num_nodes, random_state=42))
        H = G.subgraph(sampled_nodes).copy()
    else:
        H = G

    print(f"Computing centrality measures on a graph with {len(H)} nodes and {H.size()} edges...")

    degree_centrality = nx.degree_centrality(H)
    closeness_centrality = nx.closeness_centrality(H)
    betweenness_centrality = nx.betweenness_centrality(H, normalized=True, endpoints=False)

    return degree_centrality, closeness_centrality, betweenness_centrality



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
    degree_centrality, closeness_centrality, betweenness_centrality = compute_centrality(G)

    print("Top nodes by degree centrality:", sorted(degree_centrality.items(), key=lambda item: item[1], reverse=True)[:5])
    print("Top nodes by closeness centrality:", sorted(closeness_centrality.items(), key=lambda item: item[1], reverse=True)[:5])
    print("Top nodes by betweenness centrality:", sorted(betweenness_centrality.items(), key=lambda item: item[1], reverse=True)[:5])

if __name__ == '__main__':
    filepath = 'tranaction/part-00000-357d1c05-5aae-461a-b4b1-27a9376291bb-c000.snappy.parquet'
    main(filepath)