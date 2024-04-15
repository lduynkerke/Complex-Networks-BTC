import networkx as nx
import pandas as pd


def load_data(filepath):

    df = pd.read_parquet(filepath)
    return df


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


def compute_connected_components(G):

    sccs = list(nx.strongly_connected_components(G))
    largest_scc = max(sccs, key=len)

    wccs = list(nx.weakly_connected_components(G))
    largest_wcc = max(wccs, key=len)

    return {
        'number_of_sccs': len(sccs),
        'largest_scc_size': len(largest_scc),
        'number_of_wccs': len(wccs),
        'largest_wcc_size': len(largest_wcc)
    }


def main(filepath):
    transactions_df = load_data(filepath)
    G = build_graph(transactions_df)
    components_metrics = compute_connected_components(G)

    print(f"Number of SCCs: {components_metrics['number_of_sccs']}")
    print(f"Size of the largest SCC: {components_metrics['largest_scc_size']}")
    print(f"Number of WCCs: {components_metrics['number_of_wccs']}")
    print(f"Size of the largest WCC: {components_metrics['largest_wcc_size']}")


if __name__ == '__main__':
    filepath = 'tranaction/part-00000-357d1c05-5aae-461a-b4b1-27a9376291bb-c000.snappy.parquet'
    main(filepath)

