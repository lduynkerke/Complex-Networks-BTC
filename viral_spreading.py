import math
from matplotlib import pyplot as plt
from networkx import average_node_connectivity

from network import create_network


def infection_rate(G):
    # contact rate: average number of transactions received by a Bitcoin address per day
    contact_rate = G.number_of_edges() / G.number_of_nodes()

    # probability of transmission: likelihood that a transaction transmitted to a Bitcoin address will be relayed to another address
    probability_of_transmission = len([node for node in G.nodes() if _is_transmitting_node(G, node)]) / G.number_of_nodes()

    # infection rate: average number of Bitcoin addresses that will be infected by a single infected address
    infection_rate = contact_rate * probability_of_transmission

    print(contact_rate, probability_of_transmission, infection_rate)

    return infection_rate


def _is_transmitting_node(G, node):
    if G.in_degree(node) == 0 or G.out_degree(node) == 0:
        return False

    min_in_timestamp = min([timestamp for (_,_,timestamp) in G.in_edges(node, data="timestamp")])

    return any(out_timestamp > min_in_timestamp for (_,_,out_timestamp) in G.out_edges(node, data='timestamp'))

def confirmation_time(all_block_dfs, date):
    df = all_block_dfs[date]
    
    # compute confirmation time: timestamp - timestamp of previous block
    confirmation_times = []
    block_sizes = []
    transaction_counts = []

    for i in range(1, df.shape[0]):
        last_block = df.loc[df['hash'] == df['previousblockhash'].iloc[i]]

        if last_block.shape[0] > 0:
            last_timestamp = last_block['timestamp'].iloc[0]
            current_timestamp = df['timestamp'].iloc[i]

            timedelta = (current_timestamp - last_timestamp).seconds // 60

            confirmation_times.append(timedelta)
            block_sizes.append(df['size'].iloc[i])
            transaction_counts.append(df['transaction_count'].iloc[i])

    # plot histogram of confirmation times
    plt.hist(confirmation_times, bins=100)
    plt.xlabel('Confirmation Time (minutes)')
    plt.ylabel('Frequency')
    plt.show()

    # plot confirmation time vs block size
    plt.scatter(block_sizes, confirmation_times)
    plt.xlabel('Block Size')
    plt.ylabel('Confirmation Time (minutes)')
    plt.show()

    # plot confirmation time vs transaction count
    plt.scatter(transaction_counts, confirmation_times)
    plt.xlabel('Transaction Count')
    plt.ylabel('Confirmation Time (minutes)')
    plt.show()

    confirmation_time = sum(confirmation_times) / df.shape[0]

    print(confirmation_time)

    return confirmation_time

def analyse_viral_spreading(all_block_dfs, all_transaction_dfs):
    btc = create_network(all_transaction_dfs, '2024-04-01')

    beta = infection_rate(btc) #infection rate

    delta = 1 / confirmation_time(all_block_dfs, '2024-04-01') # curing rate

    print("Beta:", beta)
    print("Delta:", delta)

    effective_spreading_rate = beta / delta

    print(effective_spreading_rate)

