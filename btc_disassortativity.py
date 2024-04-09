import numpy as np
import networkx as nx
import matplotlib.pyplot as plt

def calculate_kcn_in_kout(graph):
    kcn_in = {}
    for node in graph.nodes():
        out_degree = graph.out_degree(node)
        neighbors = list(graph.neighbors(node))
        avg_in_degree = np.mean([graph.in_degree(neighbor) for neighbor in neighbors])
        kcn_in[out_degree] = avg_in_degree / out_degree if out_degree != 0 else 0
    return kcn_in


def get_disassortativity(G):
    pearson_coefficient = nx.degree_pearson_correlation_coefficient(G)
    print("Pearson correlation coefficient:", pearson_coefficient)

    kcn_in_values = calculate_kcn_in_kout(G)

    # Sort the values based on kout
    sorted_degrees = sorted(kcn_in_values.keys())
    sorted_kcn_in = [kcn_in_values[degree] for degree in sorted_degrees]

    # Plot the graph
    plt.figure(figsize=(10, 6))
    plt.plot(sorted_degrees, sorted_kcn_in, marker='o', linestyle='-')
    plt.xscale('log')
    plt.xlabel('Out-degree (kout)')
    plt.ylabel('Average in-degree of closest neighbors (kcn-in)')
    plt.title('Property of kcn-in vs kout')
    plt.grid(True)
    plt.show()
    
    # # preferential_attachment
    # preferential_attachment = nx.preferential_attachment(G)
    
    # # Determine if the graph is disassortative based on kcn-in (kout) measure
    # kcn_in_values = kcn_in_kout(G)
    # sorted_degrees = sorted(kcn_in_values.keys())
    # sorted_kcn_in = [kcn_in_values[degree] for degree in sorted_degrees]

    # # Check for a downward trend in kcn-in (kout)
    # disassortative = all(sorted_kcn_in[i] >= sorted_kcn_in[i+1] for i in range(len(sorted_kcn_in)-1))

    # if disassortative:
    #     print("The graph is disassortative based on kcn-in (kout) measure.")
    # else:
    #     print("The graph is not disassortative based on kcn-in (kout) measure.")
