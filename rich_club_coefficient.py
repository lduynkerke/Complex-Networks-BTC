import networkx as nx
import matplotlib.pyplot as plt_module

def rich_club_coefficient_directed(graph):
    degrees = dict(graph.out_degree())
    rich_club_coeffs = {}

    # Sort nodes by out-degree in descending order
    sorted_nodes = sorted(degrees, key=lambda x: degrees[x], reverse=True)

    # Initialize rich-club coefficients dictionary
    for node in sorted_nodes:
        rich_club_coeffs[node] = 0

    # DFS traversal to count edges among nodes with out-degree at least k
    def dfs(node, k, visited):
        visited.add(node)
        for neighbor in graph.successors(node):
            if degrees[neighbor] >= k:
                rich_club_coeffs[node] += 1
            if neighbor not in visited:
                dfs(neighbor, k, visited)

    # Compute rich-club coefficients for each out-degree k
    for node in sorted_nodes:
        k = degrees[node]
        visited = set()
        dfs(node, k, visited)
        # Calculate the rich-club coefficient
        max_edges = len(visited) * (len(visited) - 1)
        if max_edges > 0:
            rich_club_coeffs[node] /= max_edges

    return rich_club_coeffs


def plot_rich_club_coefficients(rich_club_coeffs):
    plt_module.figure(figsize=(10, 6))
    
    k_values = list(rich_club_coeffs.keys())
    rich_club_values = list(rich_club_coeffs.values())

    # Sample data points evenly spaced across the range of out-degrees
    sampled_k_values = k_values
    sampled_rich_club_values = rich_club_values

    plt_module.plot(sampled_k_values, sampled_rich_club_values, marker='o', linestyle='')
    plt_module.title('Rich-club Coefficients for Directed Graph')
    plt_module.xlabel('Out-degree (k)')
    plt_module.ylabel('Rich-club Coefficient')
    plt_module.xscale('log')  # Set y-axis scale to log
    plt_module.yscale('log')  # Set y-axis scale to log
    plt_module.grid(True)
    plt_module.show()

def get_rich_club_coefficient(G):
    print("Getting rich club coefficient")
    rich_club_coeffs = rich_club_coefficient_directed(G)
    plot_rich_club_coefficients(rich_club_coeffs)

    # Plot the graph
    # plt.figure(figsize=(10, 6))

    # plt.plot(degrees, rich_club_coeffs, marker='o', linestyle='-', label='Rich-club coefficient')
    # plt.plot(degrees, norm_rich_club_coeffs, marker='o', linestyle='-', label='Normalized rich-club coefficient')

    # plt.xlabel('Degree (k)')
    # plt.ylabel('Rich-club coefficient')
    # plt.title('Rich-club property')
    # plt.legend()
    # plt.grid(True)
    # plt.show()
