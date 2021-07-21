import networkx as nx

#TODO: test more distances and costs
G = nx.complete_graph(5)
nx.set_edge_attributes(G, 1, 'distance')
my_lp = LP(G, [{0, 1, 2}], 1)
my_lp.solve_lp()