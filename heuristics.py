import networkx as nx
import random
from problem import *
from utils import cost

#Possible heuristics: independent rounding, dependent rounding, dispersion (Madhav's version), k-median (w/ houses), high traffic areas

#TODO: test more distances and costs
def independent_LP():
    #G = nx.complete_graph(10)
    #nx.set_edge_attributes(G, 1, 'distance')
    poi_count = 10
    G = [[10*random.random() for i in range(poi_count-k)] for k in range(poi_count-1, -1, -1)]
    for l in G:
        print(['{:.2f}'.format(i) for i in l])
    client_locations = [[0, 1, 2], [3, 4]]
    my_lp = LP(G, client_locations, 1)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    def independent_round(X, Y):
        #round the opening of facilities w/ probabilities returned by LP
        X_rounded = [1 if random.random()<= x else 0 for x in X]

        #reassign y based on the open facilities
        Y_reassigned = {}
        for i in range(len(X_rounded)):
            if X_rounded[i] == 1:
                for index in range(len(client_locations)):
                    min_loc = min([(cost(G, loc, i), loc) for loc in client_locations[index]])[1]
                    for loc in client_locations[index]:
                        if loc == min_loc:
                            Y_reassigned[address(index, loc, i)] = 1
                        else:
                            Y_reassigned[address(index, loc, i)] = 0
            else:
                for index in range(len(client_locations)):
                    for loc in client_locations[index]:
                        Y_reassigned[address(index, loc, i)] = 0

        return X_rounded, Y_reassigned

    X_rounded, Y_reassigned = independent_round(X, Y)
    opened_facilities = []
    for i in range(len(X_rounded)):
        if X_rounded[i] == 1:
            opened_facilities.append(i)
    assigned_locations = []
    for index in range(len(client_locations)):
        for loc in client_locations[index]:
            for facility in opened_facilities:
                if Y_reassigned[address(index, loc, facility)] == 1:
                    assigned_locations.append((loc, facility))
    print(opened_facilities)
    print(assigned_locations)

#TODO: test more distances and costs
'''G = nx.complete_graph(10)
nx.set_edge_attributes(G, 1, 'distance')
my_lp = MILP(G, [{0, 1, 2}, {0, 5}, {4, 8}], 1)
my_lp.solve_lp()
X, Y = my_lp.get_variable_solution()'''