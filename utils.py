from typing import Dict, List, Tuple, Set
import random
from collections import namedtuple

address = namedtuple('address', ['index', 'location', 'facility'])

def cost(G: List[List[float]], loc1, loc2):
    if loc1==loc2: return 0
    elif loc1 < loc2:
        return G[loc2][loc1]
    else:
        return G[loc1][loc2]

def generate_input():
    poi_count = 10
    G = [[10*random.random() for i in range(poi_count-k)] for k in range(poi_count-1, -1, -1)]
    for l in G:
        print(['{:.2f}'.format(i) for i in l])
    client_locations = [[0, 1, 2], [3, 4]]
    return G, client_locations

def assign_facilities(G, client_locations, X_rounded):
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
    return Y_reassigned

def format_location_output(X_rounded, Y_reassigned):
    print([i for i in range(len(X_rounded)) if X_rounded[i] == 1])
    print([address for address in Y_reassigned.keys() if Y_reassigned[address] == 1])