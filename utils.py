from typing import Dict, List, Tuple, Set
import random
from collections import namedtuple

address = namedtuple('address', ['index', 'location', 'facility'])
assignment = namedtuple('assignment', ['location', 'facility'])

def cost(G: List[List[float]], loc1, loc2):
    if loc1==loc2: return 0
    elif loc1 < loc2:
        return G[loc2][loc1]
    else:
        return G[loc1][loc2]

#Fills in the adjacency matrix with random float distances in (0, 10)
#If constant=True, defaults to adjacency matrix of 1
def generate_input(constant = False):
    poi_count = 10
    if constant:
        G = [[1 for i in range(poi_count-k)] for k in range(poi_count-1, -1, -1)]
    else:
        G = [[10*random.random() for i in range(poi_count-k)] for k in range(poi_count-1, -1, -1)]
    for row in G:
        row[-1] = 0
    return G

def assign_facilities(G: List[List[float]], client_locations, X_rounded):
    Y_reassigned = {}
    
    open_facilities = [i for i in range(len(X_rounded)) if X_rounded[i] == 1]
    for index in range(len(client_locations)):
        possible_assignments = []
        for loc in client_locations[index]:
            for fac in open_facilities:
                Y_reassigned[address(index, loc, fac)] = 0
                possible_assignments.append((cost(G, loc, fac), loc, fac))
        
        min_loc = min(possible_assignments)
        Y_reassigned[address(index, min_loc[1], min_loc[2])] = 1
    return Y_reassigned

def calculate_objective(G: List[List[float]], X, Y):
    max_obj_value = 0
    for key in Y.keys():
        obj_val = cost(G, key.location, key.facility)*Y[key]
        if obj_val > max_obj_value:
            max_obj_value = obj_val
    return max_obj_value


def format_location_output(X_rounded, Y_reassigned):
    print("Facilities Opened: \t" + str([i for i in range(len(X_rounded)) if X_rounded[i] == 1]))
    print("Client Assignment: \t" + str([address for address in Y_reassigned.keys() if Y_reassigned[address] == 1]))