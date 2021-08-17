from typing import Dict, List, Tuple, Set
import random
import math
from config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT, address
import geopy.distance
from itertools import chain, combinations

def powerset(iterable):
    """
    Generates the powerset in reverse size order, excluding combinations of size 0
    """
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def cover_most(s: int):
    """
    Helper method for FPT: returns the set of activity locations of size s that cover the most clients
    """
    covered = set()
    selected = []
    for i in range(s):
        most_coverage = max([(len(set(LOCATIONS[l]['pid']) - covered), l) for l in LOCATIONS.keys()])
        selected.append(most_coverage[1])
        covered = covered.union(LOCATIONS[most_coverage[1]]['pid'])
    print(len(covered)/len(CLIENT_LOCATIONS))
    return selected

def calculate_distance(loc1: int, loc2: int):
    """
    Calculates the haversine distance between two location indices
    """
    if loc1 == loc2:
        return 0
    
    coord1_row = LOCATIONS[loc1]
    coord2_row = LOCATIONS[loc2]
    coord1 = (coord1_row['latitude'], coord1_row['longitude'])
    coord2 = (coord2_row['latitude'], coord2_row['longitude'])
    return geopy.distance.great_circle(coord1, coord2).km
    
# TODO: Do we still pass in visited locations
def assign_facilities(facilities: List[int]):
    """
    Assigns clients to their nearest facility from one of their visited locations.
    
    PARAMETERS
    ----------
        facilities
            list of facilities that are open
    RETURNS
    ----------
        assignments: List[Tuple[int, int]]
            lists (visited location, facility) assignment for each client
    """
    
    # TODO: assign top 500 most visited locations
    if len(facilities) == 0: return []
    
    assignments: List[Tuple[int, int]] = []
    
    for key in CLIENT_LOCATIONS.keys():
        possible_assignments = [(calculate_distance(loc, fac), loc, fac) for loc in CLIENT_LOCATIONS[key]['lid'] for fac in facilities]
        
        min_loc = min(possible_assignments)
        assignments.append((min_loc[1], min_loc[2]))
   
    return assignments

#TODO: What happens if homes are excluded are excluded from the problem entirely
def calculate_objective(assignments: List[Tuple[int, int]]) -> float:
    """
    Calculates the maximum distance any client must travel to reach their facility based on the assignments
    """
    if len(assignments) == 0: return 0
    
    obj_val = [calculate_distance(loc, fac) for loc, fac in assignments]
    return max(obj_val)

def precompute_distances(client_locations: List[List[int]], locations: List[int]):
    """
    Computes the distances between client locations (indexed by column) and facility locations (indexed by row)
    """
    G = []
    loc_map = {}
    c_loc_map = {}
    
    clients = set(l for loc in client_locations for l in loc)
    for l_ind, l in enumerate(locations):
        loc_map[l] = l_ind
        G.append([0 for i in range(len(clients))])
        
        for c_ind, c in enumerate(clients):
            c_loc_map[c] = c_ind
            G[-1][c_ind] = calculate_distance(c, l)
    
    '''for row in G:
        print(["{:.2f}".format(i) for i in row])'''
    return G, loc_map, c_loc_map

def assign_client_facilities(client_locations: List[List[int]], facilities: List[int]):
    """
    Assigns clients to their nearest facility from one of their visited locations.
    Currently a helper function for fpt
    PARAMETERS
    ----------
        client_locations
            clients represented by index, contains a list of locations visited by each indexed client
        open_facilities
            list of facilities that will be opened
    RETURNS
    ----------
        assignments
            lists (visited location, facility) assignment for each client
        obj_value
            the maximum distance that a client must travel to reach its nearest facility, where clients are from client_locations
    """
    if len(facilities) == 0: return []
    obj_val: int = 0
    
    assignments: List[Tuple[int, int]] = []
    
    for ind in range(len(client_locations)):
        possible_assignments = [(calculate_distance(loc, fac), loc, fac) for loc in client_locations[ind] for fac in facilities]
        
        min_loc = min(possible_assignments)
        if min_loc[0] > obj_val:
            obj_val = min_loc[0]
        assignments.append((min_loc[1], min_loc[2]))
   
    return assignments, obj_val

def assign_client_facilities2(G: List[List[int]], loc_map: Dict[int, int], c_loc_map: Dict[int, int], client_locations: List[List[int]], facilities: List[int]):
    """
    Assigns clients to their nearest facility from one of their visited locations.
    Currently a helper function for fpt
    PARAMETERS
    ----------
        G
            distance matrix returned from precompute_distances: rows are potential facility locations and columns are client locations
        loc_map
            mapping the potential facility locations to the index of the row in G
        c_loc_map
            mapping the client locations to the index of the column in G
        client_locations
            clients represented by index, contains a list of locations visited by each indexed client
        open_facilities
            list of facilities that are open
    RETURNS
    ----------
        obj_value: float
            the maximum distance that a client must travel to reach its nearest facility, where clients are from client_locations
    """
    if len(facilities) == 0: return []
    obj_val: int = 0
    
    for ind in range(len(client_locations)):
        possible_assignments = [G[loc_map[fac]][c_loc_map[loc]] for loc in client_locations[ind] for fac in facilities]
        
        min_loc = min(possible_assignments)
        if min_loc > obj_val:
            obj_val = min_loc
   
    return obj_val

def calculate_percentile_objective(assignments: List[Tuple[int, int]], percentile: float) -> float:
    """
    Given that we only need to cover a certain percentils of clients,
    calculates the minimum objective value (maximum distance for any individual based on the assignments)
    """
    if len(assignments) == 0: return 0
    
    obj_val = sorted([calculate_distance(loc, fac) for loc, fac in assignments])
    ind = math.floor(len(obj_val)*percentile) -1
    
    #If no clients are selected to be covered, then the objective is 0
    if ind < 0: return 0
    
    return obj_val[ind]