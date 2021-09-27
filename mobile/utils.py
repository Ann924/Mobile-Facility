from typing import Dict, List, Tuple, Set
import random
import math
from mobile.config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT, address
import geopy.distance
from itertools import chain, combinations
import tqdm

def powerset(iterable):
    """
    Generates the powerset in reverse size order, excluding combinations of size 0
    """
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def calculate_objective(assignments: List[Tuple[int, int]], percentile: float = 100) -> float:
    """
    Given that we only need to cover a certain percentils of clients,
    calculates the minimum objective value (maximum distance for any individual based on the assignments)
    """
    if len(assignments) == 0: return 0
    
    obj_val = sorted([calculate_distance(loc, fac) for loc, fac in assignments])
    ind = math.floor(len(obj_val)*percentile/100) -1
    
    #If no clients are selected to be covered, then the objective is 0
    if ind < 0: return 0
    
    return obj_val[ind]

    
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
        possible_assignments = [(calculate_distance(loc, fac), loc, fac) for loc in CLIENT_LOCATIONS[key] for fac in facilities]
        
        min_loc = min(possible_assignments)
        assignments.append((min_loc[1], min_loc[2]))
   
    return assignments

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
    
    return G, loc_map, c_loc_map

#########################################################################################################
#                                 Utility Functions For Heuristics                                      #
#########################################################################################################

def generate_sorted_list():
    
    neighbors = {}
    
    LOCATIONS_act = [l for l in range(len(LOCATIONS)) if not LOCATIONS[l]['home']]
    
    for l in tqdm.tqdm(LOCATIONS_act):
        sorted_distance_neighbors = []
        
        for j in range(len(LOCATIONS)):
            dist = calculate_distance(l, j)
            sorted_distance_neighbors.append((dist, j))
            
        sorted_distance_neighbors = sorted(sorted_distance_neighbors, reverse = False)
        neighbors[l] = sorted_distance_neighbors
    
    return neighbors

def cover_most(s: int):
    """
    Helper method for FPT: returns the set of activity locations of size s that cover the most clients
    Used with aggregate activity locations
    
    aggregation : int
    the version of aggregation selected
    0 --> none
    1 --> set cover: aggregation with repeats in coverage
    """
    
    covered = set()
    selected = []
    for i in range(s):
        most_coverage = max([(len(set(LOCATIONS[l]['pid']) - covered), l, LOCATIONS[l]) for l in range(len(LOCATIONS))])
        selected.append(most_coverage[1])
        covered = covered.union(LOCATIONS[most_coverage[1]]['pid'])
    print(f"COVERAGE OF CLIENTS BY {s} LOCATIONS: ", len(covered)/len(CLIENT_LOCATIONS.keys()))
    return selected

def assign_client_facilities(G: List[List[int]], loc_map: Dict[int, int], c_loc_map: Dict[int, int], client_locations: List[List[int]], facilities: List[int]):
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

#########################################################################################################
#                                        K-Supplier Functions                                           #
#########################################################################################################

def k_supplier(clients: List[int], locations: List[int], k: int):
    """
    Solves k-supplier (where client locations and facility locations may not overlap) with Hochbaum-Shmoys
    3-approximation algorithm
    
    PARAMETERS
    ----------
    distance
        diagonally-filled adjacency matrix for distances between locations
    clients
        each client is associated with a singular location
    locations
        points of interest at which facilities can be opened
    k
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        the facility locations that are open
    """
    l = 0
    #r = 40075
    r=100

    to_ret = -1
    #EPSILON = 10**(-6)
    EPSILON = 10**(-4)
    
    while r-l > EPSILON:
    
        mid = l + (r - l) / 2

        if len(_check_radius(mid, clients)) <= k:
            facilities: List[int] = _locate_facilities(mid,
                                    _check_radius(mid, clients), locations, k)
            if facilities:
                to_ret = mid
                r = mid
            else:
                l = mid
        else:
            l = mid
    
    return _locate_facilities(to_ret,_check_radius(to_ret, clients), locations, k)

def _check_radius(radius: int, clients: List[int]):
    """Determine the maximal independent set of pairiwse independent client balls with given radius
    
    PARAMETERS
    ----------
    radius
        from the binary search
    distances
        diagonally-filled adjacency matrix for distances between locations
    clients
        each client is associated with a singular location
    
    RETURNS
    ----------
    pairwise_disjoint
        maximal independent pairwise disjoint set of clients, where disjoint is defined as greater than a distance
        of 2*radius apart
    """
    
    pairwise_disjoint = set()

    V = set(clients)
    while len(V)!=0:
        v = V.pop()
        pairwise_disjoint.add(v)
        
        remove = set()
        for i in V:
            if calculate_distance(v, i) <= 2*radius:
                remove.add(i)
        V-=remove
    
    return pairwise_disjoint

def _locate_facilities(radius: int, pairwise_disjoint: Set[int], locations: List[int], k: int):
    """Select a facility to open within the given radius for each pairwise_disjoint client
    
    PARAMETERS
    ----------
    radius
        from the binary search
    distances
        diagonally-filled adjacency matrix for distances between locations
    pairwise_disjoint
        clients that are not within a distance of 2*radius from one another
    locations
        points of interest where facilities can be opened
    k
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities: List[int]
        the locations at which facilities are opened
    """
    
    facilities = set()
    for c in pairwise_disjoint:
        for l in locations:
            if calculate_distance(c, l) <= radius:
                facilities.add(l)
                break
    
    if len(facilities) < len(pairwise_disjoint):
        return None
    
    #Check if k larger than the number of possible facility locations
    k = min(k, len(locations))
    
    #Randomly add facilities for leftover budget
    if k>len(facilities):
        unopened_facilities = set(locations)-facilities
        for i in range(k-len(facilities)):
            facilities.add(unopened_facilities.pop())
    
    return list(facilities)
