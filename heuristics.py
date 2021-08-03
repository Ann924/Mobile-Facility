import networkx as nx
import random
from typing import Dict, List, Tuple, Set
from problem import *
from round import *
from utils import *

# TODO: standardize input structure (especially client_locations)

def independent_LP(k: int):
    """
    PARAMETERS
    ----------
    k
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = list(range(30))
    #locations = [i for i in range(len(LOCATIONS)) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    #potential_facility_locations = set(LOCATION_ASSIGNMENTS.iloc[:10].index)
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person['lid'] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    print(len(potential_facility_locations))
    print(len(client_locations))
    
    #potential_facility_locations = list(potential_facility_locations)
    #Solves the relaxed k-center linear program for multiple client locations
    my_lp = LP(list(potential_facility_locations), client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution
    
    X_index_map = {}
    X_list = []
    for i, k in enumerate(X.keys()):
        X_map[i] = k
        X_list.append(X[k])
    
    #Independently rounds on X, then reassigns Y according to X
    X_rounded = [1 if random.random()<= x else 0 for x in X_list]
    facilities = [X_index_map[ind] for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    assignments = assign_facilities(facilities)
    
    return facilities, assignments

def integer_LP(k: int):
    """
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = set(LOCATION_ASSIGNMENTS.iloc[range(30)].lid)
    #potential_facility_locations = set(LOCATION_ASSIGNMENTS.iloc[:10].index)
    
    client_locations = []
    for person in CLIENT_LOCATIONS.lid:
        #new_list = list(set(person).intersection(potential_facility_locations))
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    print(len(potential_facility_locations))
    print(len(client_locations))
    
    #Solves the integer linear program for multiple client locations
    my_lp = MILP(k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    facilities: List[int] = [ind for ind in X.keys() if X[ind]==1]
    assignments: List[Tuple[int, int]] = [() for i in range(len(CLIENT_LOCATIONS))]
    for address, indicator in Y.items():
        if indicator == 1:
            assignments[address.index] = (address.location, address.facility)
    
    return facilities, assignments

def dependent_LP(k: int):
    """
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = set(LOCATION_ASSIGNMENTS.iloc[range(30)].lid)
    #potential_facility_locations = set(LOCATION_ASSIGNMENTS.iloc[:10].index)
    
    client_locations = []
    for person in CLIENT_LOCATIONS.lid:
        #new_list = list(set(person).intersection(potential_facility_locations))
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    #Solves the relaxed linear program for multiple client locations and uses dependent rounding on X
    my_lp = LP(list(potential_facility_locations), client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_index_map = {}
    X_list = []
    for i, k in enumerate(X.keys()):
        X_map[i] = k
        X_list.append(X[k])
    
    X_rounded = D_prime(np.array(X_list))
    facilities = [X_index_map[ind] for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    assignments = assign_facilities(client_locations, facilities)
    
    return facilities, assignments

def fpt(k: int, s: int):
    """
    Assumes the number of locations visited by clients is bounded by a constant
    Run k-supplier on all combination sets of locations that will be covered by facilities. Select the guess and its open facilities with the smallest objective value.
    
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = list(range(s))
    
    #Remove homes from the client_location lists
    #TODO: Perhaps create mapping for the indices of people before exclusion and after?
    client_locations_excluded = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person['lid'][1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [i for i in range(len(LOCATIONS)) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    
    #Select the guess and resulting facilities that yield the smallest objective value with k-supplier
    min_obj_guess: Tuple[int, List[int], Dict[Tuple[int, int, int]:int]] = (math.inf, [], {})
    
    # TODO : allow all the locations to be facility ones
    for guess in powerset(list(potential_facility_locations)):
        if len(guess)==0: continue
        
        facilities = _k_supplier(list(set(guess)), locations, k)
        assignments, obj_value = assign_client_facilities(client_locations_excluded, facilities)
        
        if obj_value < min_obj_guess[0]:
            min_obj_guess = (obj_value, facilities, assignments)
    
    return min_obj_guess[1], assign_facilities(min_obj_guess[1])

def center_of_centers(k: int):
    """
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    clients = []
    
    for client_row in CLIENT_LOCATIONS.values():
        
        client = client_row["lid"]
        
        dispersion = 1e10
        effective_center = -1
        
        for center in client:
            
            max_dist = 0
            
            for loc in client:
                max_dist = max(calculate_distance(center, loc), max_dist)
                
            if max_dist < dispersion:
                dispersion = max_dist
                effective_center = center
                
        clients.append(effective_center)
        
    locations = [i for i in range(len(LOCATIONS)) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    facilities = _k_supplier(clients, locations, k)
    
    return facilities, assign_facilities(facilities)

def center_of_homes(k: int):
    """
    Opens facilities based only on home-locations
    
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = [key for key in LOCATIONS.keys() if LOCATIONS[key]['lid'] < HOME_SHIFT]
    homes = set(locs['lid'][0] for locs in CLIENT_LOCATIONS.values())
    
    facilities = _k_supplier(list(homes), potential_facility_locations, k)
    
    return facilities, assign_facilities(facilities)

def _k_supplier(clients: List[int], locations: List[int], k: int):
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
    r = 40075
    to_ret = -1
    EPSILON = 10**(-6)
    
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