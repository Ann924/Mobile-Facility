import random
from typing import Dict, List, Tuple, Set
from problem import *
from round import *
from utils import *
from config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT, aggregate_data
import time
import ray
from joblib import Parallel, delayed

# TODO: standardize input structure (especially client_locations)

def independent_LP(k: int, facility_limit: int, client_limit: int):
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
    potential_facility_locations = list(range(facility_limit))
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    if client_limit <= len(client_locations):
        client_locations = client_locations[:client_limit]
    
    print(len(potential_facility_locations), len(client_locations))
    
    #Solves the relaxed k-center linear program for multiple client locations
    my_lp = LP(list(potential_facility_locations), client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_index_map = {}
    X_list = []
    for i, k in enumerate(X.keys()):
        X_index_map[i] = k
        X_list.append(X[k])
    
    #Independently rounds on X, then reassigns Y according to X
    X_rounded = [1 if random.random()<= x else 0 for x in X_list]
    facilities = [X_index_map[ind] for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    assignments = assign_facilities(facilities)
    
    return facilities, assignments

def integer_LP(k: int, facility_limit: int, client_limit: int):
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
    potential_facility_locations = list(range(facility_limit))
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    if client_limit <= len(client_locations):
        client_locations = client_locations[:client_limit]
    
    print(len(potential_facility_locations), len(client_locations))
    
    #Solves the integer linear program for multiple client locations
    my_lp = MILP(potential_facility_locations, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    facilities: List[int] = [ind for ind in X.keys() if X[ind]==1]
    assignments: List[Tuple[int, int]] = assign_facilities(facilities)
    #for address, indicator in Y.items():
    #    if indicator == 1:
    #        assignments[address.index] = (address.location, address.facility)
    
    return facilities, assignments

def dependent_LP(k: int, facility_limit: int, client_limit: int):
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
    potential_facility_locations = list(range(facility_limit))
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    if client_limit <= len(client_locations):
        client_locations = client_locations[:client_limit]
    
    print(len(potential_facility_locations), len(client_locations))
    
    #Solves the relaxed linear program for multiple client locations and uses dependent rounding on X
    my_lp = LP(list(potential_facility_locations), client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_index_map = {}
    X_list = []
    for i, k in enumerate(X.keys()):
        X_index_map[i] = k
        X_list.append(X[k])
    
    X_rounded = D_prime(np.array(X_list))
    facilities = [X_index_map[ind] for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    assignments = assign_facilities(facilities)
    
    return facilities, assignments

#Outdated
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
        new_list = [p for p in person[1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [i for i in range(len(LOCATIONS)) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    
    #Select the guess and resulting facilities that yield the smallest objective value with k-supplier
    min_obj_guess: Tuple[int, List[int], Dict[Tuple[int, int, int]:int]] = (math.inf, [], {})
    
    # TODO : allow all the locations to be facility ones
    count = 0
    for guess in powerset(list(potential_facility_locations)):
        start = time.time()
        if len(guess)==0: continue
        
        facilities = _k_supplier(list(guess), locations, k)
        assignments, obj_value = assign_client_facilities(client_locations_excluded, facilities)
        
        if obj_value < min_obj_guess[0]:
            min_obj_guess = (obj_value, facilities, assignments)
        end = time.time()
        count +=1
        print(count, obj_value, end-start)
    return min_obj_guess[1], assign_facilities(min_obj_guess[1])

#Outdated
def fpt2(k: int, s: int):
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
        new_list = [p for p in person[1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [i for i in range(len(LOCATIONS)) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    
    #Select the guess and resulting facilities that yield the smallest objective value with k-supplier
    min_obj_guess: Tuple[int, List[int], Dict[Tuple[int, int, int]:int]] = (math.inf, [], {})

    start = time.time()
    
    G, loc_map, c_loc_map = precompute_distances(client_locations_excluded, locations)
    
    print(time.time() - start)
    count = 0
    for guess in powerset(list(potential_facility_locations)):
        start = time.time()
        if len(guess)==0: continue
        
        facilities = _k_supplier(list(guess), locations, k)
        mid = time.time()
        
        obj_value = assign_client_facilities2(G, loc_map, c_loc_map, client_locations_excluded, facilities)
        
        if obj_value < min_obj_guess[0]:
            min_obj_guess = (obj_value, facilities)
        end = time.time()
        count +=1
        print(count, obj_value, end-start)
    return min_obj_guess[1], assign_facilities(min_obj_guess[1])

#Outdated
def fpt2_parallel(k: int, s: int, track_progress = False):
    """
    Assumes the number of locations visited by clients is bounded by a constant
    Run k-supplier on all combination sets of locations that will be covered by facilities. Select the guess and its open facilities with the smallest objective value.
    
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    s: int
        total number of visited locations is bounded by s
    track_progress: bool
        prints progress for parallel computing
    
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
        new_list = [p for p in person[1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [i for i in range(len(LOCATIONS)) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    
    G, loc_map, c_loc_map = precompute_distances(client_locations_excluded, locations)
    
    def process(guess):
        facilities = _k_supplier(list(guess), locations, k)
        obj_value = assign_client_facilities2(G, loc_map, c_loc_map, client_locations_excluded, facilities)
        
        return obj_value, facilities

    results = Parallel(n_jobs=40, verbose = track_progress)(delayed(process)(guess) for guess in powerset(list(potential_facility_locations)))
    
    min_obj_guess: Tuple[int, List[int]] = min(results)
    return min_obj_guess, assign_facilities(min_obj_guess[1])

def fpt2_parallel2(k: int, s: int, aggregation: int):
    """
    Picks the s most popular activity locations
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
    
    LOCATIONS_fpt, CLIENT_LOCATIONS_fpt = aggregate_data(aggregation)
    
    potential_facility_locations = [LOCATIONS_fpt[i]['lid_ind'] for i in range(s)]
    
    #Remove homes from the client_location lists
    #TODO: Perhaps create mapping for the indices of people before exclusion and after?
    client_locations_excluded = []
    for person in CLIENT_LOCATIONS_fpt.values():
        new_list = [p for p in person[1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [LOCATIONS_fpt[i]['lid_ind'] for i in range(len(LOCATIONS_fpt)) if LOCATIONS_fpt[i]['lid'] < HOME_SHIFT]
    
    G, loc_map, c_loc_map = precompute_distances(client_locations_excluded, locations)
    
    ray.init(ignore_reinit_error=True)
    
    @ray.remote
    def process(guess):
        facilities = _k_supplier(list(guess), locations, k)
        obj_value = assign_client_facilities2(G, loc_map, c_loc_map, client_locations_excluded, facilities)
        
        return obj_value, facilities
    
    futures = [process.remote(guess) for guess in powerset(list(potential_facility_locations))]
    results = ray.get(futures)

    min_obj_guess: Tuple[int, List[int]] = min(results)
    
    return min_obj_guess, assign_facilities(min_obj_guess[1])

def fpt3_parallel2(k: int, s: int, aggregation: int):
    """
    Picks the s activity locations that cover the most clients (through a set cover approximation)
    Assumes the number of locations visited by clients is bounded by a constant
    Run k-supplier on all combination sets of locations that will be covered by facilities. Select the guess and its open facilities with the smallest objective value.
    
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    s : int
        number of activity locations examined
    aggregation : int
        the version of aggregation selected
        0 --> none
        1 --> set cover: aggregation without repeats in coverage
        2 --> set cover: aggregation with repeats in coverage
        
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    
    LOCATIONS_fpt, CLIENT_LOCATIONS_fpt = aggregate_data(aggregation)
    
    potential_facility_locations = cover_most(s, LOCATIONS_fpt, CLIENT_LOCATIONS_fpt)
    
    #Remove homes from the client_location lists
    #TODO: Perhaps create mapping for the indices of people before exclusion and after?
    client_locations_excluded = []
    for person in CLIENT_LOCATIONS_fpt.values():
        new_list = [p for p in person[1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [LOCATIONS_fpt[i]['lid_ind'] for i in range(len(LOCATIONS_fpt)) if LOCATIONS_fpt[i]['lid'] < HOME_SHIFT]
    
    G, loc_map, c_loc_map = precompute_distances(client_locations_excluded, locations)
    
    ray.init(ignore_reinit_error=True)
    
    @ray.remote
    def process(guess):
        facilities = _k_supplier(list(guess), locations, k)
        obj_value = assign_client_facilities2(G, loc_map, c_loc_map, client_locations_excluded, facilities)
        return obj_value, facilities
    
    futures = [process.remote(guess) for guess in powerset(list(potential_facility_locations))]
    results = ray.get(futures)

    min_obj_guess: Tuple[int, List[int]] = min(results)
    return min_obj_guess, assign_facilities(min_obj_guess[1])

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
    
    for client in CLIENT_LOCATIONS.values():
        
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

def center_of_centers2(k: int):
    """
    Finds center of each client's locations by calculating the average meridian coordinates.
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
    
    for client in CLIENT_LOCATIONS.values():
        
        latitude = []
        longitude = []
        for center in client:
            lat, long = LOCATIONS[center]['latitude'], LOCATIONS[center]['longitude']
            latitude.append(lat)
            longitude.append(long)
        
        clients.append((sum(latitude)/len(latitude), sum(longitude)/len(longitude)))
    
    original_loc_length = len(LOCATIONS)
    
    for i in range(len(clients)):
        LOCATIONS.append({'lid_ind': i+original_loc_length, 'lid': -1, 'longitude': clients[i][1], 'latitude': clients[i][0], 'activity':-1, 'pid':[i]})
    
    locations = [i for i in range(original_loc_length) if LOCATIONS[i]['lid'] < HOME_SHIFT]
    facilities = _k_supplier(list(range(original_loc_length+ len(clients))), locations, k)
    
    del LOCATIONS[original_loc_length: original_loc_length+len(clients)]
    
    print(len(LOCATIONS))
    
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
    potential_facility_locations = [key for key in range(len(LOCATIONS)) if LOCATIONS[key]['lid'] < HOME_SHIFT]
    homes = set(locs[0] for locs in CLIENT_LOCATIONS.values())
    
    facilities = _k_supplier(list(homes), potential_facility_locations, k)
    
    return facilities, assign_facilities(facilities)

def center_of_homes_agg(k: int, aggregation: int):
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
    
    LOCATIONS_home, CLIENT_LOCATIONS_home = aggregate_data(aggregation)
    
    potential_facility_locations = [LOCATIONS_home[key]['lid_ind'] for key in range(len(LOCATIONS_home)) if LOCATIONS_home[key]['lid'] < HOME_SHIFT]
    homes = set(locs[0] for locs in CLIENT_LOCATIONS_home.values())
    
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

def most_populous(k: int):
    return list(range(k)), assign_facilities
    
def most_populous_agg(k: int, aggregation: int):
    LOCATIONS_agg, CLIENT_LOCATIONS_agg = aggregate_data(aggregation)
    
    facilities = [LOCATIONS_agg[i]['lid_ind'] for i in range(k)]
    return facilities, assign_facilities(facilities)