import networkx as nx
import random
from typing import Dict, List, Tuple, Set
from problem import *
from round import *
from utils import *

# TODO: standardize input structure (especially client_locations)

def independent_LP(G:List[List[float]], client_locations:List[List[int]], k: int):
    """Solves the relaxed k-center linear program for multiple client locations and independently rounds on X"""
    
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    X_rounded = [1 if random.random()<= x else 0 for x in X]
    facilities = [ind for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return facilities, Y_reassigned

def integer_LP(G:List[List[float]], client_locations:List[List[int]], k: int):
    """Solves the integer linear program for multiple client locations"""
    
    my_lp = MILP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    facilities = [ind for ind in range(len(X)) if X[ind]==1]
    
    return facilities, Y

def dependent_LP(G:List[List[float]], client_locations:List[List[int]], k: int):
    """Solves the relaxed linear program for multiple client locations and uses dependent rounding on X"""
    
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_rounded = D_prime(np.array(X))
    facilities = [ind for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return facilities, Y_reassigned

def fpt(G: List[List[float]], client_locations: List[List[int]], k):
    """
    The number of locations visited by clients is bounded by a constant
    Run k-supplier on all combination sets of locations that will be covered by facilitie and select the guess and its open facilities with the smallest objective value
    """
    
    #Remove homes from the client_location lists
    #TODO: Perhaps create mapping for the indices of people before exclusion and after?
    client_locations_excluded = [person[1:] for person in client_locations if len(person)>1]
    
    #Select the guess and resulting facilities that yield the smallest objective value with k-supplier
    locations = set(loc for clients in client_locations_excluded for loc in clients)
    min_obj_guess: Tuple[int, List[int], Dict[Tuple[int, int, int]:int]] = (math.inf, [], {})
    
    for guess in powerset(list(locations)):
        
        if len(guess)==0: continue
        
        facilities = _k_supplier(G, list(set(guess)), locations, k)
        Y = assign_facilities(G, client_locations_excluded, facilities)
        obj_value = calculate_objective(G, Y)
        
        if obj_value < min_obj_guess[0]:
            min_obj_guess = (obj_value, facilities, Y)
    
    return min_obj_guess[1], min_obj_guess[2]

def center_of_centers(distances: List[List[float]], client_locations: List[List[int]], k: int):
    
    clients = []
    
    for client in client_locations:
        
        dispersion = 1e10
        effective_center = -1
        
        for center in client:
            
            max_dist = 0
            
            for loc in client:
                max_dist = max(distances[center][loc], max_dist)
                
            if max_dist < dispersion:
                dispersion = max_dist
                effective_center = center
                
        clients.append(effective_center)
        
    homes = [locs[0] for locs in client_locations]
    locations = [i for i in range(len(distances)) if i not in homes]
    facilities = _k_supplier(distances, clients, locations, k)
    
    return facilities, assign_facilities(G, [locs[:1] for locs in client_locations], facilities)

    
def center_of_homes(distances: List[List[float]], client_locations: List[List[int]], k: int):
    """Opens facilities based only on home-locations"""
    
    homes = [locs[0] for locs in client_locations]
    locations = [i for i in range(len(distances)) if i not in homes]
    facilities = _k_supplier(distances, homes, locations, k)
    
    return facilities, assign_facilities(G, [locs[:1] for locs in client_locations], facilities)
    

def _k_supplier(distances: List[List[float]], clients: List[int], locations: List[int], k: int):
    """
    Solves k-supplier (where client locations and facility locations may not overlap) with Hochbaum-Shmoys
    3-approximation algorithm
    """
    
    #Set the radius to ensure there exists a facility within radius r of each client
    max_min_dist = max([min([cost(distances, c, f) for f in locations]) for c in clients])
    
    #Binary search on r from the sorted list of distances
    possible_OPT = [i for k in distances for i in k if i>=max_min_dist]
    possible_OPT.sort()
    
    l = 0;
    r = len(possible_OPT)-1
    to_ret = -1
    pairwise_disjoint = set()
    
    while l <= r:
  
        mid = l + (r - l) // 2;
        
        if len(_check_radius(possible_OPT[mid], distances, clients)) <= k:
            r = mid - 1
            to_ret = mid
        else:
            l = mid + 1
        
    if to_ret >= 0:
        facilities = _locate_facilities(possible_OPT[to_ret], distances,
                                        _check_radius(possible_OPT[mid], distances, clients), locations, k)
        return facilities
    else:
        print("No Solution")
        return []

def _check_radius(radius: int, distances: List[List[float]], clients: List[int]):
    """Determine the maximal independent set of pairiwse independent client balls with given radius"""
    
    pairwise_disjoint = set()
    
    V = set(clients)
    while len(V)!=0:
        v = V.pop()
        pairwise_disjoint.add(v)
        remove = set()
        for i in V:
            if cost(distances,v,i) <= 2*radius:
                remove.add(i)
        V-=remove
    
    return pairwise_disjoint


def _locate_facilities(radius: int, distances: List[List[float]], pairwise_disjoint: Set[int], locations: List[int], k: int):
    """Select a facility to open within the given radius for each pairwise_disjoint client"""
    
    facilities = set()
    for c in pairwise_disjoint:
        for l in locations:
            if cost(distances, c, l) <= 2*radius:
                facilities.add(l)
                break
    
    #Check if k larger than the number of possible facility locations
    k = min(k, len(locations))
    
    #Randomly add facilities for leftover budget
    if k>len(facilities):
        unopened_facilities = set(locations)-facilities
        for i in range(k-len(facilities)):
            facilities.add(unopened_facilities.pop())
    
    return list(facilities)