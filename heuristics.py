import networkx as nx
import random
from typing import Dict, List, Tuple, Set
from problem import *
from round import *
from utils import cost, generate_input, assign_facilities


def independent_LP(G:List[List[int]], client_locations:List[List[int]], k: int):
    """
    Solves the relaxed k-center linear program for multiple client locations
    Independently rounds on X, the indicator variables for which facilities are opened
    """
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    X_rounded = [1 if random.random()<= x else 0 for x in X]
    facilities = [ind for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return facilities, Y_reassigned

def integer_LP(G:List[List[int]], client_locations:List[List[int]], k: int):
    """
    Solves the integer linear program for multiple client locations
    """
    my_lp = MILP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    facilities = [ind for ind in range(len(X)) if X[ind]==1]
    
    return facilities, Y

def dependent_LP(G:List[List[int]], client_locations:List[List[int]], k: int):
    """
    Solves the relaxed linear program for multiple client locations
    Uses dependent rounding on X, the indicator variable for facilities opened
    Reassigns Y (the assignment of each individual from one of their visited locations to a facility) based on the rounded X
    """
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_rounded = D_prime(np.array(X))
    facilities = [ind for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return facilities, Y_reassigned

def integer_k_center(G:List[List[int]], client_locations: List[List[int]], k: int):
    """
    Solve k_center (where each person only has one visited location) with an integer program
    Reformats client_locations as clients: List[List[int]] to contain the first location in each list
    """
    clients = [[visited[0]] for visited in client_locations]

    
    my_lp = MILP(G, clients, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    facilities = [ind for ind in range(len(X)) if X[ind]==1]
    
    return facilities, Y

def fpt(G:List[List[int]], client_locations: List[List[int]], k: int):
    #Working on in notebook
    print()

def center_of_centers():
    print()

def center_of_homes(G: List[List[int]], client_locations: List[List[int]], k: int):
    print()

def _k_supplier(distances: List[List[int]], clients: List[int], locations: List[int], k: int):
    
    #set the radius to ensure there exists a facility within radius r of each client
    max_min_dist = max([min([cost(distances, c, f) for f in locations]) for c in clients])
    
    possible_OPT = [i for k in distances for i in k if i>=max_min_dist]
    possible_OPT.sort()
    
    l = 0;
    r = len(possible_OPT)-1
    to_ret = -1
    pairwise_disjoint = set()
    
    while l <= r:
  
        mid = l + (r - l) // 2;
        
        pairwise_disjoint =  _check_radius(possible_OPT[mid], distances, clients, locations, k)
        if len(pairwise_disjoint) <= k:
            r = mid - 1
            to_ret = mid
        else:
            l = mid + 1
        
    if to_ret >= 0:
        facilities = _locate_facilities(possible_OPT[to_ret], distances, pairwise_disjoint, locations, k)
        return facilities
    else:
        print("NO SOLUTION")

def _check_radius(radius: int, distances: List[List[int]], clients: List[int], locations: List[int], k: int):
    
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


def _locate_facilities(radius: int, distances: List[List[int]], pairwise_disjoint: Set[int], locations: List[int], k: int):
    
    facilities = set()
    for c in pairwise_disjoint:
        for l in locations:
            if cost(distances, c, l) <= 2*radius:
                facilities.add(l)
                break
    
    #Check if k is too large
    k = min(k, len(locations))
    
    #If there are more facilities to open
    if k>len(facilities):
        unopened_facilities = set(locations)-facilities
        for i in range(k-len(facilities)):
            facilities.add(unopened_facilities.pop())
    return list(facilities)