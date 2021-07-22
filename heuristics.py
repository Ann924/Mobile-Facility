import networkx as nx
import random
from typing import Dict, List, Tuple, Set
from problem import *
from round import *
from utils import cost, generate_input, assign_facilities

#Possible heuristics: independent rounding, dependent rounding, dispersion (Madhav's version), k-median/k-center (w/ houses), high traffic areas

def independent_LP(G:List[List[int]], client_locations:List[List[int]], k: int):
    """
    """
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    X_rounded = [1 if random.random()<= x else 0 for x in X]
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return X_rounded, Y_reassigned

def integer_LP(G:List[List[int]], client_locations:List[List[int]], k: int):
    """
    """
    my_lp = MILP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    return X, Y

def dependent_LP(G:List[List[int]], client_locations:List[List[int]], k: int):
    """
    """
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_rounded = D_prime(np.array(X))
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return X_rounded, Y_reassigned

def integer_k_center(G:List[List[int]], client_locations: List[List[int]], k: int):
    """
    """
    #reformat input of client_locations to contain only the home location (as a list with a single element)
    clients = [[visited[0]] for visited in client_locations]
    
    #use integer program
    my_lp = MILP(G, clients, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    return X, Y

def fpt():
    print()

def center_of_centers():
    print()

def center_of_homes(G: List[List[int]], client_locations: List[List[int]], k: int):
    print()

def _k_supplier(distances: List[List[int]], clients: List[int], locations: List[int], k: int):
    
    possible_OPT = [i for k in distances for i in k]
    possible_OPT.sort()
    
    l = 0;
    r = len(possible_OPT)-1
    to_ret = -1
    pairwise_disjoint = set()
    
    while l <= r:
  
        mid = l + (r - l) // 2;
        
        pairwise_disjoint =  _check_radius(possible_OPT[mid], distances, clients, locations, k)
        if len(pairwise_disjoint) <= k:
            l = mid + 1
            to_ret = mid
        else:
            r = mid - 1
    
    if to_ret >= 0:
        print(possible_OPT[to_ret])
        facilities = _locate_facilities(possible_OPT[to_ret], distances, pairwise_disjoint, locations, k)
        return facilities
    else:
        print("BAD CODE")


def _check_radius(radius: int, distances: List[List[int]], clients: List[int], locations: List[int], k: int):
    
    pairwise_disjoint = set()
    
    V = set(clients)
    while len(V)!=0:
        v = V.pop()
        pairwise_disjoint.add(v)
        remove = set()
        for i in V:
            if cost(distances,v,i)<2*radius:
                remove.add(i)
        V-=remove
    
    return pairwise_disjoint


def _locate_facilities(radius: int, distances: List[List[int]], pairwise_disjoint: Set[int], locations: List[int], k: int):
    
    facilities = set()
    for c in pairwise_disjoint:
        for l in locations:
            if cost(distances, c, l)< 2*radius:
                facilities.add(l)
                break
    return facilities