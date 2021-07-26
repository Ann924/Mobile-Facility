from typing import Dict, List, Tuple, Set
import random
from collections import namedtuple
from itertools import chain, combinations

# TODO: Change Y structure (from indicator variables to direct mapping)

address = namedtuple('address', ['index', 'location', 'facility'])

def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def cost(G: List[List[float]], loc1, loc2):
    """
    Given two locations, returns the distance (or the cost) between them
    
    PARAMETERS
    ----------
        G
            a diagonal 2D matrix, filled from the bottom left corner
            ex:  Three points in a line, each a distance of 1 from one another
                 G = [[0],
                      [1, 0],
                      [2, 1, 0]]
        loc1
            first location index
        loc2
            second location index (order does not matter)
    
    RETURNS
    ----------
        cost
            distance between the two locations
    """
    
    if loc1==loc2: return 0
    elif loc1 < loc2:
        return G[loc2][loc1]
    else:
        return G[loc1][loc2]

# TODO: What if homes are excluded from the problem entirely
def assign_facilities(G: List[List[float]], client_locations: List[List[int]], facilities: List[int]):
    """
    Assigns clients to their nearest facility from one of their visited locations.
    
    PARAMETERS
    ----------
        G
            diagonal adjacency matrix for location distances
        client_locations
            clients represented by index, contains a list of locations visited by each indexed client
        open_facilities
            list of facilities that are open
    RETURNS
    ----------
        assignments
            lists (visited location, facility) assignment for each client
    """
    
    assignments: List[Tuple[int, int]] = []
    
    for index in range(len(client_locations)):
        possible_assignments = [(cost(G, loc, fac), loc, fac) for loc in client_locations[index] for fac in facilities]
        min_loc = min(possible_assignments)
        assignments.append((min_loc[1], min_loc[2]))
   
    return assignments

#TODO: What happens if homes are excluded are excluded from the problem entirely
def calculate_objective(G: List[List[float]], assignments: List[Tuple[int, int]]) -> float:
    """
    Calculates the maximum distance for any individual based on the assignments
    """
    obj_val = [cost(G, loc, fac) for loc,fac in assignments]
    return max(obj_val)

def format_location_output(facilities: List[int], assignments: List[Tuple[int, int]]):
    """
    Prints out the opened facilities and the assignments corresponding to those facilities
    """
    print("Facilities Opened: \t" + str(facilities))
    print("Client Assignment: \t" + str(assignments))