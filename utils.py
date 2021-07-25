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
    Input:
        G is a diagonal 2D matrix, filled from the bottom left corner
        ex:  Three points in a line, each a distance of 1 from one another
             G = [[0],
                  [1, 0],
                  [2, 1, 0]]                                             
    
    Given two locations, returns the distance (or the cost) between them
    """
    
    if loc1==loc2: return 0
    elif loc1 < loc2:
        return G[loc2][loc1]
    else:
        return G[loc1][loc2]

def generate_input(number_of_points:int, distance_range: Tuple[float, float]) -> List[List[float]]:
    """
    number_of_points > 0
    distance_range --> [lower-bound, upper-bound]
    
    Creates and fills in the adjacency matrix (G) of parametrized number of locations with random float distances in inputted range
    """
    
    poi_count = number_of_points
    G = [[(distance_range[1]-distance_range[0])*random.random()+distance_range[0]
          for i in range(poi_count-k)] for k in range(poi_count-1, -1, -1)]
    
    for row in G:
        row[-1] = 0
    
    return G

# TODO: What if homes are excluded from the problem entirely
# TODO: Change format of Y
def assign_facilities(G: List[List[float]], client_locations: List[List[int]], open_facilities: List[int]):
    """
    Input:
        G --> diagonal adjacency matrix for location distances
        client_locations --> clients represented by index, contains a list of locations visited by each indexed client
        open_facilities --> list of facilities that are open
    
    Assigns clients to their nearest facility from one of their visited locations.
    """
    
    #Maps address to indicator variable of optimal assignment
    Y_reassigned: Dict[Tuple[int, int, int]: int] = {}
    
    for index in range(len(client_locations)):
        possible_assignments = []
        for loc in client_locations[index]:
            for fac in open_facilities:
                Y_reassigned[address(index, loc, fac)] = 0
                possible_assignments.append((cost(G, loc, fac), loc, fac))
        
        min_loc = min(possible_assignments)
        Y_reassigned[address(index, min_loc[1], min_loc[2])] = 1
    
    return Y_reassigned

#TODO: If homes are excluded are excluded from the problem entirely
def calculate_objective(G: List[List[float]], Y: Dict[Tuple[int, int, int], int]) -> float:
    """
    Calculates the k-center objective value (maximum distance for any individual) based on the assignment Y
    """
    max_obj_value = 0
    for key in Y.keys():
        obj_val = cost(G, key.location, key.facility)*Y[key]
        if obj_val > max_obj_value:
            max_obj_value = obj_val
    return max_obj_value

def format_location_output(facilities: List[int], Y_reassigned: Dict[Tuple[int, int, int], int]):
    """
    Prints out the opened facilities and the assignments corresponding to those facilities
    """
    print("Facilities Opened: \t" + str(facilities))
    print("Client Assignment: \t" + str([address for address in Y_reassigned.keys() if Y_reassigned[address] == 1]))