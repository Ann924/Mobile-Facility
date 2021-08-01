from typing import Dict, List, Tuple, Set
import random
import pandas as pd
import geopy.distance
from collections import namedtuple
from itertools import chain, combinations

address = namedtuple('address', ['index', 'location', 'facility'])
HOME_SHIFT=1000000000

def create_location_data():
    df_activity = pd.read_csv("usa_va_charlottesville_city_activity_locations.csv").rename({"alid": "lid"}, axis = 'columns')
    df_residence = pd.read_csv("usa_va_charlottesville_city_residence_locations.csv")
    
    df_residence['rlid'] = df_residence['rlid'].apply(lambda x: x+HOME_SHIFT)
    df_residence = df_residence.rename({"rlid": "lid"}, axis = 'columns')
    locations = pd.concat([df_activity[['lid', 'longitude', 'latitude']], df_residence[['lid', 'longitude', 'latitude']]]).reset_index(drop = True)
    
    location_dict = locations[['lid']]
    location_dict.reset_index(inplace=True)
    location_dict = location_dict.set_index('lid')
    
    client_locations = pd.read_csv("usa_va_charlottesville_city_adult_activity_location_assignment_week.csv")
    home_coords = client_locations.loc[client_locations.activity_type==1,['latitude', 'longitude']]
    home_coords = set(home_coords[['latitude', 'longitude']].apply(tuple, axis=1).tolist())
    
    client_locations['home'] = client_locations.apply(lambda x: True if (x.loc['latitude'], x.loc['longitude']) in home_coords else False, axis = 1)
    client_locations.loc[client_locations.home == True, 'lid'] += HOME_SHIFT
    
    #Popularity of locations, which locations are visited by which individuals
    assignments = client_locations.copy()
    assignments = assignments.groupby(['lid'])['pid'].apply(set).reset_index(name = 'pid')
    assignments['lid'] = assignments['lid'].apply(lambda x: location_dict.loc[x, 'index'])
    assignments['activity'] = assignments['pid'].apply(lambda x: len(x))
    assignments = assignments.sort_values(by = 'activity', ascending = False)
    
    client_locations = client_locations.groupby(['pid'])['lid'].apply(set).reset_index(name = 'lid')

    #Replace lid with the index of the lid in locations
    def filter(x):
        return_list = []
        for i in x:
            #Insert home locations at the front of the list
            if (i>HOME_SHIFT):
                return_list.insert(0, location_dict.loc[i, 'index'])
            else:
                return_list.append(location_dict.loc[i, 'index'])
        return return_list

    client_locations['lid'] = client_locations['lid'].apply(lambda x: filter(x))

    return locations, assignments, client_locations

LOCATIONS, LOCATION_ASSIGNMENTS, CLIENT_LOCATIONS = create_location_data()

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

def calculate_distance(loc1: int, loc2: int):
    coord1 = (LOCATIONS.loc[loc1, 'latitude'], LOCATIONS.loc[loc1, 'longitude'])
    coord2 = (LOCATIONS.loc[loc2, 'latitude'], LOCATIONS.loc[loc2, 'longitude'])
    return geopy.distance.distance(coord1, coord2).km
    
# TODO: What if homes are excluded from the problem entirely
def assign_facilities(client_locations_excluded:List[int], facilities: List[int]):
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
    if len(facilities) == 0: return []
    
    assignments: List[Tuple[int, int]] = []
    
    for index in range(len(client_locations_excluded)):
        possible_assignments = [(calculate_distance(loc, fac), loc, fac) for loc in client_locations_excluded[index] for fac in facilities]
        
        min_loc = min(possible_assignments)
        assignments.append((min_loc[1], min_loc[2]))
   
    return assignments

#TODO: What happens if homes are excluded are excluded from the problem entirely
def calculate_objective(assignments: List[Tuple[int, int]]) -> float:
    """
    Calculates the maximum distance for any individual based on the assignments
    """
    if len(assignments) == 0: return 0
    
    obj_val = [calculate_objective(loc, fac) for loc, fac in assignments]
    return max(obj_val)

def format_location_output(facilities: List[int], assignments: List[Tuple[int, int]]):
    """
    Prints out the opened facilities and the assignments corresponding to those facilities
    """
    print("Facilities Opened: \t" + str(facilities))
    print("Client Assignment: \t" + str(assignments))