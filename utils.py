from typing import Dict, List, Tuple, Set
import random
import pandas as pd
import geopy.distance
from collections import namedtuple
from itertools import chain, combinations

address = namedtuple('address', ['index', 'location', 'facility'])
HOME_SHIFT=1000000000

def create_data_input():
    #Read in both the activity and residence locations
    df_activity = pd.read_csv("usa_va_charlottesville_city_activity_locations.csv").rename({"alid": "lid"}, axis = 'columns')
    df_residence = pd.read_csv("usa_va_charlottesville_city_residence_locations.csv")

    #Shift the residence lid
    df_residence['lid'] = df_residence['rlid'] + HOME_SHIFT
    locations = pd.concat([df_activity[['lid', 'longitude', 'latitude']], df_residence[['lid', 'longitude', 'latitude']]]).reset_index(drop = True)

    #Read in the client visited locations data (adults only for now)
    client_locations = pd.read_csv("usa_va_charlottesville_city_adult_activity_location_assignment_week.csv")

    #Get the coordinates of all the residential locations
    home_coords = set(df_residence[['latitude', 'longitude']].apply(tuple, axis=1).tolist())

    #Shift lids for residential areas
    client_locations['coord'] = client_locations[['latitude', 'longitude']].apply(lambda x: (x.latitude, x.longitude), axis = 1)
    client_locations.loc[client_locations.coord.isin(home_coords), 'lid'] += HOME_SHIFT
    
    #Find popularity of locations, which locations are visited by which individuals
    assignments = client_locations.copy()
    assignments = assignments.groupby(['lid'])['pid'].apply(set).reset_index(name = 'pid')
    assignments = assignments.set_index('lid')
    assignments['activity'] = assignments['pid'].apply(lambda x: len(x))

    locations['activity'] = locations['lid'].apply(lambda x: assignments.at[x, 'activity'] if x in assignments.index else 0)
    locations = locations.sort_values(by = 'activity', ascending = False).reset_index(drop = True)

    client_locations = client_locations.groupby(['pid'])['lid'].apply(set).reset_index(name = 'lid')

    #Replace lid with the index of the lid in locations
    def filter(x):
        return_list = []
        for i in x:
        #Insert home locations at the front of the list
            if (i>HOME_SHIFT):
                return_list.insert(0, locations.loc[locations.lid==i].index[0])
            else:
                return_list.append(locations.loc[locations.lid==i].index[0])
        return return_list

    client_locations['lid'] = client_locations['lid'].apply(lambda x: filter(x))

    return locations.to_dict('index'), client_locations.to_dict('index')

#OUTDATED
'''def create_location_data():
    """
    RETURNS
    --------
    locations: Dict[int: Dict[str: int]]
        Contains lid (location id), latitude, longitude
        Index is used as a key for the lid
    assignments: pd.DataFrame
        Contains lid_index (the index mapping to each lid from locations), a set of pid (personal ids) visiting those locations, 
        and activity (the size of the set) as a measure of popularity
        lid_index is simply titled as 'lid' in the dataframe
    client_locations: Dict[int: Dict[str: int]]
        Contains pid, set of lid_index visited by each pid with the home location as the first element
        lid_index is simply titled as 'lid' in the dataframe
    """
    
    #Read in both the activity and residence locations
    df_activity = pd.read_csv("usa_va_charlottesville_city_activity_locations.csv").rename({"alid": "lid"}, axis = 'columns')
    df_residence = pd.read_csv("usa_va_charlottesville_city_residence_locations.csv")
    
    #Shift the residence lid
    df_residence['rlid'] = df_residence['rlid'].apply(lambda x: x+HOME_SHIFT)
    df_residence = df_residence.rename({"rlid": "lid"}, axis = 'columns')
    locations = pd.concat([df_activity[['lid', 'longitude', 'latitude']], df_residence[['lid', 'longitude', 'latitude']]]).reset_index(drop = True)
    
    #Create a reverse mapping, mapping the lid to the index
    location_dict = locations[['lid']]
    location_dict.reset_index(inplace=True)
    location_dict = location_dict.set_index('lid')
    
    locations = locations.to_dict('index')
    
    #Read in the client visited locations data (adults only for now)
    client_locations = pd.read_csv("usa_va_charlottesville_city_adult_activity_location_assignment_week.csv")
    
    #Get the coordinates of all the residential locations
    home_coords = set(df_residence[['latitude', 'longitude']].apply(tuple, axis=1).tolist())
    
    #Shift lids for residential areas
    client_locations['home'] = client_locations.apply(lambda x: True if (x.loc['latitude'], x.loc['longitude']) in home_coords else False, axis = 1)
    client_locations.loc[client_locations.home == True, 'lid'] += HOME_SHIFT
    
    #Popularity of locations, which locations are visited by which individuals
    assignments = client_locations.copy()
    assignments = assignments.groupby(['lid'])['pid'].apply(set).reset_index(name = 'pid')
    assignments['lid'] = assignments['lid'].apply(lambda x: location_dict.loc[x, 'index'])
    assignments['activity'] = assignments['pid'].apply(lambda x: len(x))
    assignments = assignments.sort_values(by = 'activity', ascending = False).reset_index(drop = True)
    
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
    client_locations = client_locations.to_dict('index')
    
    return locations, assignments, client_locations'''

LOCATIONS, CLIENT_LOCATIONS = create_data_input()

def powerset(iterable):
    "powerset([1,2,3]) --> () (1,) (2,) (3,) (1,2) (1,3) (2,3) (1,2,3)"
    s = list(iterable)
    return chain.from_iterable(combinations(s, r) for r in range(len(s)+1))

def calculate_distance(loc1: int, loc2: int):
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
        assignments
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

def assign_client_facilities(client_locations: List[List[int]], facilities: List[int]):
    """
    Assigns clients to their nearest facility from one of their visited locations.
    
    PARAMETERS
    ----------
        client_locations
            clients represented by index, contains a list of locations visited by each indexed client
        open_facilities
            list of facilities that are open
    RETURNS
    ----------
        assignments
            lists (visited location, facility) assignment for each client
        obj_value
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

#TODO: What happens if homes are excluded are excluded from the problem entirely
def calculate_objective(assignments: List[Tuple[int, int]]) -> float:
    """
    Calculates the maximum distance for any individual based on the assignments
    """
    if len(assignments) == 0: return 0
    
    obj_val = [calculate_distance(loc, fac) for loc, fac in assignments]
    return max(obj_val)

def format_location_output(facilities: List[int], assignments: List[Tuple[int, int]]):
    """
    Prints out the opened facilities and the assignments corresponding to those facilities
    """
    print("Facilities Opened: \t" + str(facilities))
    print("Client Assignment: \t" + str(assignments))

def cost(G, loc1, loc2):
    if loc1 <= loc2: return G[loc2][loc1]
    else: return G[loc1][loc2]
'''
indices may be entirely different
'''
def precompute_distances(client_locations: List[List[int]], locations: List[int]):
    G = []
    loc_map = {}
    client_loc_map = {}
    clients = set(l for loc in client_locations for l in loc)
    for l_ind, l in enumerate(locations):
        loc_map[l_ind] = l
        G.append([0 for i in range(len(locations))])
        for c_ind, c in enumerate(clients):
            client_loc_map[c_ind] = c
            if c_ind<l_ind:
                G[-1][c_ind] = calculate_distance(c, l)
    return G, loc_map, c_loc_map
