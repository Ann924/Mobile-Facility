import random
from typing import Dict, List, Tuple, Set
from utils import *
from config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT, aggregate_data
import time
import ray
from joblib import Parallel, delayed

# TODO: standardize input structure (especially client_locations)
# TODO: A LOT OF THINGS

def fpt(k: int, s: int, aggregation: int = 0):
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

def center_of_centers(k: int, aggregation: int = 0):
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
    
    LOCATIONS_center, CLIENT_LOCATIONS_center = aggregate_data(aggregation)
    
    clients = []
    
    for client in CLIENT_LOCATIONS_center.values():
        
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
        
    locations = [LOCATIONS_center[i]['lid_ind'] for i in range(len(LOCATIONS_center)) if LOCATIONS_center[i]['lid'] < HOME_SHIFT]
    facilities = _k_supplier(clients, locations, k)
    
    return facilities, assign_facilities(facilities)

def center_of_homes(k: int, aggregation: int = 0):
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

def most_coverage(k: int, aggregation: int = 0):
    LOCATIONS_agg, CLIENT_LOCATIONS_agg = aggregate_data(aggregation)
    
    facilities = cover_most(k, LOCATIONS_agg, CLIENT_LOCATIONS_agg)
    return facilities, assign_facilities(facilities)

def most_populous(k: int, aggregation: int = 0):
    LOCATIONS_agg, CLIENT_LOCATIONS_agg = aggregate_data(aggregation)
    
    facilities = [LOCATIONS_agg[i]['lid_ind'] for i in range(k)]
    return facilities, assign_facilities(facilities)