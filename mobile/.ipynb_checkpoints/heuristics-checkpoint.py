import random
from typing import Dict, List, Tuple, Set
from mobile.utils import *
from mobile.config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT
import time
import ray
import numpy as np
import scipy
import scipy.stats
import scipy.special
from joblib import Parallel, delayed

def cover_approx(neighbors, k: int):
    
    l = 0.1
    h = 2.5
    
    facilities = []
    objective = 10005
    
    #alpha = 0
    
    #for i in range(1, len(CLIENT_LOCATIONS)+1):
    #    alpha += 1/i
    
    #print(alpha)
    
    alpha = 1
    
    while h-l > 1e-3:
        r = (l+h)/2
        
        print(r)
        
        sol = set_cover_softmax(neighbors, radius = r, top=10, times = 40)
        
        if len(sol) <= alpha * k:
            h = (l+h)/2
            facilities = sol
            objective = r
        else:
            l = (l+h)/2
        
    return facilities, objective

def set_cover_softmax(neighbors, radius: float, top: int = 1, times: int = 1):

    radius_dict = {}

    for l, neighbor in tqdm.tqdm(neighbors.items()):

        radius_dict[l] = set()

        for n in neighbor:

            if n[0] <= radius:                
                ngbr = n[1]
                radius_dict[l] = radius_dict[l].union(LOCATIONS[ngbr]['pid'])
            else:
                break
    
    total_length = len(CLIENT_LOCATIONS)
    #radius_dict_id = ray.put(radius_dict)
    
    results = []
    #@ray.remote
    #def process(radius_dict):
        #print('starting process')
    for i in range(times):
        covered = set()
        chosen = set()

        while len(covered) != total_length:

            max_coverage = []

            for loc in radius_dict.keys():

                if loc not in chosen:

                    individuals_covered = radius_dict[loc] - covered
                    max_coverage.append((len(individuals_covered), loc, individuals_covered))

            max_coverage = sorted(max_coverage, reverse = True)

            if max_coverage[0][0] == 0:
                break

            choice = max_coverage[scipy.stats.boltzmann.rvs(lambda_=0.8, N=top)]

            covered = covered.union(choice[2])
            chosen.add(choice[1])
            #print(len(covered))

        #return (len(chosen), chosen, covered)
        results.append((len(chosen), chosen, covered))

    #print("here")
    #results = [ray.get(process.remote(radius_dict_id)) for _ in range(times)]
    results = sorted(results)
       
    
    return results[0][1]

def fpt(k: int, s: int):
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
    
    potential_facility_locations = cover_most(s)
    
    #Remove homes from the client_location lists
    #TODO: Perhaps create mapping for the indices of people before exclusion and after?
    client_locations_excluded = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person[1:] if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations_excluded.append(new_list)
    
    locations = [i for i in range(len(LOCATIONS)) if not LOCATIONS[i]['home']]
    
    G, loc_map, c_loc_map = precompute_distances(client_locations_excluded, locations)
    
    ray.init(ignore_reinit_error=True)
    
    @ray.remote
    def process(guess):
        facilities = k_supplier(list(guess), locations, k)
        obj_value = assign_client_facilities(G, loc_map, c_loc_map, client_locations_excluded, facilities)
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
        
    locations = [i for i in range(len(LOCATIONS)) if not LOCATIONS[i]['home']]
    facilities = k_supplier(clients, locations, k)
    
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
    #print(len(LOCATIONS))
    
    potential_facility_locations = [key for key in range(len(LOCATIONS)) if not LOCATIONS[key]['home']]
    homes = set(locs[0] for locs in CLIENT_LOCATIONS.values())
    
    facilities = k_supplier(list(homes), potential_facility_locations, k)
    
    return facilities, assign_facilities(facilities)

def most_coverage(k: int):
    
    facilities = cover_most(k)
    return facilities, assign_facilities(facilities)

def most_populous(k: int):
    facilities = [i for i in range(k)]
    return facilities, assign_facilities(facilities)