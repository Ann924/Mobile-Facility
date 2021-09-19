import random
import json
from mobile.heuristics import *
from mobile.utils import *
from mobile.config import LOCATIONS, CLIENT_LOCATIONS
from mobile import PROJECT_ROOT
import time
import tqdm

def generate_sorted_neighbors_list_2(radius: float):

    start = time.time()

    radius_dict = {}

    LOCATIONS_act = [l for l in range(len(LOCATIONS)) if not LOCATIONS[l]['home']]

    for l in tqdm.tqdm(LOCATIONS_act):
        
        clients_covered = set()
        
        for j in range(len(LOCATIONS)):
            dist = calculate_distance(l, j)
            
            if dist <= radius:
                clients_covered = clients_covered.union(LOCATIONS[j]['pid'])
        
        radius_dict[l] = clients_covered
    
    end = time.time()

    print(end-start)
    
    return radius_dict


def set_cover_2(radius: float):
    
    radius_dict = generate_sorted_neighbors_list_2(radius)
    
    covered = set()
    chosen = set()
    
    while len(covered) != len(CLIENT_LOCATIONS):
        
        max_coverage = (0, -1, set())
        
        for loc in radius_dict.keys():
            
            if loc not in chosen:
            
                radius_dict[loc] = radius_dict[loc] - covered

                individuals_covered = radius_dict[loc]

                if len(individuals_covered) > max_coverage[0]:
                    max_coverage = (len(individuals_covered), loc, individuals_covered)
        
        if max_coverage[0] == 0:
            break
        
        covered = covered.union(max_coverage[2])
        chosen.add(max_coverage[1])
    
    return chosen, radius_dict


def generate_sorted_list():
    start = time.time()
    
    neighbors = {}
    
    LOCATIONS_act = [l for l in range(len(LOCATIONS)) if not LOCATIONS[l]['home']]
    
    for l in tqdm.tqdm(LOCATIONS_act):
        sorted_distance_neighbors = []
        
        for j in range(len(LOCATIONS)):
            dist = calculate_distance(l, j)
            sorted_distance_neighbors.append((dist, j))
            
        sorted_distance_neighbors = sorted(sorted_distance_neighbors, reverse = False)
        neighbors[l] = sorted_distance_neighbors
    
    end = time.time()
    print(end-start)
    
    return neighbors

def set_cover(neighbors, radius: float):
    
    radius_dict = {}
    
    for l, neighbor in tqdm.tqdm(neighbors.items()):
        
        radius_dict[l] = set()
        
        for n in neighbor:
            
            if n[0] <= radius:                
                ngbr = n[1]
                radius_dict[l] = radius_dict[l].union(LOCATIONS[ngbr]['pid'])
            else:
                break
    
    #print(len(radius_dict))
    
    covered = set()
    chosen = set()
    
    while len(covered) != len(CLIENT_LOCATIONS):
        
        max_coverage = (0, -1, set())
        
        for loc in radius_dict.keys():
            
            if loc not in chosen:
            
                radius_dict[loc] = radius_dict[loc] - covered

                individuals_covered = radius_dict[loc]

                if len(individuals_covered) > max_coverage[0]:
                    max_coverage = (len(individuals_covered), loc, individuals_covered)
        
        if max_coverage[0] == 0:
            break
        
        covered = covered.union(max_coverage[2])
        chosen.add(max_coverage[1])
        
        #print(len(covered))
    
    return chosen

radius_to_cover = {}
neighbors = generate_sorted_list()

for radius in range(5, 26, 5):
    
    fac = set_cover(neighbors, radius/10)
    radius_to_cover[radius/10] = list(fac)
    
    print(radius/10, len(fac))
    print({r: len(v) for r,v in radius_to_cover.items()})

with open(PROJECT_ROOT/ 'output'/ 'runs'/ 'charlottesville_city' / f'set_cover.json', 'w') as f:
    json.dump({"chosen_cover": radius_to_cover}, f)

with open(PROJECT_ROOT/ 'output'/ 'runs'/ 'charlottesville_city' / f'neighbors.json', 'w') as f:
    json.dump({"neighbors": neighbors}, f)