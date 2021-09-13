import random
from typing import Dict, List, Tuple, Set
from mobile import PROJECT_ROOT
from mobile.utils import *
from mobile.config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT
import json
import tqdm
import time

def radius_nbrs(radius: int):
    start = time.time()
    
    neighbors = {}
    
    LOCATIONS_act = [l for l in range(len(LOCATIONS)) if not LOCATIONS[l]['home']]
    
    for l in tqdm.tqdm(LOCATIONS_act):
        dist_nbrs = set()
        
        for j in range(len(LOCATIONS)):
            dist = calculate_distance(l, j)
            if dist < radius:
                dist_nbrs = dist_nbrs.union(LOCATIONS[j]["pid"])
            
        neighbors[l] = dist_nbrs
    
    end = time.time()
    print(end-start)
    
    return neighbors

def cover_most_radius(r: int, k: int):
    """
    Helper method for FPT: returns the set of activity locations of size s that cover the most clients
    Used with aggregate activity locations
    
    aggregation : int
    the version of aggregation selected
    0 --> none
    1 --> set cover: aggregation with repeats in coverage
    """
    
    neighbors = radius_nbrs(r)
    
    covered = set()
    selected = []
    for i in range(k):
        most_coverage = max([(len(neighbors[l] - covered), l, neighbors[l]) for l in neighbors.keys()])
        selected.append(most_coverage[1])
        covered = covered.union(LOCATIONS[most_coverage[1]]['pid'])
    print(f"COVERAGE OF CLIENTS BY {k} {r} LOCATIONS: ", len(covered)/len(CLIENT_LOCATIONS.keys()))
    return selected

data = {}
for i in range(0, 11):
    facilities = cover_most_radius(i/2, 5)
    assignments = assign_facilities(facilities)
    print(i/2, calculate_objective(assignments), facilities)
    data[i/2] = {"facilities": facilities, "assignments": assignments, "obj_value": calculate_objective(assignments)}

with open(PROJECT_ROOT/ 'output'/ 'runs'/ 'charlottesville_city' / f'frequent_cover.json', 'w') as f:
    json.dump(data, f)