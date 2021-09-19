from mobile import PROJECT_ROOT
import json
import tqdm
from mobile.utils import *
from mobile.heuristics import *

with open(PROJECT_ROOT/ 'output'/ 'runs'/ 'charlottesville_city' / f'neighbors.json', 'r') as f:
    data = json.load(f)
    neighbors = data['neighbors']
    print(len(neighbors))

def cover_approx(neighbors, k: int):
    
    l = 1.5
    h = 2.5
    
    facilities = []
    objective = 10005
    
    #alpha = 0
    
    #for i in range(1, len(CLIENT_LOCATIONS)+1):
    #    alpha += 1/i
    
    #print(alpha)
    
    alpha = 1
    
    #caclulate neighbors
    
    while h-l > 1e-6:
        r = (l+h)/2
        
        print(r)
        
        sol = set_cover(neighbors, radius = r)
        
        if len(sol) <= alpha * k:
            h = (l+h)/2
            facilities = sol
            objective = r
        else:
            l = (l+h)/2
            
        print(r, len(sol))
        
    return facilities, objective

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
    
    return chosen

facilities, obj = cover_approx(neighbors, 5)
print(obj, len(facilities), facilities)

with open(PROJECT_ROOT/ 'output'/ 'runs'/ 'charlottesville_city' / f'find_cover.json', 'r') as f:
    json.dump({"facilities": facilities, "obj_value": obj})