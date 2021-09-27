from mobile.config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT
from mobile.utils import *
from mobile.heuristics import *
from mobile import PROJECT_ROOT

import numpy as np
import scipy
import scipy.stats
import scipy.special
import ray
import time
import geopy
import json



data = {}

for alg in [center_of_homes, most_coverage, most_populous]:
    data[alg.__name__] = []
    
    for num_facility in range(3,11,1):
        (fac, asgn) = alg(num_facility)
        obj = calculate_objective(asgn, 95)
        
        data[alg.__name__].append([fac, asgn, obj])
        
neighbors = generate_sorted_list()
data['cover_approx'] = []

for k in range(3,11,1):
    (fac, obj_fake) = cover_approx(neighbors, k)
    asgn = assign_facilities(fac)
    obj = calculate_objective(asgn, 95)
    data['cover_approx'].append([fac, asgn, obj])
    
data['fpt15'] = []

for k in range(3,11,1):
    (min_obj_guess, asgn) = fpt(k, 15)
    fac = min_obj_guess[1]
    obj = calculate_objective(asgn, 95)
    data['fpt15'].append([fac, asgn, obj])

with open(PROJECT_ROOT / 'output' / 'runs' / 'charlottesville_city' / 'tradeoff.json', 'w') as f:
        json.dump(data, f)