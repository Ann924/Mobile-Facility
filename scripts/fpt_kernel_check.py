import random
from typing import Dict, List, Tuple, Set
from mobile import PROJECT_ROOT
from mobile.utils import *
from mobile.config import LOCATIONS, CLIENT_LOCATIONS, HOME_SHIFT
from mobile.heuristics import fpt
import json

print(len(LOCATIONS))
print(LOCATIONS[0].keys())
print(list(CLIENT_LOCATIONS.values())[0])
#print(LOCATIONS[0])

data = {}
for k in range(5, 11):
    fac, assignments = fpt(k, 25)
    print(fac)
    data[k] = {"facilities":fac[1], "assignments":assignments, "obj_value": calculate_objective(assignments)}

with open(PROJECT_ROOT/ 'output'/ 'runs'/ 'charlottesville_city' / f'kernel_check_25.json', 'w') as f:
    json.dump(data, f)