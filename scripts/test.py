import random
import json
from mobile.heuristics import *
from mobile.utils import calculate_objective

def contains_float(X):
    
    for val in X:
        if not val.is_integer(): return True
    return False

def lp_experiment(G, client_locations, k):
    """
    Work in progress, need to check
    """
    #Solves the relaxed k-center linear program for multiple client locations
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    print(X)
    
    fixed_variables = set()
    while contains_float(X):
    #Independently rounds on X, then reassigns Y according to X
        epsilon = 0.1
        X_important = [i for i in range(len(X)) if i not in fixed_variables and (X[i]>1-epsilon or X[i]<epsilon)]

        while len(X_important) == 0:
            epsilon += 0.1
            X_important = [i for i in range(len(X)) if i not in fixed_variables and (X[i]>1-epsilon or X[i]<epsilon)]
        
        for var in X_important:
            fixed_variables.add(var)
            val = round(X[var])
            my_lp.set_variable(var, val)

        my_lp.solve_lp()
        X, Y = my_lp.get_variable_solution()
        print(X)
    
    facilities = [ind for ind in range(len(X)) if X[ind]==1]
    assignments = assign_facilities(G, client_locations, facilities)
    format_location_output(facilities, assignments)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, assignments)))
    
    return facilities, assignments

def test_function(k:int, s:int):
    """
    Runs each heuristic and calculates the objective value over all clients and client locations
    """

    
    print("----------Center of Homes----------------")
    X_home, Y_home = center_of_homes(k)
    print(X_home)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_home)))
    
    print("----------Center of Centers--------------")
    X_center, Y_center = center_of_centers(k)
    print(X_center)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_center)))
    
    print("----------Most Populous Centers--------------")
    X_pop, Y_pop = most_populous(k)
    print(X_pop)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_pop)))
    
    print("----------------FPT----------------------")
    X_fpt, Y_fpt = fpt(k, s)
    print(X_fpt)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_fpt)))

"""
FPT performance w.r.t. different k,s combinations
"""
def fpt_sensitivity_experiments(k_lim: int, s_lim: int, aggregation: int = 1):
    
    data = {}
    for k in range(5, k_lim+1):
        for s in range(10, s_lim+1, 5):
            X_fpt, Y_fpt = fpt3_parallel2(k, s, aggregation)
            obj_value = calculate_objective(Y_fpt)
            
            data[str((k, s))] = {"facilities": X_fpt[1], "assignments": Y_fpt, "obj_value": obj_value}
            print(k, s, X_fpt, obj_value)
    
    filename = f"fpt_exp_sensitivity_{aggregation}.json"
    with open(filename, 'w') as f:
        json.dump(data, f)

"""
Compare fpt performance on different versions of aggregation
(Not yet completed since aggregation versions aren't clear yet)
"""
def fpt_version_experiments(k: int, s: int):
    
    data = {}
    
    X_fpt, Y_fpt = fpt3_parallel2(k, s, 2)
    obj_value = calculate_objective(Y_fpt)
    data["Repeats"] = {"k": k, "s": s, "facilities": X_fpt, "assignments": Y_fpt, "obj_value": obj_value}
    print(k, s, X_fpt, obj_value)
    
    X_fpt, Y_fpt = fpt3_parallel2(k, s, 1)
    obj_value = calculate_objective(Y_fpt)
    data["No_Repeats"] = {"k": k, "s": s, "facilities": X_fpt, "assignments": Y_fpt, "obj_value": obj_value}
    print(k, s, X_fpt, obj_value)
    
    filename = "fpt_exp_version.json"
    with open(filename, 'w') as f:
        json.dump(data, f)

def single_fpt_run(k: int, s: int, aggregation: int):
    facilities, assignments = fpt3_parallel2(k, s, aggregation)
    print(facilities)
    obj_value = calculate_objective(assignments)
    print(obj_value)
    
    filename = f"fpt_{k}_{s}_{aggregation}.json"
    data = {"k": k, "s": s, "facilities": facilities, "assignments": assignments, "obj_value": obj_value}
    with open(filename, 'w') as f:
        json.dump(data, f)

"""
What happens when we pick different centers for clients?
"""
def center_of_centers_experiments(k: int):
    facilities, assignments = center_of_centers2(k)
    print(facilities)
    print(len(LOCATIONS))
    print(calculate_objective(assignments))
    
    facilities, assignments = center_of_centers(k)
    print(facilities)
    print(len(LOCATIONS))
    print(calculate_objective(assignments))

"""
Runs all the heuristics we have (excluding LPs)
"""
def run_all_heuristics(k: int, s: int, aggregation: int = 0):
    
    fac_home, assign_home = center_of_homes_agg(k, aggregation)
    print("CENTER OF HOMES : ", fac_home)
    print(calculate_objective(assign_home))
    
    fac_center, assign_center = center_of_centers_agg(k, aggregation)
    print("CENTER OF CENTERS : ", fac_center)
    print(calculate_objective(assign_center))
    
    fac_pop, assign_pop = most_populous_agg(k, aggregation)
    print("MOST POPULAR : ", fac_pop)
    print(calculate_objective(assign_pop))
    
    fac_coverage, assign_coverage = most_coverage_agg(k, aggregation)
    print("MOST COVERAGE : ", fac_coverage)
    print(calculate_objective(assign_coverage))
    
    fac_fpt, assign_fpt = fpt3_parallel2(k, s, aggregation)
    print("FPT : ", fac_fpt)
    print(calculate_objective(assign_fpt))
    
    
"""X_ind, Y_ind = integer_LP(5, 10, 10)
print(X_ind)
print("Recalculated Objective Value: \t" + str(calculate_objective(Y_ind)))"""
test_function(5, 10)
run_all_heuristics(5, 10, 2)
#single_fpt_run(6, 25, 1)