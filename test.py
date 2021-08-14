import networkx as nx
import random
import json
from heuristics import *
from utils import calculate_objective

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

    print("------------IND ROUNDING------------------")
    X_ind, Y_ind = independent_LP(k, 10, 10)
    print(X_ind)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_ind)))
    
    
    print("------------INTEGER LP--------------------")
    X_int, Y_int = integer_LP(k, 10, 10)
    print(X_int)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_int)))
    
    print("------------DEP ROUNDING-----------------")
    X_dep, Y_dep = dependent_LP(k, 10, 10)
    print(X_dep)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_dep)))
    
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
    X_fpt, Y_fpt = fpt2_parallel2(k, 20)
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

def center_of_centers_experiments(k: int):
    facilities, assignments = center_of_centers2(k)
    print(facilities)
    print(len(LOCATIONS))
    print(calculate_objective(assignments))
    
    facilities, assignments = center_of_centers(k)
    print(facilities)
    print(len(LOCATIONS))
    print(calculate_objective(assignments))

#NOT USED
'''def test_line(number_of_points:int, distance_range:Tuple[float, float]) -> List[List[float]]:
    """
    number_of_points > 0
    """
    G = []
    for k in range(number_of_points-1, -1, -1):
        G.append([])
        dist_from_previous = (distance_range[1]-distance_range[0])*random.random() + distance_range[0]
        for i in range(number_of_points-k-1):
            #add up distances somehow
            G[-1].append(dist_from_previous + G[-2][i])
        G[-1].append(0)
    
    return G'''
#NOT USED
'''def generate_random_input(number_of_points:int, distance_range: Tuple[float, float]) -> List[List[float]]:
    """
    number_of_points > 0
    distance_range --> [lower-bound, upper-bound]
    
    Creates and fills in the adjacency matrix (G) of parametrized number of locations with random float distances in inputted range
    """
    
    poi_count = number_of_points
    G = [[(distance_range[1]-distance_range[0])*random.random()+distance_range[0]
          for i in range(poi_count-k)] for k in range(poi_count-1, -1, -1)]
    
    for row in G:
        row[-1] = 0
    
    return G'''
#NOT USED
'''def generate_2D_input(number_of_points:int, x_coordinate_range: Tuple[float, float], y_coordinate_range: Tuple[float, float]) -> List[List[float]]:
    
    points = [(random.random()*(x_coordinate_range[1]-x_coordinate_range[0]) + x_coordinate_range[0], random.random()*(y_coordinate_range[1]-y_coordinate_range[0]) + y_coordinate_range[0]) for i in range(number_of_points)]
    #print(points)
    G = []
    for r in range(number_of_points):
        G.append([])
        for c in range(r):
            loc0 = points[r]
            loc1 = points[c]
            distance = ((loc0[0]-loc1[0])**2 + (loc0[1]-loc1[1])**2)**(1/2)
            G[-1].append(distance)
        G[-1].append(0)
    
    return G'''

fpt_sensitivity_experiments(7, 20, 0)
#test_function(5, 30)
#center_of_centers_experiments(5)

"""X_ind, Y_ind = integer_LP(5, 10, 10)
print(X_ind)
print("Recalculated Objective Value: \t" + str(calculate_objective(Y_ind)))"""