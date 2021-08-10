import networkx as nx
import random
from heuristics import *
import utils

#Possible heuristics: independent rounding, dependent rounding, dispersion (Madhav's version), k-median (w/ houses), high traffic areas
#TODO: test more distances and costs

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

def contains_float(X):
    
    for val in X:
        if not val.is_integer(): return True
    return False

def lp_experiment(G, client_locations, k):
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

    '''print("------------IND ROUNDING------------------")
    X_ind, Y_ind = independent_LP(k)
    print(X_ind)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_ind)))'''
    
    '''
    print("------------INTEGER LP--------------------")
    X_int, Y_int = integer_LP(k)
    print(X_int)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_int)))
    
    print("------------DEP ROUNDING-----------------")
    X_dep, Y_dep = dependent_LP(k)
    print(X_dep)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_dep)))'''
    
    '''print("----------------FPT----------------------")
    X_fpt, Y_fpt = fpt(k, s)
    print(X_fpt)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_fpt)))'''
    
    print("----------Center of Homes----------------")
    X_home, Y_home = center_of_homes(k)
    print(X_home)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_home)))
    
    print("----------Center of Centers--------------")
    X_center, Y_center = center_of_centers(k)
    print(X_center)
    print("Recalculated Objective Value: \t" + str(calculate_objective(Y_center)))


test_function(5, 30)