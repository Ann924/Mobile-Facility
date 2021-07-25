import networkx as nx
import random
from heuristics import *
from utils import *

#Possible heuristics: independent rounding, dependent rounding, dispersion (Madhav's version), k-median (w/ houses), high traffic areas
#TODO: test more distances and costs

def test_LP():
    k = 10
    G = generate_input()
    print("-------------------G----------------------")
    for row in G:
        print(['{:.2f}'.format(i) for i in row])
    client_locations = [list({int(10*random.random()) for i in range(int(9*random.random()+1))}) for k in range(int(9*random.random()+1))]
    print("----------CLIENT LOCATIONS----------------")
    print(client_locations)
    
    print("------------IND ROUNDING------------------")
    X_ind, Y_ind = independent_LP(G, client_locations, k)
    format_location_output(X_ind, Y_ind)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_ind)))
    print("------------INTEGER LP--------------------")
    X_int, Y_int = integer_LP(G, client_locations, k)
    format_location_output(X_int, Y_int)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_int)))
    print("------------DEP ROUNDING-----------------")
    X_dep, Y_dep = dependent_LP(G, client_locations, k)
    format_location_output(X_dep, Y_dep)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_dep)))

def test_K():
    k = 10
    G = generate_input()
    print("-------------------G----------------------")
    for row in G:
        print(['{:.2f}'.format(i) for i in row])
    client_locations = [list({int(10*random.random()) for i in range(int(9*random.random()+1))}) for k in range(int(9*random.random()+1))]
    print("----------CLIENT LOCATIONS----------------")
    print(client_locations)
    X_home, Y_home = center_of_homes(G, client_locations, k)
    format_location_output(X_home, Y_home)

def test_line(number_of_points:int, distance_range:Tuple[float, float]):
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
    
    return G

def test_function(number_of_points:int, distance_range:Tuple[float, float], number_of_clients:Tuple[int, int], number_of_visits: Tuple[int, int], k:int):
    
    print("-------------------G----------------------")
    G = test_line(number_of_points, distance_range)
    for row in G:
        print(['{:.2f}'.format(i) for i in row])
    
    print("----------CLIENT LOCATIONS----------------")
    client_locations = [list({int(number_of_points*random.random()) 
                              for i in range(int((number_of_visits[1]-number_of_visits[0])*random.random() + number_of_visits[0]))})
                        for k in range(int((number_of_clients[1]-number_of_clients[0])*random.random()+number_of_clients[0]))]
    for i, row in enumerate(client_locations):
        print(str(i) + "\t" + str(row))

    print("------------IND ROUNDING------------------")
    X_ind, Y_ind = independent_LP(G, client_locations, k)
    format_location_output(X_ind, Y_ind)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_ind)))
    
    print("------------INTEGER LP--------------------")
    X_int, Y_int = integer_LP(G, client_locations, k)
    format_location_output(X_int, Y_int)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_int)))
    
    print("------------DEP ROUNDING-----------------")
    X_dep, Y_dep = dependent_LP(G, client_locations, k)
    format_location_output(X_dep, Y_dep)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_dep)))
    
    print("----------------FPT----------------------")
    X_fpt, Y_fpt = fpt(G, client_locations, k)
    format_location_output(X_fpt, Y_fpt)
    print("Recalculated Objective Value: \t" + str(calculate_objective(G, Y_fpt)))

test_function(5, (1,1), (1,3), (2,4), 2)