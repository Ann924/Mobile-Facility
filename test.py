import networkx as nx
import random
from problem import *
from heuristics import *
from round import *
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

def test_case_line():
    G = [[0], [5, 0], [12, 7, 0]]
    clients = [0, 2]
    locations = [1]
    k = 1
    
    
#test_LP()
#test_K()