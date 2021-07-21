import networkx as nx
import random
from problem import *
from round import *
from utils import cost, generate_input, assign_facilities

#Possible heuristics: independent rounding, dependent rounding, dispersion (Madhav's version), k-median (w/ houses), high traffic areas
#TODO: test more distances and costs

def independent_LP(G, client_locations, k):
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    X_rounded = [1 if random.random()<= x else 0 for x in X]
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return X_rounded, Y_reassigned

def integer_LP(G, client_locations, k):
    my_lp = MILP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    return X, Y

def dependent_LP(G, client_locations, k):
    my_lp = LP(G, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_rounded = D_prime(np.array(X))
    Y_reassigned = assign_facilities(G, client_locations, X_rounded)
    
    return X_rounded, Y_reassigned
    
def k_center(G, clients, k):
    my_lp = K_LP(G, clients, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    

def int_k_center(G, clients, k):
    my_lp = integer_K_LP(G, clients, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()