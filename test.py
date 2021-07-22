import networkx as nx
import random
from problem import *
from heuristics import *
from round import *
from utils import *

#Possible heuristics: independent rounding, dependent rounding, dispersion (Madhav's version), k-median (w/ houses), high traffic areas
#TODO: test more distances and costs

G, client_locations = generate_input()
clients = [int(10*random.random()) for i in range(3)]
#print(clients)

X_ind, Y_ind = independent_LP(G, client_locations, 1)
format_location_output(X_ind, Y_ind)
print("---------------------------------")
X_int, Y_int = integer_LP(G, client_locations, 1)
format_location_output(X_int, Y_int)
print("---------------------------------")
X_dep, Y_dep = dependent_LP(G, client_locations, 1)
format_location_output(X_dep, Y_dep)
print("---------------------------------")
k_center(G, clients, 1)
print("---------------------------------")
integer_k_center(G, clients, 1)