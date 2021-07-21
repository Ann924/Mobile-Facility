import random
import networkx as nx
import numpy as np
from collections import namedtuple

from typing import Dict, List, Tuple, Set
from ortools.linear_solver import pywraplp
from ortools.linear_solver.pywraplp import Variable, Constraint, Objective

address = namedtuple('address', ['ind', 'loc', 'node'])

def cost(G: nx.Graph, node1, node2):
    if node1==node2: return 0
    else:
        return G[node1][node2]['distance']

#TODO: include more functions to get variables back, etc.
class LP:
    
    def __init__(self, G: nx.Graph, client_locations: List[Set[int]], k: int):
        self.G = G
        self.client_locations = client_locations
        self.k = k
        
        self.solver = pywraplp.Solver.CreateSolver('GLOP')
        
        self.init_variables()
        self.init_constraints()
        self.init_objective()
    
    def init_variables(self):
        #Set indicator variables for indicating whether a facility is open
        self.X: Dict[int, Variable] = {}
        for node in self.G.nodes:
            self.X[node] = self.solver.NumVar(0, 1, f"x_{node}")
        
        #Set indicator variables for indicating an individual's assignment to a location and facility
        self.Y: Dict[Tuple[int, int, int], Variable] = {}
        for ind in range(len(self.client_locations)):
            for loc in self.client_locations[ind]:
                #Will not assign a client from a visited location to facility that is another visited location
                for node in set(G.nodes)-(self.client_locations[ind]-{loc}):
                    self.Y[address(ind, loc, node)] = self.solver.NumVar(0, 1, f"y_{ind, loc, node}")
        
        self.w = self.solver.NumVar(0, self.solver.infinity(), 'w')
        
        print('Number of variables =', self.solver.NumVariables())

    def init_constraints(self):
        #Setting the constraint for the number of open facilities
        self.budget = self.solver.Constraint(0, self.k, 'budget')
        for ind in self.X.keys():
            self.budget.SetCoefficient(self.X[ind], 1)
        
        #Assigning one person to only one facility
        for ind in range(len(self.client_locations)):
            person_limit: Constraint = self.solver.Constraint(1, 1, 'person_limit')
            for address in self.Y.keys():
                if address.ind == ind:
                    person_limit.SetCoefficient(self.Y[address], 1)
        
        for address in self.Y.keys():
            #Finding maximum assignment cost
            self.solver.Add(self.w >= self.Y[address] * cost(self.G, address.loc, address.node))
            #Making sure assignment follow open facilities
            self.solver.Add(self.Y[address] <= self.X[address.node])
        
        print('Number of constraints =', self.solver.NumConstraints())
    
    def init_objective(self):
        
        self.objective = self.solver.Objective()
        self.objective.SetCoefficient(self.w, 1)
        self.objective.SetMinimization()

    def solve_lp(self):
        
        self.solver.Solve()
        print('Solution:')
        print('Objective value =', self.objective.Value())
        
        print("X VALUES")
        for ind in range(len(self.X)):
            if self.X[ind].solution_value()>0:
                print(str(ind) + "\t" + str(self.X[ind].solution_value()))
        
        print("Y VALUES")
        for address in self.Y.keys():
            if self.Y[address].solution_value()>0:
                print(str(address) + "\t" + str(self.Y[address].solution_value()))
