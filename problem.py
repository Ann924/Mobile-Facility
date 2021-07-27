import random
import numpy as np
from collections import namedtuple
from utils import *

from typing import Dict, List, Tuple, Set
from ortools.linear_solver import pywraplp
from ortools.linear_solver.pywraplp import Variable, Constraint, Objective

#index = index of the individual
#location = Node representing location visited by that individual
#facility = Node at which a facility can be placed

# TODO: will clean up class structures
# TODO: allow variables to be set and then rerun

'''
Data input:
1) Matrix of distances (half-filled)
2) List of lists w/ first element as the home
3) Number of facilities to be opened
'''

class LP:
    def __init__(self, G:List[List[float]], client_locations: List[List[int]], k: int, solver_id = "GLOP"):
        self.G = G
        self.client_locations = client_locations
        self.k = k
        
        self.solver = pywraplp.Solver.CreateSolver(solver_id)
        
        self.init_variables()
        self.init_constraints()
        self.init_objective()
    
    def init_variables(self):
        """
        X is the indicator variable for whether a facility is open
        Y is the indicator variable to assign an individual from one of the travelled locations to the nearest facility
        """
        #Set indicator variables for indicating whether a facility is open
        self.X: Dict[int, Variable] = {}
        for node in range(len(self.G)):
            self.X[node] = self.solver.NumVar(0, 1, f"x_{node}")
        
        #Set indicator variables for indicating an individual's assignment to a location and facility
        self.Y: Dict[Tuple[int, int, int], Variable] = {}
        for ind in range(len(self.client_locations)):
            for loc in self.client_locations[ind]:
                #Will not assign a client from a visited location to facility that is another visited location
                for node in set(range(len(self.G)))-(set(self.client_locations[ind])-{loc}):
                    self.Y[address(ind, loc, node)] = self.solver.NumVar(0, 1, f"y_{ind, loc, node}")
        
        self.w = self.solver.NumVar(0, self.solver.infinity(), 'w')
        
        #print('Number of variables =', self.solver.NumVariables())

    def init_constraints(self):
        #Setting the constraint for the number of open facilities
        self.budget = self.solver.Constraint(0, self.k, 'budget')
        for ind in self.X.keys():
            self.budget.SetCoefficient(self.X[ind], 1)
        
        #Setting the constraint for no open facilities at homes
        '''
        self.home = self.solver.Constraint(0, 0, 'home')
        for location_list in self.client_locations:
            self.home.SetCoefficient(self.X[location_list[0]], 1)'''
        
        #Assigning each person to only one facility
        for ind in range(len(self.client_locations)):
            person_limit: Constraint = self.solver.Constraint(1, 1, 'person_limit')
            for address in self.Y.keys():
                if address.index == ind:
                    person_limit.SetCoefficient(self.Y[address], 1)
        
        for address in self.Y.keys():
            #Finding maximum assignment cost
            self.solver.Add(self.w >= self.Y[address] * cost(self.G, address.location, address.facility))
            #Making sure assignment follow open facilities
            self.solver.Add(self.Y[address] <= self.X[address.facility])
        
        #print('Number of constraints =', self.solver.NumConstraints())
    
    def init_objective(self):
        """
        k-center objective to minimize the maximum distance any client travels
        """
        self.objective = self.solver.Objective()
        self.objective.SetCoefficient(self.w, 1)
        self.objective.SetMinimization()
    
    #Not tested
    def set_variable(self, facility, value):
        self.solver.Add(self.X[facility] == value)

    def solve_lp(self):
        
        self.status = self.solver.Solve()
        
        if self.status == pywraplp.Solver.OPTIMAL:
            print('Objective value =', self.objective.Value())

            #print('Problem solved in %f milliseconds' % self.solver.wall_time())
            #print('Problem solved in %d iterations' % self.solver.iterations())
        else:
            print('The problem does not have an optimal solution.')
    
    def get_variable_solution(self):
        if self.status == pywraplp.Solver.OPTIMAL:
            return [self.X[ind].solution_value() for ind in range(len(self.X))], {address: self.Y[address].solution_value() for address in self.Y.keys()}
        else:
            print("Not optimal")
            return [], {}
    
    def get_objective_solution(self):
        if self.status == pywraplp.Solver.OPTIMAL:
            return self.objective.Value()
        else:
            print("Not optimal")
            return -1
        
class MILP(LP):
    def __init__(self, G:List[List[float]], client_locations: List[List[int]], k: int, solver_id = 'SCIP'):
        super().__init__(G, client_locations, k, solver_id)
    
    def init_variables(self):
        #Set indicator variables for indicating whether a facility is open
        self.X: Dict[int, Variable] = {}
        for node in range(len(self.G)):
            self.X[node] = self.solver.IntVar(0, 1, f"x_{node}")
        
        #Set indicator variables for indicating an individual's assignment to a location and facility
        self.Y: Dict[Tuple[int, int, int], Variable] = {}
        for ind in range(len(self.client_locations)):
            for loc in self.client_locations[ind]:
                #Will not assign a client from a visited location to facility that is another visited location
                for node in set(range(len(self.G)))-(set(self.client_locations[ind])-{loc}):
                    self.Y[address(ind, loc, node)] = self.solver.IntVar(0, 1, f"y_{ind, loc, node}")
        
        self.w = self.solver.NumVar(0, self.solver.infinity(), 'w')
        
        #print('Number of variables =', self.solver.NumVariables())