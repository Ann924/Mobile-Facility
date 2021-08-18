import random
import numpy as np
from collections import namedtuple
from utils import precompute_distances 
from config import address
import time
import ray
from joblib import Parallel, delayed

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
    def __init__(self, facility_locations: List[int], client_locations: List[List[int]], k: int, solver_id = "GLOP"):
        
        #Set up client locations and possible facility locations
        self.facility_locations = facility_locations
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
        for node in self.facility_locations:
            self.X[node] = self.solver.NumVar(0, 1, f"x_{node}")
        
        #Set indicator variables for indicating an individual's assignment to a location and facility
        self.Y: Dict[Tuple[int, int, int], Variable] = {}
        self.Y_ind_address: Dict[int, List[Tuple[int, int, int]]] = {}
        for ind in range(len(self.client_locations)):
            self.Y_ind_address[ind] = []
            for loc in self.client_locations[ind]:
                #Will not assign a client from a visited location to facility that is another visited location
                for node in self.facility_locations:
                    if node == loc or node not in self.client_locations[ind]:
                        self.Y[address(ind, loc, node)] = self.solver.NumVar(0, 1, f"y_{ind, loc, node}")
                        self.Y_ind_address[ind].append(address(ind, loc, node))
        
        
        self.w = self.solver.NumVar(0, self.solver.infinity(), 'w')
        
        print('Number of variables =', self.solver.NumVariables())

    def init_constraints(self):
        #Setting the constraint for the number of open facilities
        self.budget = self.solver.Constraint(0, self.k, 'budget')
        for ind in self.X.keys():
            self.budget.SetCoefficient(self.X[ind], 1)
        
        #Setting the constraint for no open facilities at homes
        
        '''self.home = self.solver.Constraint(0, 0, 'home')
        for location_list in self.client_locations:
            self.home.SetCoefficient(self.X[location_list[0]], 1)'''
        
        start = time.time()

        G, loc_map, c_loc_map = precompute_distances(self.client_locations, self.facility_locations)
            
        end = time.time()
        
        print("distances calculated", end-start)
        
        start = time.time()
        
        """def process(self, ind):
            person_limit: Constraint = self.solver.Constraint(1,1, 'person_limit')
            for address in self.Y_ind_address[ind]:
                person_limit.SetCoefficient(self.Y[address], 1)
                self.solver.Add(self.Y[address] <= self.X[address.facility])
            self.solver.Add(self.w >= self.solver.Sum([self.Y[address]* G[loc_map[address.facility]][c_loc_map[address.location]] for address in self.Y_ind_address[ind]]))

        results = Parallel(n_jobs=40, verbose = 1)(delayed(process)(self, ind) for ind in range(len(self.client_locations)))"""
        
        """Why does this take so LONG?"""
        """ray.init(ignore_reinit_error=True)
        
        @ray.remote
        def set_constraints(self, ind):
            person_limit: Constraint = self.solver.Constraint(1,1, 'person_limit')
            for address in self.Y_ind_address[ind]:
                person_limit.SetCoefficient(self.Y[address], 1)
                self.solver.Add(self.Y[address] <= self.X[address.facility])
            
            self.solver.Add(self.w >= self.solver.Sum([self.Y[address]* G[loc_map[address.facility]][c_loc_map[address.location]] for address in self.Y_ind_address[ind]]))
        
        for ind in range(len(self.client_locations)):
            set_constraints.remote(self, ind)"""
        
        for ind in range(len(self.client_locations)):
            person_limit: Constraint = self.solver.Constraint(1,1, 'person_limit')
            #max_limit = []
            for address in self.Y_ind_address[ind]:
                person_limit.SetCoefficient(self.Y[address], 1)
                self.solver.Add(self.Y[address] <= self.X[address.facility])
                #max_limit.append(self.Y[address]* G[loc_map[address.facility]][c_loc_map[address.location]])
            #self.solver.Add(self.solver.Sum([self.Y[address] for address in self.Y_ind_address[ind]]) == 1)
            #self.solver.Add(self.w >= self.solver.Sum(max_limit))
            self.solver.Add(self.w >= self.solver.Sum([self.Y[address]* G[loc_map[address.facility]][c_loc_map[address.location]] for address in self.Y_ind_address[ind]]))
        
        end = time.time()
        print(end-start)
        
        print('Number of constraints =', self.solver.NumConstraints())
    
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
            return {ind: self.X[ind].solution_value() for ind in self.X.keys()}, {address: self.Y[address].solution_value() for address in self.Y.keys()}
        else:
            print("Not optimal")
            return {}, {}
    
    def get_objective_solution(self):
        if self.status == pywraplp.Solver.OPTIMAL:
            return self.objective.Value()
        else:
            print("Not optimal")
            return -1
        
class MILP(LP):
    def __init__(self, facility_locations: List[int], client_locations: List[List[int]], k: int, solver_id = 'SCIP'):
        super().__init__(facility_locations, client_locations, k, solver_id)
    
    def init_variables(self):
        #Set indicator variables for indicating whether a facility is open
        self.X: Dict[int, Variable] = {}
        for node in self.facility_locations:
            self.X[node] = self.solver.IntVar(0, 1, f"x_{node}")
        
        #Set indicator variables for indicating an individual's assignment to a location and facility
        self.Y: Dict[Tuple[int, int, int], Variable] = {}
        self.Y_ind_address: Dict[int, List[Tuple[int, int, int]]] = {}
        for ind in range(len(self.client_locations)):
            self.Y_ind_address[ind]= []
            for loc in self.client_locations[ind]:
                #Will not assign a client from a visited location to facility that is another visited location
                for node in set(self.facility_locations)-(set(self.client_locations[ind])-{loc}):
                    self.Y[address(ind, loc, node)] = self.solver.IntVar(0, 1, f"y_{ind, loc, node}")
                    self.Y_ind_address[ind].append(address(ind, loc, node))
        
        self.w = self.solver.NumVar(0, self.solver.infinity(), 'w')
        
        print('Number of variables =', self.solver.NumVariables())
        
        
def independent_LP(k: int, facility_limit: int, client_limit: int):
    """
    PARAMETERS
    ----------
    k
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = list(range(facility_limit))
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    if client_limit <= len(client_locations):
        client_locations = client_locations[:client_limit]
    
    print(len(potential_facility_locations), len(client_locations))
    
    #Solves the relaxed k-center linear program for multiple client locations
    my_lp = LP(list(potential_facility_locations), client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_index_map = {}
    X_list = []
    for i, k in enumerate(X.keys()):
        X_index_map[i] = k
        X_list.append(X[k])
    
    #Independently rounds on X, then reassigns Y according to X
    X_rounded = [1 if random.random()<= x else 0 for x in X_list]
    facilities = [X_index_map[ind] for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    assignments = assign_facilities(facilities)
    
    return facilities, assignments

def integer_LP(k: int, facility_limit: int, client_limit: int):
    """
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = list(range(facility_limit))
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    if client_limit <= len(client_locations):
        client_locations = client_locations[:client_limit]
    
    print(len(potential_facility_locations), len(client_locations))
    
    #Solves the integer linear program for multiple client locations
    my_lp = MILP(potential_facility_locations, client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()

    facilities: List[int] = [ind for ind in X.keys() if X[ind]==1]
    assignments: List[Tuple[int, int]] = assign_facilities(facilities)
    #for address, indicator in Y.items():
    #    if indicator == 1:
    #        assignments[address.index] = (address.location, address.facility)
    
    return facilities, assignments

def dependent_LP(k: int, facility_limit: int, client_limit: int):
    """
    PARAMETERS
    ----------
    k : int
        number of facilities to be opened
    
    RETURNS
    ----------
    facilities : List[int]
        contains facility indices that are open
    assignments : List[Tuple[int, int]]
        visited location and facility assignment indexed by each client
    """
    potential_facility_locations = list(range(facility_limit))
    
    client_locations = []
    for person in CLIENT_LOCATIONS.values():
        new_list = [p for p in person if p in potential_facility_locations]
        if len(new_list)>0:
            client_locations.append(new_list)
    
    if client_limit <= len(client_locations):
        client_locations = client_locations[:client_limit]
    
    print(len(potential_facility_locations), len(client_locations))
    
    #Solves the relaxed linear program for multiple client locations and uses dependent rounding on X
    my_lp = LP(list(potential_facility_locations), client_locations, k)
    my_lp.solve_lp()
    X, Y = my_lp.get_variable_solution()
    
    X_index_map = {}
    X_list = []
    for i, k in enumerate(X.keys()):
        X_index_map[i] = k
        X_list.append(X[k])
    
    X_rounded = D_prime(np.array(X_list))
    facilities = [X_index_map[ind] for ind in range(len(X_rounded)) if X_rounded[ind]==1]
    assignments = assign_facilities(facilities)
    
    return facilities, assignments
