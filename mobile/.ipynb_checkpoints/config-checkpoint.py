from collections import namedtuple
from os import path
import json
from mobile import PROJECT_ROOT
from typing import Dict, List, Tuple, Set
import pandas as pd
import geopy.distance

"""
Create global variables

address: namedtuple containing the client index, visited location, and potential facility

HOME_SHIFT : To adjust the lids of residential locations

LOCATIONS : List containing dicts of the following
    "lid" : The lid (with applied HOME_SHIFT) of a given location
    "lid_ind" : The index of each location in LOCATIONS
    "longitude"
    "latitude"
    "activity": The number of clients that visit this location
    "pid" : List of all the pids of clients that visit this location

CLIENT_LOCATIONS : Dict mapping pids of clients to the locations (represented by location indicies from LOCATIONS) each client visits

LOCATIONS_agg : List of dicts of the aggregated locations (within radius 10 m)
<<<<<<< HEAD
    "lid_ind" : The sorted order numbering of aggregated locations
    "longitude" : Longitude of the center
    "latitude" : Latitude of the center
=======
    "lid" : The index of the lid (from LOCATIONS) of the center of the aggregated location
    "lid_ind" : The index of each location in LOCATIONS
    "longitude" : Longitude of the center
    "latitude" : Latitude of the center
    "members" : The lid indices that belong to this cluster
>>>>>>> 1764347dbd3edc0c22a38dd6f65887bc35634632
    "activity" : The number of clients that visit this aggregated cluster of locations
    "pid" : The list of clients that visit this aggregated cluster of locations
    "home" : Is the location a residential location?

CLIENT_LOCATIONS_agg : Dict mapping pids of clients to the aggregated locations
---------

"""
address = namedtuple('address', ['index', 'location', 'facility'])
HOME_SHIFT=1000000000

def create_data_input(county_name: str = 'charlottesville_city'):
    """
    LOCATIONS contains the pid of clients that visit the location
    """
    #Read in both the activity and residence locations
    
    df_activity = pd.read_csv(PROJECT_ROOT/ 'data'/ 'raw'/ county_name / f"usa_va_{county_name}_activity_locations.csv").rename({"alid": "lid"}, axis = 'columns')
    df_residence = pd.read_csv(PROJECT_ROOT/ 'data'/ 'raw'/ county_name/ f"usa_va_{county_name}_residence_locations.csv")

    #Shift the residence lid
    df_residence['lid'] = df_residence['rlid'] + HOME_SHIFT
    locations = pd.concat([df_activity[['lid', 'longitude', 'latitude']], df_residence[['lid', 'longitude', 'latitude']]]).reset_index(drop = True)

    #Read in the client visited locations data (adults only for now)
    client_locations = pd.read_csv(PROJECT_ROOT/ 'data'/ 'raw'/ county_name/ f"usa_va_{county_name}_adult_activity_location_assignment_week.csv")

    #Get the coordinates of all the residential locations
    home_coords = set(df_residence[['latitude', 'longitude']].apply(tuple, axis=1).tolist())

    #Shift lids for residential areas
    client_locations['coord'] = client_locations[['latitude', 'longitude']].apply(lambda x: (x.latitude, x.longitude), axis = 1)
    client_locations.loc[client_locations.coord.isin(home_coords), 'lid'] += HOME_SHIFT
    
    #Find popularity of locations, which locations are visited by which individuals
    assignments = client_locations.copy()
    assignments = assignments.groupby(['lid'])['pid'].apply(set).reset_index(name = 'pid')
    assignments = assignments.set_index('lid')
    assignments['activity'] = assignments['pid'].apply(lambda x: len(x))

    locations['activity'] = locations['lid'].apply(lambda x: assignments.at[x, 'activity'] if x in assignments.index else 0)
    locations['pid'] = locations['lid'].apply(lambda x: list(assignments.at[x, 'pid']) if x in assignments.index else [])
    locations = locations.sort_values(by = 'activity', ascending = False).reset_index(drop = True)

    client_locations = client_locations.groupby(['pid'])['lid'].apply(set).reset_index(name = 'lid')

    #Replace lid with the index of the lid in locations
    def filter(x):
        return_list = []
        for i in x:
        #Insert home locations at the front of the list
            if (i>HOME_SHIFT):
                return_list.insert(0, int(locations.loc[locations.lid==i].index[0]))
            else:
                return_list.append(int(locations.loc[locations.lid==i].index[0]))
        return return_list

    client_locations['lid'] = client_locations['lid'].apply(lambda x: filter(x))

    return locations.to_dict('index'), client_locations.to_dict('index')

def write_data_input(county_name: str = 'charlottesville_city'):
    with open(PROJECT_ROOT / 'data' / 'processed' / county_name / 'original.json', 'w') as f:
        LOCATIONS, CLIENT_LOCATIONS = create_data_input(county_name)
        data = {"LOCATIONS": LOCATIONS, "CLIENT_LOCATIONS": CLIENT_LOCATIONS}
        json.dump(data, f)

def read_data_input(county_name: str = 'charlottesville_city', filename: str = 'original.json'):
    
    directory_path = PROJECT_ROOT / 'data' / 'processed' / county_name / filename
    
    if not path.exists(directory_path):
        write_data_input(county_name)
    
    file = open(directory_path, 'r')
    data = json.load(file)

    LOCATIONS = []
    for key, val in data["LOCATIONS"].items():
        val["lid_ind"] = int(key)
        val["home"] = val["lid"] >= HOME_SHIFT
        LOCATIONS.append(val)
    
    CLIENT_LOCATIONS = {int(value['pid']): value['lid'] for ind, value in data["CLIENT_LOCATIONS"].items()}
    return LOCATIONS, CLIENT_LOCATIONS



############################################## AGGREGATION #################################################



def radius_cover(LOCATIONS, CLIENT_LOCATIONS, radius: float, county_name: str = 'charlottesville_city'):
    """
    Helper method for set_cover_aggregation
    
    Generates balls for the given radius
    Stores chosen locations and radius-coverage mappings in a json file
    """
    
    radius_dict = {}
    LOCATIONS_act = [(ind, value) for ind,value in enumerate(LOCATIONS) if not LOCATIONS[ind]['home']]
    
    for i in range(len(LOCATIONS_act)):
        loc1 = LOCATIONS_act[i][0]
        radius_dict[loc1] = []
        
        for j in range(len(LOCATIONS_act)):
            loc2 = LOCATIONS_act[j][0]
            
            coord_1 = (LOCATIONS[loc1]['latitude'], LOCATIONS[loc1]['longitude'])
            coord_2 = (LOCATIONS[loc2]['latitude'], LOCATIONS[loc2]['longitude'])
            
            dist = geopy.distance.great_circle(coord_1, coord_2).km
            
            if dist < radius:
                radius_dict[loc1].append(loc2)

    cover = set()
    chosen = set()
    
    while len(cover) < len(LOCATIONS_act):
        max_choice = (0, set(), -1)
        
        for key, val in radius_dict.items():
            if key not in chosen:
                set_choice = set(val)-cover
                if len(set_choice)>max_choice[0]:
                    max_choice = (len(set_choice), set_choice, key)
        
        cover = cover.union(max_choice[1])
        chosen.add(max_choice[2])
    
    with open(PROJECT_ROOT/ 'data'/ 'processed'/ county_name / f'radius_cover_{int(1000*radius)}.json', 'w') as f:
        data = {"radius": radius, "radius_dict": radius_dict, "chosen": list(chosen)}
        json.dump(data, f)

def set_cover_aggregation(LOCATIONS, CLIENT_LOCATIONS, county_name: str = 'charlottesville_city', radius: float = 0.01):
    """
    Must be run after original LOCATIONS and CLIENT_LOCATIONS are read in
    
    Creates balls of given radius around each location
    Picks location balls to cover all possible facility locations through set cover approximation
    Creates and returns a version of LOCATIONS and CLIENT_LOCATIONS that are based on the aggregate locs
    """
    
    file_radius = open(PROJECT_ROOT/ 'data'/ 'processed'/ county_name / f"radius_cover_{int(1000*radius)}.json", 'r')
    data_radius = json.load(file_radius)
    
    #Dict[int, List[int]] ==> {loc: locations covered by a ball of given radius centered at loc}
    cluster_dict = {int(ind): val for ind, val in data_radius["radius_dict"].items()}
    
    #Set[int] ==> contains the picked loc centers (for which the balls are constructed)
    chosen_points = set(data_radius["chosen"])
    
    #--------------------------------- LOCATIONS ------------------------------# 
    
    LOCATIONS_act = [(ind, value) for ind, value in enumerate(LOCATIONS) if not LOCATIONS[ind]['home']]
    
    #Reverse map the lids to the renumbered lid_ind
    reverse_lid_index = {}
    for i, loc in enumerate(LOCATIONS_act):
        reverse_lid_index[loc[0]] = i

    activity_locations = []
    for point in chosen_points:
        
        pid_set = set()
        member_list = cluster_dict[point]
        
        for loc in cluster_dict[point]:
            pid_set = pid_set.union(LOCATIONS[loc]['pid'])
        
        activity_locations.append({"lid_ind" : point,
                     "longitude" : LOCATIONS[point]['longitude'] ,
                     "latitude" : LOCATIONS[point]['latitude'],
                     "activity" : len(pid_set),
                     "pid" : list(pid_set),
                     "home" : False})
        
    residential_locations = []
    LOCATIONS_res = [(ind, value) for ind,value in enumerate(LOCATIONS) if LOCATIONS[ind]['home']]
    
    for loc in LOCATIONS_res:
        
        locations_dict = LOCATIONS[loc[0]]
        
        residential_locations.append({"lid_ind": locations_dict['lid_ind'],
                    "longitude" : locations_dict['longitude'],
                    "latitude" : locations_dict['latitude'],
                    "activity" : locations_dict['activity'],
                    "pid" : locations_dict["pid"],
                    "home" : True
                    })
    
    LOCATIONS_agg = sorted(activity_locations + residential_locations, key = lambda x: x["activity"], reverse = True)
    
    ind_to_reindex = {}
    for ind, loc in enumerate(LOCATIONS_agg):
        ind_to_reindex[loc['lid_ind']] = ind
        loc['lid_ind'] = ind
    
    #------------------------- CLIENT LOCATIONS --------------------------------#
    
    #Maps each location to chosen locations covering it
    coverage_matching = {}
    for cluster in cluster_dict.keys():
        overlap = [member for member in cluster_dict[cluster] if member in chosen_points]
        coverage_matching[cluster] = overlap
    
    CLIENT_LOCATIONS_agg = {}
    for key, val in CLIENT_LOCATIONS.items():
        new_lid_list = []
        for loc in val:
            
            if not LOCATIONS[loc]['home']:
                for elem in coverage_matching[loc]:
                    #Avoid repeats in client locations
                    if ind_to_reindex[elem] not in new_lid_list:
                        new_lid_list.append(ind_to_reindex[elem])
            else:
                new_lid_list.append(ind_to_reindex[loc])
        
        CLIENT_LOCATIONS_agg[key] = new_lid_list
    
    return LOCATIONS_agg, CLIENT_LOCATIONS_agg

#TODO: incorporate pipelines
def single_linkage_aggregation(LOCATIONS, CLIENT_LOCATIONS, county_name: str = 'charlottesville_city', radius: float = 0.02):
    """
    Must be run after LOCATIONS and CLIENT_LOCATIONS are read in
    Returns aggregated versions of LOCATIONS and CLIENT_LOCATIONS with single linkage clustering
    """
    
    file_dict = open(PROJECT_ROOT / 'data'/ 'processed' / county_name /"aid2pid.json", 'r')
    data_dict = json.load(file_dict)
    AID_LOC = data_dict['data']

    file_client = open(PROJECT_ROOT / 'data'/ 'processed' / county_name /"pid2aid.json", 'r')
    data_client = json.load(file_client)
    AID_CLIENT = {int(key):val for key,val in data_client['data'].items()}
    
    lid_to_ind = {}
    for ind, val in enumerate(LOCATIONS):
        lid_to_ind[val['lid']] = ind
    
    aid_to_ind = {}
    LOCATIONS_agg = []
    
    for i in range(len(AID_LOC)):
        
        new_dict = {"lid_ind" : i,
            "longitude" : AID_LOC[i]['longitude'],
            "latitude" : AID_LOC[i]['latitude'],
            "activity" : AID_LOC[i]["activity"],
            "pid" : AID_LOC[i]["visitors"],
            "home" : AID_LOC[i]['aid'] >= HOME_SHIFT
        }

        aid_to_ind[AID_LOC[i]['aid']] = i
        LOCATIONS_agg.append(new_dict)
    
    CLIENT_LOCATIONS_agg = {}
    for key in CLIENT_LOCATIONS.keys():
        new_list = []
        for loc in AID_CLIENT[key]:
            if loc >= HOME_SHIFT:
                new_list.insert(0, aid_to_ind[loc])
            else:
                new_list.append(loc)
        CLIENT_LOCATIONS_agg[key] = new_list
    
    return LOCATIONS_agg, CLIENT_LOCATIONS_agg

"""
1) Represent each aid by the loc closest to median
2) Represent each aid by the loc that's least far from all other locs
3) Represent each aid by most popular loc
"""

def single_linkage_aggregation2(LOCATIONS, CLIENT_LOCATIONS, county_name: str = 'charlottesville_city', radius: float = 0.02):
    """
    Must be run after LOCATIONS and CLIENT_LOCATIONS are read in
    Returns aggregated versions of LOCATIONS and CLIENT_LOCATIONS with single linkage clustering
    """
    
    file_dict = open(PROJECT_ROOT / 'data'/ 'processed'/county_name/"aid2pid.json", 'r')
    data_dict = json.load(file_dict)
    AID_LOC = data_dict['data']

    file_client = open(PROJECT_ROOT / 'data'/ 'processed'/county_name/ "pid2aid.json", 'r')
    data_client = json.load(file_client)
    AID_CLIENT = {int(key):val for key,val in data_client['data'].items()}
    
    lid_to_ind = {}
    for ind, loc in enumerate(LOCATIONS):
        lid_to_ind[loc['lid']] = ind
    
    aid_to_ind = {}
    LOCATIONS_agg = []

    for i in range(len(AID_LOC)):
        
        if AID_LOC[i]['aid'] >= HOME_SHIFT:
            new_dict = {"lid_ind" : i,
                        "longitude" : AID_LOC[i]['longitude'],
                        "latitude" : AID_LOC[i]['latitude'],
                        "activity" : AID_LOC[i]["activity"],
                        "pid" : AID_LOC[i]["visitors"],
                        "home" : True
            }
            
            aid_to_ind[AID_LOC[i]['aid']] = i
        
        else:
            
            members_list = [lid_to_ind[m] for m in AID_LOC[i]['members']]
            
            min_dist = (10**9, -1, -1)
            
            for m in members_list:
                
                distance_list = []
                
                for b in members_list:
                        
                        coord1_row = LOCATIONS[m]
                        coord2_row = LOCATIONS[b]
                        
                        coord1 = (coord1_row['latitude'], coord1_row['longitude'])
                        coord2 = (coord2_row['latitude'], coord2_row['longitude'])
                        
                        dist = geopy.distance.great_circle(coord1, coord2).km
                        
                        distance_list.append((dist, m, b))
                
                dispersion = max(distance_list)
                
                if dispersion[0]<min_dist[0]:
                    min_dist = dispersion
            
            new_dict = {"lid_ind" : i,
                        "longitude" : LOCATIONS[min_dist[1]]['longitude'],
                        "latitude" : LOCATIONS[min_dist[1]]['latitude'],
                        "activity" : AID_LOC[i]["activity"],
                        "pid" : AID_LOC[i]["visitors"],
                        "home" : False
            }
            
            aid_to_ind[AID_LOC[i]['aid']] = i
            
        LOCATIONS_agg.append(new_dict)
    
    CLIENT_LOCATIONS_agg = {}
    
    for key in CLIENT_LOCATIONS.keys():
        new_list = []
        for loc in AID_CLIENT[key]:
            
            if aid_to_ind[loc] >= len(AID_CLIENT):
                print(loc, aid_to_ind[loc])
            
            if loc >= HOME_SHIFT:
                new_list.insert(0, aid_to_ind[loc])
            else:
                new_list.append(aid_to_ind[loc])
        CLIENT_LOCATIONS_agg[key] = new_list
    
    return LOCATIONS_agg, CLIENT_LOCATIONS_agg

def aggregate_data(county_name: str = 'charlottesville_city', aggregation: int = 1, radius: float = 0.01):
    """
    Aggregation Options
        0 : default, no aggregation
        1 : set cover aggregation
        2 : single linkage aggregation
    """
    
    LOCATIONS, CLIENT_LOCATIONS = read_data_input(county_name)

    if aggregation == 2:
        
        return single_linkage_aggregation2(LOCATIONS, CLIENT_LOCATIONS, county_name, radius)
        
    elif aggregation == 1:
        aggregation_file = PROJECT_ROOT/'data'/'processed'/county_name/f"aggregation_{aggregation}_{int(1000*radius)}.json"
        
        ###Check if combination has already been attempted###
        if path.exists(aggregation_file):
            with open(aggregation_file, 'r') as f:
                data = json.load(f)
                LOCATIONS_1 = data['LOCATIONS']
                CLIENT_LOCATIONS_1 = {int(key):value for key,value in data['CLIENT_LOCATIONS'].items()}
                return LOCATIONS_1, CLIENT_LOCATIONS_1
        
        else:
            filename = PROJECT_ROOT / 'data'/ 'processed'/ county_name / f"radius_cover_{int(1000*radius)}.json"
            if not path.exists(filename):
                radius_cover(LOCATIONS, CLIENT_LOCATIONS, radius, county_name)
            LOCATIONS_1, CLIENT_LOCATIONS_1 = set_cover_aggregation(LOCATIONS, CLIENT_LOCATIONS, county_name, radius)
            
            ###Store output in json file###
            with open(aggregation_file, 'w') as f:
                json.dump({"LOCATIONS": LOCATIONS_1, "CLIENT_LOCATIONS": CLIENT_LOCATIONS_1}, f)
            
            return LOCATIONS_1, CLIENT_LOCATIONS_1
        
    else:
        
        for loc_dict in LOCATIONS:
            del loc_dict['lid']
        
        return LOCATIONS, CLIENT_LOCATIONS

################################# GLOBAL DATASTRUCTURES #############################################

LOCATIONS, CLIENT_LOCATIONS = aggregate_data(county_name = 'charlottesville_city', aggregation = 1, radius = 0.025)

#####################################################################################################