from collections import namedtuple
from os import path
import json
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
    "lid" : The index of the lid (from LOCATIONS) of the center of the aggregated location
    "lid_ind" : The index of each location in LOCATIONS
    "longitude" : Longitude of the center
    "latitude" : Latitude of the center
    "members" : The lid indices that belong to this cluster
    "activity" : The number of clients that visit this aggregated cluster of locations
    "pid" : The list of clients that visit this aggregated cluster of locations

CLIENT_LOCATIONS_agg : Dict mapping pids of clients to the aggregated locations
---------

"""
address = namedtuple('address', ['index', 'location', 'facility'])
HOME_SHIFT=1000000000

def create_data_input():
    """
    LOCATIONS contains the pid of clients that visit the location
    """
    #Read in both the activity and residence locations
    df_activity = pd.read_csv("usa_va_charlottesville_city_activity_locations.csv").rename({"alid": "lid"}, axis = 'columns')
    df_residence = pd.read_csv("usa_va_charlottesville_city_residence_locations.csv")

    #Shift the residence lid
    df_residence['lid'] = df_residence['rlid'] + HOME_SHIFT
    locations = pd.concat([df_activity[['lid', 'longitude', 'latitude']], df_residence[['lid', 'longitude', 'latitude']]]).reset_index(drop = True)

    #Read in the client visited locations data (adults only for now)
    client_locations = pd.read_csv("usa_va_charlottesville_city_adult_activity_location_assignment_week.csv")

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

def write_data_input():
    with open('original.json', 'w') as f:
        LOCATIONS, CLIENT_LOCATIONS = create_data_input()
        data = {"LOCATIONS": LOCATIONS, "CLIENT_LOCATIONS": CLIENT_LOCATIONS}
        json.dump(data, f)

def read_data_input(filename = 'original.json'):
    file = open(filename, 'r')
    data = json.load(file)

    LOCATIONS = []
    for key, val in data["LOCATIONS"].items():
        val["lid_ind"] = int(key)
        LOCATIONS.append(val)
    
    CLIENT_LOCATIONS = {int(value['pid']): value['lid'] for ind, value in data["CLIENT_LOCATIONS"].items()}
    return LOCATIONS, CLIENT_LOCATIONS

######################################################

LOCATIONS, CLIENT_LOCATIONS = read_data_input()

######################################################

LOCATIONS, CLIENT_LOCATIONS = [], {}


def radius_cover(radius: float):
    """
    Helper method for set_cover_aggregation
    
    Generates balls for the given radius
    Stores chosen locations and radius-coverage mappings in a json file
    """
    
    radius_dict = {}
    LOCATIONS_act = [(ind, value) for ind,value in enumerate(LOCATIONS) if LOCATIONS[ind]['lid']<HOME_SHIFT]
    
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
    
    with open(f'radius_cover_{1000*radius}.json', 'w') as f:
        data = {"radius": radius, "radius_dict": radius_dict, "chosen": list(chosen)}
        json.dump(data, f)

def set_cover_aggregation(radius: float = 0.01):
    """
    Must be run after original LOCATIONS and CLIENT_LOCATIONS are read in
    
    Creates balls of given radius around each location
    Picks location balls to cover all possible facility locations through set cover approximation
    Creates and returns a version of LOCATIONS and CLIENT_LOCATIONS that are based on the aggregate locs
    """
    
    file_radius = open(f"radius_cover_{radius}.json", 'r')
    data_radius = json.load(file_radius)
    cluster_dict = {int(ind): val for ind, val in data_radius["radius_dict"].items()}
    chosen_points = set(data_radius["chosen"])
    
    LOCATIONS_act = [(ind, value) for ind,value in enumerate(LOCATIONS) if LOCATIONS[ind]['lid']<HOME_SHIFT]
    
    reverse_lid_index = {}
    for i, loc in enumerate(LOCATIONS_act):
        reverse_lid_index[loc[0]] = i
    
    temp = []
    cluster_dict_single = {}
    
    #To avoid repeat coverage, assign aggregation centers (from radius_dict) to the closest location in their set
    min_matching = {}
    for cluster in cluster_dict.keys():
        
        overlap = [member for member in cluster_dict[cluster] if member in chosen_points]
        min_matching[cluster] = overlap
    
    for point in chosen_points:
        
        pid_set = set()
        member_list = cluster_dict[point]
        
        for loc in cluster_dict[point]:
            pid_set = pid_set.union(LOCATIONS[loc]['pid'])
        
        temp.append({"lid_ind": point,
                     "lid": LOCATIONS[point]['lid'],
                     "longitude": LOCATIONS[point]['longitude'] ,
                     "latitude": LOCATIONS[point]['latitude'],
                     "members": member_list,
                     "activity": len(pid_set),
                     "pid": list(pid_set)})
    
    temp_res = []
    LOCATIONS_res = [(ind, value) for ind,value in enumerate(LOCATIONS) if LOCATIONS[ind]['lid'] > HOME_SHIFT]
    
    for loc in LOCATIONS_res:
        
        locations_dict = LOCATIONS[loc[0]]
        
        new_dict = {}
        new_dict["lid_ind"] = loc[0]
        new_dict["lid"] = locations_dict['lid']
        new_dict["longitude"] = locations_dict['longitude']
        new_dict["latitude"] = locations_dict['latitude']
        new_dict["members"] = [loc[0]]
        new_dict["activity"] = locations_dict['activity']
        new_dict["pid"] = locations_dict["pid"]
        temp_res.append(new_dict)
    
    temp = sorted(temp+temp_res, key = lambda x: x["activity"], reverse = True)
    
    #-----Now for clients----#
    
    temp_client = {}
    for key, val in CLIENT_LOCATIONS.items():
        new_lid_list = []
        for loc in val:
            if loc not in new_lid_list and LOCATIONS[loc]['lid'] < HOME_SHIFT:
                for elem in min_matching[loc]:
                    new_lid_list.append(elem)
            else:
                new_lid_list.append(loc)
        
        temp_client[key] = new_lid_list
    
    return temp, temp_client

#TODO: incorporate pipelines
def single_linkage_aggregation(radius: float = 0.02):
    """
    Must be run after LOCATIONS and CLIENT_LOCATIONS are read in
    
    
    Returns aggregated versions of LOCATIONS and CLIENT_LOCATIONS with single linkage clustering
    """
    
    file_dict = open("aid2pid.json", 'r')
    data_dict = json.load(file_dict)
    AID_LOC = data_dict['data']

    file_client = open("pid2aid.json", 'r')
    data_client = json.load(file_client)
    AID_CLIENT = {int(key):val for key,val in data_client['data'].items()}
    
    lid_to_ind = {}
    for ind, val in enumerate(LOCATIONS):
        lid_to_ind[val['lid']] = ind
    
    ind_to_aid = {}
    LOCATIONS_agg = []
    original_loc_length = len(LOCATIONS)
    
    count = 0
    for i in range(len(AID_LOC)):
        
        if AID_LOC[i]['aid'] >= HOME_SHIFT:
            new_dict = {}
            new_dict["lid_ind"] = lid_to_ind[AID_LOC[i]['aid']]
            new_dict["lid"] = AID_LOC[i]['aid']
            new_dict["longitude"] = AID_LOC[i]['longitude']
            new_dict["latitude"] = AID_LOC[i]['latitude']
            new_dict["members"] = [lid_to_ind[AID_LOC[i]['aid']]]
            new_dict["activity"] = AID_LOC[i]["activity"]
            new_dict["pid"] = AID_LOC[i]["visitors"]
            
            ind_to_aid[lid_to_ind[AID_LOC[i]['aid']]] = i
        
        else:
            #lids of new aggregated locations are set to be negative
            new_dict = {}
            new_dict["lid_ind"] = original_loc_length + count
            new_dict["lid"] = -1 * (count+1)
            new_dict["longitude"] = AID_LOC[i]['longitude']
            new_dict["latitude"] = AID_LOC[i]['latitude']
            new_dict["members"] = [lid_to_ind[m] for m in AID_LOC[i]['members']]
            new_dict["activity"] = AID_LOC[i]["activity"]
            new_dict["pid"] = AID_LOC[i]["visitors"]
            
            ind_to_aid[original_loc_length + count] = i
            
            count+=1
            
            LOCATIONS.append(new_dict)
        
        LOCATIONS_agg.append(new_dict)
    
    CLIENT_LOCATIONS_agg = {}
    for key in CLIENT_LOCATIONS.keys():
        new_list = []
        for loc in AID_CLIENT[key]:
            if loc >= HOME_SHIFT:
                new_list.insert(0, lid_to_ind[loc])
            else:
                new_list.append(LOCATIONS_agg[loc]['lid_ind'])
        CLIENT_LOCATIONS_agg[key] = new_list
    
    return LOCATIONS_agg, CLIENT_LOCATIONS_agg

"""
1) Represent each aid by the loc closest to median
2) Represent each aid by the loc that's least far from all other locs
3) Represent each aid by most popular loc
"""

def single_linkage_aggregation2(radius: float = 0.02):
    """
    Must be run after LOCATIONS and CLIENT_LOCATIONS are read in
    Returns aggregated versions of LOCATIONS and CLIENT_LOCATIONS with single linkage clustering
    """
    
    file_dict = open("aid2pid.json", 'r')
    data_dict = json.load(file_dict)
    AID_LOC = data_dict['data']

    file_client = open("pid2aid.json", 'r')
    data_client = json.load(file_client)
    AID_CLIENT = {int(key):val for key,val in data_client['data'].items()}
    
    lid_to_ind = {}
    for ind, val in enumerate(LOCATIONS):
        lid_to_ind[val['lid']] = ind
    
    ind_to_aid = {}
    LOCATIONS_agg = []
    original_loc_length = len(LOCATIONS)
    
    count = 0
    for i in range(len(AID_LOC)):
        
        if AID_LOC[i]['aid'] >= HOME_SHIFT:
            new_dict = {}
            new_dict["lid_ind"] = lid_to_ind[AID_LOC[i]['aid']]
            new_dict["lid"] = AID_LOC[i]['aid']
            new_dict["longitude"] = AID_LOC[i]['longitude']
            new_dict["latitude"] = AID_LOC[i]['latitude']
            new_dict["members"] = [lid_to_ind[AID_LOC[i]['aid']]]
            new_dict["activity"] = AID_LOC[i]["activity"]
            new_dict["pid"] = AID_LOC[i]["visitors"]
            
            ind_to_aid[lid_to_ind[AID_LOC[i]['aid']]] = i
        
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
            
            
            #lids of new aggregated locations are set to be negative
            new_dict = {}
            new_dict["lid_ind"] = min_dist[1]
            new_dict["lid"] = LOCATIONS[min_dist[1]]['lid']
            new_dict["longitude"] = AID_LOC[i]['longitude']
            new_dict["latitude"] = AID_LOC[i]['latitude']
            new_dict["members"] = [lid_to_ind[m] for m in AID_LOC[i]['members']]
            new_dict["activity"] = AID_LOC[i]["activity"]
            new_dict["pid"] = AID_LOC[i]["visitors"]
            
            ind_to_aid[new_dict["lid_ind"]] = i
            
            count+=1
            
        LOCATIONS_agg.append(new_dict)
    
    CLIENT_LOCATIONS_agg = {}
    for key in CLIENT_LOCATIONS.keys():
        new_list = []
        for loc in AID_CLIENT[key]:
            if loc >= HOME_SHIFT:
                new_list.insert(0, lid_to_ind[loc])
            else:
                new_list.append(LOCATIONS_agg[loc]['lid_ind'])
        CLIENT_LOCATIONS_agg[key] = new_list
    
    return LOCATIONS_agg, CLIENT_LOCATIONS_agg

def aggregate_data(aggregation: int = 1, radius: float = 0.01):
    """
    Aggregation Options
        0 : default, no aggregation
        1 : set cover aggregation
        2 : single linkage aggregation
    """
    
    if aggregation == 2:
        return single_linkage_aggregation2()
        
    elif aggregation == 1:
        
        filename = f"radius_cover_{radius}.json"
        if not path.exists(filename):
            radius_cover(radius)
        return set_cover_aggregation()
        
    else:
        
        return LOCATIONS, CLIENT_LOCATIONS
