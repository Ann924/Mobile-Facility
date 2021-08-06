from collections import namedtuple
import json
from typing import Dict, List, Tuple, Set
import pandas as pd

"""
Create global variables
"""

def create_data_input():
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
    locations = locations.sort_values(by = 'activity', ascending = False).reset_index(drop = True)

    client_locations = client_locations.groupby(['pid'])['lid'].apply(set).reset_index(name = 'lid')

    #Replace lid with the index of the lid in locations
    def filter(x):
        return_list = []
        for i in x:
        #Insert home locations at the front of the list
            if (i>HOME_SHIFT):
                return_list.insert(0, locations.loc[locations.lid==i].index[0])
            else:
                return_list.append(locations.loc[locations.lid==i].index[0])
        return return_list

    client_locations['lid'] = client_locations['lid'].apply(lambda x: filter(x))

    return locations.to_dict('index'), client_locations.to_dict('index')

def read_data_input():
    file = open("data.json", 'r')
    data = json.load(file)
    LOCATIONS = {int(ind): value for ind, value in data["LOCATIONS"].items()}
    return LOCATIONS, data["CLIENT_LOCATIONS"]

address = namedtuple('address', ['index', 'location', 'facility'])
HOME_SHIFT=1000000000
LOCATIONS, CLIENT_LOCATIONS = read_data_input()