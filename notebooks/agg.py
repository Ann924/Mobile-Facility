#%%

# %load_ext autoreload
# %autoreload 2

import random
import pandas as pd
from typing import Dict, List, Tuple, Set
from src.problem import *
from src.round import *
from src.heuristics import *
from src.utils import *
from src.config import *
import folium
import numpy as np
from folium.plugins import FastMarkerCluster, MarkerCluster

#%% 
# Data loading and preprocessing
RAW_DATA_DIR = PROJECT_ROOT / 'data' / 'raw'

adult_df = pd.read_csv(RAW_DATA_DIR / "usa_va_charlottesville_city_adult_activity_location_assignment_week.csv")
act_df = pd.read_csv(RAW_DATA_DIR / "usa_va_charlottesville_city_activity_locations.csv")
res_df = pd.read_csv(RAW_DATA_DIR / "usa_va_charlottesville_city_residence_locations.csv")

# Define lid
act_df['lid'] = act_df['alid']
res_df['lid'] = res_df['rlid'] + HOME_SHIFT

loc_df = pd.concat([act_df[['lid', 'longitude', 'latitude']], res_df[['lid', 'longitude', 'latitude']]]).set_index('lid')
loc_mat = loc_df[['longitude', 'latitude']].to_numpy()

#%% 
# Generate single-linkage clusters (aid' -> lid)
import matplotlib.pyplot as plt
import numpy as np
from sklearn.neighbors import BallTree
from networkx.utils.union_find import UnionFind

THRESHOLD = .025 # distance in km
EARTH_RADIUS = 6371 # kilometers
THRESHOLD_R = THRESHOLD / EARTH_RADIUS

tree = BallTree(loc_mat, leaf_size=40, metric='haversine')
# Threshold all values less than radius r
groups = tree.query_radius(loc_mat, r=THRESHOLD_R)
valid_groups = [group for group in groups if len(group) > 1]

uf = UnionFind(range(len(loc_df)))
for group in groups:
    uf.union(*group)

agg_groups = list(uf.to_sets())

#%%
# Generate tentative aid groups (aid' -> lid)
lid2pid = adult_df.groupby("lid")[["pid"]].agg(set).pid.apply(list)

aidPidGroups = []
for group in agg_groups:
    aidPidGroups.append(set().union(*[lid2pid[l] for l in group]))