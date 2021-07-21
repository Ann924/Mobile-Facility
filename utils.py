from typing import Dict, List, Tuple, Set

def cost(G: List[List[float]], loc1, loc2):
    if loc1==loc2: return 0
    elif loc1 < loc2:
        return G[loc2][loc1]
    else:
        return G[loc1][loc2]