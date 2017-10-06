#!/usr/bin/env python

"""
    spatial_helpers.py
"""

import numpy as np
from math import radians, degrees, cos, sin, asin, sqrt, atan2

# --
# Helpers

def _compute_midpoint(point1, point2):
    """
        compute the midpoint of two points on a sphere
    """
    latitude1, longitude1 = map(radians, point1)
    latitude2, longitude2 = map(radians, point2)
    
    bx = cos(latitude2) * cos(longitude2 - longitude1)
    by = cos(latitude2) * sin(longitude2 - longitude1)
    
    latitude3 = atan2(sin(latitude1) + sin(latitude2), \
           sqrt((cos(latitude1) + bx) * (cos(latitude1) \
           + bx) + by**2))
    longitude3 = longitude1 + atan2(by, cos(latitude1) + bx)
    
    return map(degrees, [latitude3, longitude3])


def haversine_distance(point1, point2, radius=6371):
    """
        (approximate) distance between two points on earth's surface (in KM)
    """
    latitude1, longitude1 = map(radians, point1)
    latitude2, longitude2 = map(radians, point2)
    
    dlongitude = longitude2 - longitude1 
    dlatitude = latitude2 - latitude1 
    a    = sin(dlatitude/2)**2 + cos(latitude1) * cos(latitude2) * sin(dlongitude/2)**2
    c    = 2 * asin(sqrt(a)) 
    km   = radius * c
    return km


def _spatial_median(X, f, eps=1e-3, max_iter=1000):
    """
        compute spatial median of a set of > 2 points
        
        That is, given a set X find the point m s.t.
            
            sum([dist(x, m) for x in X])
        
        is minimized. This is a robust estimator of the mode of the set.
    """
    iter_ = 0
    y = np.mean(X, 0)
    while True:
        iter_ += 1
        
        D         = np.array(map(lambda x: f(x, y), X))[:,np.newaxis]
        nonzeros  = (D != 0)[:, 0]
        Dinv      = 1 / D[nonzeros]
        Dinvs     = np.sum(Dinv)
        W         = Dinv / Dinvs
        T         = np.sum(W * X[nonzeros], 0)
        num_zeros = len(X) - np.sum(nonzeros)
        
        if num_zeros == 0:
            y1 = T
        elif num_zeros == len(X):
            out = y
            break
        else:
            R    = (T - y) * Dinvs
            r    = np.linalg.norm(R)
            rinv = 0 if r == 0 else num_zeros/r
            y1   = max(0, 1-rinv)*T + min(1, rinv)*y
        
        if (np.linalg.norm(y - y1) < eps) or (iter_ > max_iter):
            out = y1
            break
                
        y = y1
        
    return {
        "n_obs"                : len(X),
        "median_abs_deviation" : np.median(D),
        "latitude"             : out[0],
        "longitude"            : out[1],
    }


def spatial_center(x):
    """
        Compute "center" of a set of points
        
        If only one point, "center" is point itself
        If two points, "center" is the midpoint on the line connecting the points
        If > two points, "center" is the spatial median of the set (see _spatial_median)
    """
    x = list(x)
    if len(x) == 1:
        only_point = x[0]
        return {
            "n_obs"                 : 1,
            "median_abs_deviation"  : 0,
            "latitude"              : only_point[0],
            "longitude"             : only_point[1],
            
        }
    elif len(x) == 2:
        midpoint = _compute_midpoint(*x)
        return {
            "n_obs"                : 2,
            "median_abs_deviation" : haversine_distance(midpoint, x[0]),
            "latitude"             : midpoint[0],
            "longitude"            : midpoint[1],
        }
    else:
        return _spatial_median(np.array(x), haversine_distance)
