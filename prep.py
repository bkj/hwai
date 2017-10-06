#!/usr/bin/env python

"""
    prep.py
    
    1) Pull out (posting user, tagged user, n times tagged) tag graph
    2) Pull out message locations
    3) Compute user location as spatial median of message locations
"""

import os
import sys
import ujson as json
from pyspark import SparkContext

# --
# Helpers

# Edge helpers
def get_tags(x):
    """
        given message, yield edges connecting user that posted the message and 
        each user tagged in the message (excluding self edges)
    """
    poster = x['_source']['user']['id']
    for user in x['_source']['users_in_photo']:
        if user['user']['id'] != poster:
            yield (poster, user['user']['id'])

def format_wedge(x):
    return '\t'.join(map(str, (
        x[0][0],
        x[0][1],
        x[1]
    )))


# Location helpers
def get_message_location(message):
    try:
        return {
            "user_id"    : message['_source']['user']['id'],
            "latitude"   : message['_source']['location']['latitude'],
            "longitude"  : message['_source']['location']['longitude'],
        }
    except:
        return None

def format_message_location(message_location):
    return '\t'.join(map(str, (
        message_location['user_id'],
        message_location['latitude'],
        message_location['longitude']
    )))

def format_user_location(obj):
    user_id, user_location = obj
    return '\t'.join(map(str, (
        user_id,
        user_location['n_obs'],
        user_location['latitude'],
        user_location['longitude'],
        user_location['median_abs_deviation'],
    )))


# --
# Run

if __name__ == "__main__":
    
    # --
    # Configuration
    
    config = json.load(open(sys.argv[1]))
    sc = SparkContext(appName=config['app_name'])
    sc.addPyFile('./spatial_helpers.py')
    from spatial_helpers import spatial_center
    
    
    # --
    # Load raw data
    
    messages = sc.textFile(config['inpath']).map(json.loads)
    
    
    # --
    # Pull out graph
    
    # Pull out `(posting user, tagged user)` edges
    edges = messages.flatMap(get_tags)
    
    # Count edges to create weighted graph of `(posting user, tagged user, n times tagged)` weighted edges
    wedges = edges.map(lambda x: (x, 1)).reduceByKey(lambda a,b: a + b)
    
    # Write to disk
    wedges.map(format_wedge).saveAsTextFile(config['graph'])
    
    
    # --
    # Pull out message locations
    
    message_locations = messages.map(get_message_location).filter(lambda x: x is not None)
    
    
    # --
    # Compute spatial median of a user's posts
    
    locations_by_user = message_locations\
        .map(lambda x: (x['user_id'], (x['latitude'], x['longitude'])))\
        .groupByKey()
        
    user_locations = locations_by_user.mapValues(spatial_center)
    
    user_locations.map(format_user_location).saveAsTextFile(config['user_locations'])
