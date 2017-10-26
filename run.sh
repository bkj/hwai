#!/bin/bash

# run.sh

# --
# Helpers

# Can change parameters according to cluster/dataset size
SPARK_SUBMIT="spark-submit --num-executors 64 --executor-memory 2g  --driver-memory 2g"

# --
# Configuration

INPATH="/user/bjohnson/data/instagram/trickle/messages"
OUTPATH="/user/bjohnson/projects/wai/instagram-v3/trickle/output/"
./generate-config.py --inpath $INPATH --outpath $OUTPATH > config.json

# --
# Prepare data
# 
# eg, go from raw data to:
#   1) graph of social network
#   2) labels for the nodes (eg, predicted location of user)
# 
# !! If you don't have raw JSON messages, you can skip this step

# $SPARK_SUBMIT ./prep.py config.json

# --
# Run "spatial label propagation"
# 
# Uses output of previous step to predict user's location from 
# the locations of their neighbors in the graph

$SPARK_SUBMIT ./predict.py config.json


# --
# Inspect results
#
# Merge predictions back w/ actual user locations, create plot showing distribution of error

# $SPARK_SUBMIT inspect-results.py config.json
