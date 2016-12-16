#!/usr/bin/env bash

cd target
spark-submit --class ClusterSOData.Main --master local KMeans-0.0.1.jar
