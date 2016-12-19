#!/usr/bin/env bash

cd target
spark-submit --packages com.databricks:spark-xml_2.11:0.4.0 --class ClusterSOData.Main --master local KMeans-0.0.1.jar
