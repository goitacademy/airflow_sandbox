#!/bin/bash
spark-submit \
  --jars mysql-connector-j-8.0.32.jar \
  olympic_pipeline_job.py