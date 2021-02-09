#!/usr/bin/env bash
set -e
SPARK_VERSION='3.0.1'
HADOOP_VERSION='2.7'

curl http://apache.mirror.gtcomm.net/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz --output /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz
cd /tmp && tar xzf /tmp/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz