#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
export DATABASE=mysql
export SPRING_PROFILES_ACTIVE=${DATABASE}
export SPRING_DATASOURCE_URL="jdbc:mysql://127.0.0.1:3306/dolphinscheduler?useUnicode=true&characterEncoding=UTF-8"
export SPRING_DATASOURCE_USERNAME=root
export SPRING_DATASOURCE_PASSWORD=bYR-KFa-AEJ-Y9U2018
# JAVA_HOME, will use it to start DolphinScheduler server
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_281.jdk/Contents/Home
export PATH=$PATH:$JAVA_HOME/bin
export M2_HOME=/Users/lizu/app/apache-maven-3.6.3
export PATH=$PATH:$M2_HOME/bin
export SCALA_HOME=/Users/lizu/app/scala-2.11.12
#export SCALA_HOME=/Users/lizu/app/scala-2.12.13
export PATH=$PATH:$SCALA_HOME/bin
#export SPARK_HOME=/Users/lizu/app/spark-2.4.3-bin-hadoop2.7
export SPARK_HOME=/Users/lizu/app/spark-3.0.2-bin-hadoop2.7
export PATH=$PATH:$SPARK_HOME/bin
export FLINK_HOME=/Users/lizu/app/flink-1.13.6
export PATH=$PATH:$FLINK_HOME/bin
export PATH=/Library/Frameworks/Python.framework/Versions/3.6/bin:$PATH
export HADOOP_HOME=/Users/lizu/app/hadoop-2.7.6
export PATH=$PATH:$HADOOP_HOME/bin
#export HIVE_HOME=/Users/lizu/app/apache-hive-1.2.1-bin
export HIVE_HOME=/Users/lizu/app/hive-2.3.4
export PATH=$PATH:$HIVE_HOME/bin
#export NODE_HOME=/Users/lizu/app/node-v14.16.0
export NODE_HOME=/Users/lizu/app/node-v16.17.0
export PATH=$PATH:$NODE_HOME/bin
export PATH=$PATH:/Users/lizu/app/go/bin
export PATH=$PATH:/Users/lizu/app/gradle-6.3/bin
alias python="/usr/local/bin/python3"
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/pyspark:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH