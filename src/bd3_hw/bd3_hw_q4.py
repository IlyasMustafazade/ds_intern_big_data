# In HDFS system, there is namenode, datanodes, job tracker, and task tracker.
# Namenode acts like a master and stores information about datanodes.
# For example, placement of datanodes, number of replicas, and placement of replicas.
# Datanodes send "heartbeat" signals to namenode frequently so that namenode is aware of dead nodes.
# If a datanode does not send any signals for some time, namenode considers it as dead node.
# Secondary namenodes store copy of namenode so that nothing is lost when main namenode is lost.
# Datanodes store actual data that consist of blocks. 
# Typically, blocks are 128 MB. 
# Smaller blocks would not be efficient to store large files.
# Job tracker receives information from namenode to know places of blocks that will be processed.
# Task tracker is responsible for actually applying processing on blocks whose information is taken from job tracker.
# By default, HDFS stores 3 replicas of each block across different nodes.
# Purpose of this is to avoid data loss when machine fails.

# HDFS splits large files to blocks and splits them among nodes (machines) in cluster.
# It does this efficiently: Not moving large files too much.
