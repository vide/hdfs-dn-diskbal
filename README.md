## HDFS DataNode local disks balance

`hdfs-dn-diskbal.sh` is a small tool to internally balance datanode's disks.
It can be useful when you have for instance replaced one or more disks in a 
datanode and you want to force blocks to be assigned to the new, empty disk.
By default HDFS [prior to 3.0](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HDFSCommands.html#balancer) consider that a
*Cluster is balanced if each datanode is balanced* (`datanode` policy). 
With 3.0 there is another `blockpool` policy which should balance disks (blockpools, BP)
within the same datanode, effectively balancing local disks.
This is what I'm trying to mimic in a poor-man-way.

**DISCLAIMER**: this script has only been tested on our HortonWorks HDP Hadoop cluster (2.3, 2.5)
and even if I tried to make as agnostic as possible, it can burn your servers to ashes,
grind your disks, make disturbing noises etc. You have been warned.

### Usage

**NOTE**: you should run this script as the `hdfs` user, or whatever unix user is the
owner of your HDFS data files.

* Stop the datanode (and possibly any other hadoop service running on the node, 
especially if they have shortcircuit enabled
* Run `hdfs-dn-diskbal.sh` (possibly behind a `screen` session) and wait. It will output
what's doing on stdout

### Parameters

* by default this script will look for HDFS config in `/etc/hadoop/conf/hdfs-site.xml`. You can 
specify where it should look for the XML configuration with the `--hdfs-config` switch
* Balancing will stop when the difference between the most and least used disks is below 5%
You can tune this behaviour with the `--threshold` switch

