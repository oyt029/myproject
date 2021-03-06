package fz.hadoop2.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class Hadoop2Util {
 private static Configuration conf=null;
 
 private static final String YARN_RESOURCE="192.168.1.159:8032";
 private static final String DEFAULT_FS="hdfs://192.168.1.159:8020";
 
 public static Configuration getConf(){
  if(conf==null){
   conf = new YarnConfiguration();
   conf.set("fs.defaultFS", DEFAULT_FS);
   conf.set("mapreduce.framework.name", "yarn");
   conf.set("yarn.resourcemanager.address", YARN_RESOURCE);
  }
  return conf;
 }
}