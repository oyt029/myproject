

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
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
  
	public void cat(String outFile) throws IOException {
		// TODO Auto-generated method stub
		Path path = new Path(outFile);
        FileSystem fs = FileSystem.get(URI.create(DEFAULT_FS), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + outFile);
        try {  
            fsdis =fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);  
          } finally {  
            IOUtils.closeStream(fsdis);
            fs.close();
          }
		
	}
}
