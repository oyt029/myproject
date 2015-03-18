

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

/**
 * ²âmahout org.apache.mahout.cf.taste.hadoop.item.RecommenderJob
 * environment£º 
 * mahout0.9
 * hadoop2.2
 * @author fansy
 *
 */
public class RecommenderJobTest{
  static //RecommenderJob rec = null;
  Configuration conf =null;
 
  public void setUp(){
  //	rec= new RecommenderJob();
    conf= Hadoop2Util.getConf();
    System.out.println("Begin to test...");
  }
  
 
  public static void main(String args[]) throws Exception{
    String[] args1 ={
     "-i","hdfs://192.168.1.169:8020/user/hdfs/item.csv",  
         "-o","hdfs://192.168.1.169:8020/user/hdfs/rec001",  
         "-n","3","-b","false","-s","SIMILARITY_EUCLIDEAN_DISTANCE",  
         "--maxPrefsPerUser","7","--minPrefsPerUser","2",  
         "--maxPrefsInItemSimilarity","7",  
         "--outputPathForSimilarityMatrix","hdfs://192.168.1.169:8020/user/hdfs/matrix/rec001",
         "--tempDir","hdfs://192.168.1.169:8020/user/hdfs/temp/rec001"}; 
    ToolRunner.run(conf, new RecommenderJob(), args1);
    
    Hadoop2Util hdfs01 = new Hadoop2Util();
    String outFile;
    outFile = "hdfs://192.168.1.169:8020/user/hdfs/temp/rec001";
	hdfs01.cat(outFile);
  }
  
 
  public void cleanUp(){
    
  }
}
