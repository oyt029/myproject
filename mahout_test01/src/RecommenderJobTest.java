

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;

/**
 * ²âmahout org.apache.mahout.cf.taste.hadoop.item.RecommenderJob
 * environment£º 
 * mahout0.9
 * hadoop2.6
 * @author ouyangtao
 *
 */
public class RecommenderJobTest{
  static //RecommenderJob rec = null;

  Configuration conf = new Configuration();
 
//  public void setUp(){
//  //	rec= new RecommenderJob();
//	  Configuration conf = new Configuration();
//    System.out.println("Begin to test...");
//  }
  
 
  public static void main(String args[]) throws Exception{
//    String[] args1 ={
//     "-i","hdfs://192.168.1.169:8020/user/hdfs/item.csv",  
//         "-o","hdfs://192.168.1.169:8020/user/hdfs/rec001",  
//         "-n","3","-b","false","-s","SIMILARITY_EUCLIDEAN_DISTANCE",  
//         "--maxPrefsPerUser","7","--minPrefsPerUser","2",  
//         "--maxPrefsInItemSimilarity","7",  
//         "--outputPathForSimilarityMatrix","hdfs://192.168.1.169:8020/user/hdfs/matrix/rec001",
//         "--tempDir","hdfs://192.168.1.169:8020/user/hdfs/temp/rec001"}; 
    
	String localpath = "/home/hadoop/a.txt";  
	String filepath = "hdfs://192.168.1.169:8020/user/hdfs/";
	String filepath01 = "hdfs://192.168.1.169:8020/user/hdfs/a.txt";
    String filename = "item.csv";
    String outFile = filepath+"rec001/part-r-00000";
    
	// ToolRunner.run(conf, new RecommenderJob(), args1);
    //datain(localpath,filepath01);
    //recommender(filepath,filename);
    //dataout(outFile);
	//cleanUp(filepath);
  }
  
  
  public static void recommender(String filepath,String filename) throws Exception
  {
	  String[] args1 ={
			     "-i",filepath+filename,  
			         "-o",filepath+"rec001",  
			         "-n","3","-b","false","-s","SIMILARITY_EUCLIDEAN_DISTANCE",  
			         "--maxPrefsPerUser","7","--minPrefsPerUser","2",  
			         "--maxPrefsInItemSimilarity","7",  
			         "--outputPathForSimilarityMatrix",filepath+"matrix/rec001",
			         "--tempDir",filepath+"temp/rec001"}; 
			     ToolRunner.run(conf, new RecommenderJob(), args1);
			
	  
  }
  
  public static void datain(String LocalFilePath,String HdfsFilePath) throws FileNotFoundException, IOException{
	  
	  	hdfsfile hdfs01 = new hdfsfile();
		hdfs01.uploadToHdfs(LocalFilePath, HdfsFilePath);;
	  
  }
  public static void dataout(String filepath) throws IOException{
	    
	    hdfsfile hdfs01 = new hdfsfile();
		hdfs01.cat(filepath);
  }
 
  public static void cleanUp(String path) throws IOException{
	  hdfsfile hdfs02 = new hdfsfile();
	  
	  String outFile1,outFile2,outFile3;
	  outFile1 = path+"temp/";
	  outFile2 = path+"matrix";
	  outFile3 = path+"rec001";
	  hdfs02.rmr(outFile1);
	  hdfs02.rmr(outFile2);
	  hdfs02.rmr(outFile3);
	   System.out.println("clean over...");
	  
    
  }
}
