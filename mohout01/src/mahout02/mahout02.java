package mahout02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.als.RecommenderJob;

import fz.hadoop2.util.Hadoop2Util;
/**
 * 测试mahout org.apache.mahout.cf.taste.hadoop.item.RecommenderJob
 * environment：
 * mahout0.9
 * hadoop2.2
 * @author fansy
 *
 */
public class mahout02{
 static //RecommenderJob rec = null;
 Configuration conf =null;

 public void setUp(){
 // rec= new RecommenderJob();
  conf= Hadoop2Util.getConf();
  System.out.println("Begin to test...");
 }
 public static void main( String[] args ) throws Exception{
//  String[] args1 ={
//   "-i","hdfs://master2:8020/user/hdfs/rating/ratings.csv", 
//        "-o","hdfs://master:8020/user/hdfs/", 
//        "-n","3","-b","false","-s","SIMILARITY_EUCLIDEAN_DISTANCE", 
//        "--maxPrefsPerUser","7","--minPrefsPerUser","2", 
//        "--maxPrefsInItemSimilarity","7", 
//        "--outputPathForSimilarityMatrix","hdfs://master2:8020/user/hdfs/dataset",
//        "--tempDir","hdfs://master2:8020/temp/rec001" };
//  ToolRunner.run(conf, new RecommenderJob(), args1);
// }
 
 
 String[] args1 = { "-i", "hdfs://master2:8020/user/hdfs/rating/ratings.csv", "-o",
         "hdfs://master2:8020/user/hdfs/dataset"," --similarityClassname org.apache.mahout.math.hadoop.similarity.cooccurrence.measures.EuclideanDistanceSimilarity","--tempDir",
         "hdfs://master2:8020/user/hdfs/" };
 ToolRunner.run(conf, new RecommenderJob(), args1);
 }
}