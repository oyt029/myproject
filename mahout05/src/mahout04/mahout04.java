package mahout04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.pseudo.RecommenderJob;



@SuppressWarnings("deprecation")
public class mahout04 {

	
	public static void main(String[] arg) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = new Configuration();
        String[] args = { "-i", "/user/hdfs/input/item.csv", "-o",
                "/user/hdfs/", "-n", "3", "-b", "false",
                "-s", "SIMILARITY_EUCLIDEAN_DISTANCE", "--maxPrefsPerUser",
                "7", "--minPrefsPerUser", "2", "--maxPrefsInItemSimilarity",
                "7", "--outputPathForSimilarityMatrix",
                "/mahout/similarity", "--tempDir",
                "/mahout/tmpdir" };
        ToolRunner.run(conf, new RecommenderJob(), args);
    }

}
