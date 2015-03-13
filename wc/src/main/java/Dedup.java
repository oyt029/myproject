 
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class Dedup {
 
    //map�������е�value���Ƶ������ݵ�key�ϣ���ֱ�����
    public static class Map extends Mapper<Object,Text,Text,Text>{
        private static Text line=new Text();//ÿ�����
       
        //ʵ��map����
        public void map(Object key,Text value,Context context)
                throws IOException,InterruptedException{
            line=value;
            context.write(line, new Text(""));
        }
       
    }
   
    //reduce�������е�key���Ƶ������ݵ�key�ϣ���ֱ�����
    public static class Reduce extends Reducer<Text,Text,Text,Text>{
        //ʵ��reduce����
        public void reduce(Text key,Iterable<Text> values,Context context)
                throws IOException,InterruptedException{
            context.write(key, new Text(""));
        }
       
    }
   
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        //��仰�ܹؼ�
       // conf.set("mapred.job.tracker", "192.168.1.2:9001");
       
        //String[] ioArgs=new String[]{"dedup_in","dedup_out"};
     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
     if (otherArgs.length != 2) {
     System.err.println("Usage: Data Deduplication <in> <out>");
     System.exit(2);
     }
     
     Job job = new Job(conf, "Data Deduplication");
     job.setJarByClass(Dedup.class);
     
     //����Map��Combine��Reduce������
     job.setMapperClass(Map.class);
     job.setCombinerClass(Reduce.class);
     job.setReducerClass(Reduce.class);
     
     //�����������
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
     
     //������������Ŀ¼
     FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
     FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
     System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}



