package hdfsdemo;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HarFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.junit.Test;

import com.google.protobuf.ServiceException;

import org.htrace.Trace;
@SuppressWarnings("all")
public class hdfsdemo {
	private static final String HDFS_PATH = "hdfs://192.168.1.96:8020";
	private Configuration conf;
	public static boolean stringToHdfs(String content, String filePath) {
		// log.info(filePath);
		boolean flag = true;
		InputStream in = null;
		try {
			in = new ByteArrayInputStream(content.getBytes("utf-8"));
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(filePath), conf);
			FSDataOutputStream out = fs.create(new Path(filePath));
			IOUtils.copyBytes(in, out, 4096, true);

		} catch (IOException e) {
			// log.error(e);
			flag = false;
			return flag;
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					// log.error(e);
				}
			}
		}
		return flag;

	}

	/**
	 * ÉÏ´«ÎÄ¼þµ½HDFSÉÏÈ¥
	 */
	@Test
	private static void uploadToHdfs(String localSrc,String dst) throws FileNotFoundException,
			IOException {

		// String localSrc = "C:\\Users\\Administrator\\Desktop\\SpringMvc.pdf";
		// String dst = "hdfs://10.83.3.132:8020/SpringMvc.pdf";
		//C:\Users\yangtao.ou\Desktop
		

		InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataOutputStream out = fs.create(new Path(dst));
		IOUtils.copyBytes(in, out, 4096, true);

		System.out.println("-------------------uploadToHdfs end------------------------");
	}

	/**
	 * ´ÓHDFSÉÏ¶ÁÈ¡ÎÄ¼þ
	 */
	private static void readFromHdfs(String dst,String local) throws FileNotFoundException,
			IOException {
		// String dst = "hdfs://10.83.3.132:8020/SpringMvc.pdf";
		//String dst = "hdfs://192.168.1.96:8020/tmp/QQ-test.jpg";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));

		OutputStream out = new FileOutputStream(local);
		byte[] ioBuffer = new byte[1024];
		int readLen = hdfsInStream.read(ioBuffer);

		while (-1 != readLen) {
			out.write(ioBuffer, 0, readLen);
			readLen = hdfsInStream.read(ioBuffer);
		}
		System.out.println("-------------------readFromHdfs end------------------------");
	}

	/**
	 * ÒÔappend·½Ê½½«ÄÚÈÝÌí¼Óµ½HDFSÉÏÎÄ¼þµÄÄ©Î²;
	 * 
	 * ×¢Òâ£ºÎÄ¼þ¸üÐÂ£¬ÐèÒªÔÚhdfs-site.xmlÖÐÌí¼Ó <property> <name>dfs.support.append</name>
	 * <value>true</value> </property>
	 * */

	
	
	/*
	 * 
	 * Ð¡ÎÄ¼þÓÅ»¯
	 * 
	 * */
	
	
	
	
	
	
	private static void appendToHdfs(String dst) throws FileNotFoundException,
			IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataOutputStream out = fs.append(new Path(dst));

		int readLen = "append by hdfs java api".getBytes().length;

		if (-1 != readLen) {
			out.write("append by hdfs java api".getBytes(), 0, readLen);
			// out.sync(); } out.close(); fs.close();
			System.out
					.println("-------------------appendToHdfs end------------------------");
		}
	}

	// ´ÓHDFSÉÏÉ¾³ýÎÄ¼þ



	// ±éÀúHDFSÉÏµÄÄ¿Â¼

	private static void getDirectoryFromHdfs(String dst) throws FileNotFoundException,
			IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus fileList[] = fs.listStatus(new Path(dst));
		
		int size = fileList.length;
		for (int i = 0; i < size; i++) {
			
			if(fileList[i].isDirectory())
				System.out.println("name:" + fileList[i].getPath().getName()
						+ "       size:" + fileList[i].getLen());
			
		}
		fs.close();
		System.out.println("-------------------getDirectoryFromHdfs end------------------------");
	}
	

	
	
	private static void getFileFromHdfs(String dst) throws FileNotFoundException,
	IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus fileList[] = fs.listStatus(new Path(dst));

		int size = fileList.length;
		for (int i = 0; i < size; i++) {
	
			if(!fileList[i].isDirectory())
				System.out.println("name:" + fileList[i].getPath().getName()
				+ "       size:" + fileList[i].getLen());
	
}
fs.close();
System.out.println("-------------------getFileFromHdfs end------------------------");
}

	
	private static void writeTest(FileSystem fs, int count, int seed, Path file,  
            CompressionType compressionType, CompressionCodec codec) 
throws IOException { 
fs.delete(file, true); 


////Ö¸Ã÷Ñ¹Ëõ·½Ê½ 
//	SequenceFile.Writer writer =  
//			SequenceFile.createWriter(fs, conf, file,  
//            RandomDatum.class, RandomDatum.class, compressionType, codec); 
//	RandomDatum.Generator generator = new RandomDatum.Generator(seed); 
//	for (int i = 0; i < count; i ) { 
//		generator.next(); 
//	
////keyh 
//	RandomDatum key = generator.getKey(); 
//
////value 
//	RandomDatum value = generator.getValue(); 
///×·¼ÓÐ´Èë 
//	writer.append(key, value); 
} 
//writer.close(); 
//} 
	

	public void cat(String remoteFile) throws IOException {
        Path path = new Path(remoteFile);
        FileSystem fs = FileSystem.get(URI.create(HDFS_PATH), conf);
        FSDataInputStream fsdis = null;
        System.out.println("cat: " + remoteFile);
        try {  
            fsdis =fs.open(path);
            IOUtils.copyBytes(fsdis, System.out, 4096, false);  
          } finally {  
            IOUtils.closeStream(fsdis);
            fs.close();
          }
    }
	private static void deleteFromHdfs(String dst) throws FileNotFoundException,
	IOException {

Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(URI.create(dst), conf);
fs.deleteOnExit(new Path(dst));
fs.close();
System.out.println("-------------------deleteFromHdfs end------------------------");
}

	private static void mkdirs(String folder) throws IOException {
		   
		   Configuration conf = new Configuration();
	        Path path = new Path(folder);
	        FileSystem fs = FileSystem.get(URI.create(folder), conf);
	        if (!fs.exists(path)) {
	            fs.mkdirs(path);
	            System.out.println("Create: " + folder);
	        }
	        fs.close();
	    }
	    
//	    public void rmr(String folder) throws IOException {
//	        Path path = new Path(folder);
//	        FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
//	        fs.deleteOnExit(path);
//	        System.out.println("Delete: " + folder);
//	        fs.close();
//	    }
	
	
	
	public static void main(String[] args) throws Exception {
		String dst = "hdfs://192.168.1.169:8020/tmp/1.txt";
		String localSrc = "/home/admin/test/1.txt";
		String localSrc01 = "C:\\Users\\yangtao.ou\\Desktop\\testFromHDFS.txt";
		String dst01 = "hdfs://172.17.199.50:8020/tmp/test444.txt";
		String dst02 = "hdfs://211.152.38.164:8020/tmp/testa.txt";
		String dst03 = "har://hdfs-192.168.1.96:8020//outputdir/zoo.har";
		 //deleteFromHdfs(dst);
		// uploadToHdfs(localSrc,dst);
		// mkdirs(dst);
		// deleteFromHdfs(dst);
		// readFromHdfs(dst,localSrc);
		// deleteFromHdfs(dst);
//		Configuration conf = new Configuration();
//		conf.set("fs.default.name", "hdfs://172.17.199.50:8020");
//		HarFileSystem fs = new HarFileSystem();
//		fs.initialize(new URI(dst03), conf);
//		FileStatus[] listStatus = fs.listStatus(new Path("hive"));
//		for (FileStatus fileStatus : listStatus) {
//			System.out.println(fileStatus.getPath().toString());
//		}
		//´òÓ¡ÎÄ¼þÄ¿Â¼
		
		//getDirectoryFromHdfs(dst03);
		
		//´òÓ¡ÎÄ¼þ
		getFileFromHdfs(dst03);
		//
		
		 //appendToHdfs(dst);
		//stringToHdfs("testOOKK",dst );
		//readFromHdfs(dst,localSrc01);
		getDirectoryFromHdfs(dst03);
		
		
		
//		Configuration conf=new Configuration();
//		FileSystem fs=FileSystem.get(conf);
//		Path seqFile=new Path("C:\\Users\\yangtao.ou\\Desktop\\testFromHDFS.txt");
//		//ReaderÄÚ²¿ÀàÓÃÓÚÎÄ¼þµÄ¶ÁÈ¡²Ù×÷
//		SequenceFile.Reader reader=new SequenceFile.Reader(fs,seqFile,conf);
//		//WriterÄÚ²¿ÀàÓÃÓÚÎÄ¼þµÄÐ´²Ù×÷,¼ÙÉèKeyºÍValue¶¼ÎªTextÀàÐÍ
//		SequenceFile.Writer writer=new SequenceFile.Writer(fs,conf,seqFile,Text.class,Text.class);
//		//Í¨¹ýwriterÏòÎÄµµÖÐÐ´Èë¼ÇÂ¼
//		writer.append(new Text("key"),new Text("value"));
//		IOUtils.closeStream(writer);//¹Ø±ÕwriteÁ÷
//		//Í¨¹ýreader´ÓÎÄµµÖÐ¶ÁÈ¡¼ÇÂ¼
//		Text key=new Text();
//		Text value=new Text();
//		while(reader.next(key,value)){
//			System.out.println(key);
//			System.out.println("ok?");
//			//System.out.println(value);
//		}
//		IOUtils.closeStream(reader);//¹Ø±ÕreadÁ÷
	}
}