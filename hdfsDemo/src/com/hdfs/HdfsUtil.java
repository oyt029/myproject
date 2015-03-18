package com.hdfs;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsUtil {
	
	//public static String path = "hdfs://10.83.3.194:8020/";
	
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
	 * 上传文件到HDFS上去
	 */
	public static void uploadToHdfs(InputStream in,String dst) throws FileNotFoundException,
			IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataOutputStream out = fs.create(new Path(dst));
		IOUtils.copyBytes(in, out, 4096, true);

	}

	/**
	 * search file in server
	 */
	public static String readFromHdfs(String dst) throws FileNotFoundException,
			IOException {
		 //String dst = "hdfs://192.168.1.96:8020/tmp/QQ-test.jpg";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		FileStatus fileStatus =  fs.getFileStatus(new Path(dst));  
        BlockLocation[] blockLocations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        String hostip = "";
        for(BlockLocation block : blockLocations){  
        	hostip = Arrays.toString(block.getNames());
        	//System.out.println(Arrays.toString(block.getHosts())+ "\t" + Arrays.toString(block.getNames()));  
        }  
        if(!hostip.equals(""))
        	hostip = hostip.substring(1,hostip.indexOf(":"));
		//OutputStream out = new FileOutputStream(local);
		//byte[] ioBuffer = new byte[1024];
		//int readLen = hdfsInStream.read(ioBuffer);
        return hostip;
		//while (-1 != readLen) {
		//	out.write(ioBuffer, 0, readLen);
		//	readLen = hdfsInStream.read(ioBuffer);
		//}
	}
	
	// 从HDFS上删除文件或者文件夹
	public static void deleteFromHdfs(String dst) throws FileNotFoundException,
			IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		fs.deleteOnExit(new Path(dst));
		fs.close();
	}
	
	/**
	 * 从HDFS创建文件夹
	 * @param dst
	 * @throws Exception
	 */
	public static void mkdir(String folder) throws Exception{
		Configuration conf = new Configuration();
		Path path = new Path(folder);
		FileSystem fs = FileSystem.get(URI.create(folder),conf);
		fs.mkdirs(path);
		fs.close();
	}

	// 遍历HDFS上的目录
	public static String getDirectoryFromHdfs(String dst) throws Exception{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(dst), conf);
		FileStatus fileList[] = fs.listStatus(new Path(dst));
		String dirs = "";
		int size = fileList.length;
		for (int i = 0; i < size; i++) {
			if(fileList[i].isDirectory())
				dirs = fileList[i].getPath().getName()+","+dirs;
		}
		fs.close();
		return dirs;
	}
	
	// 遍历HDFS上的文件
	public static void getFileFromHdfs(String dst) throws Exception {
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
		System.out.println("-------------------getDirectoryFromHdfs end------------------------");

	}
	
}