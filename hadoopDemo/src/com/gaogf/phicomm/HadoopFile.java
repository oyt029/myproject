package com.gaogf.phicomm;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Operate Hadoop FileSystem
 * @author root
 *
 */
public class HadoopFile {
	public static final String HDFS_PATH="hdfs://192.168.1.114:/tmp";
	public static final String DIR_PATH="/home/admin/test_oyt01";
	public static final String FILE_PATH="/home/admin/test_oyt01txt";
	
	public static void upLoadFile(FileSystem fileSystem) throws IOException{
		FSDataOutputStream out = fileSystem.create(new Path(DIR_PATH));
		FileInputStream in = new FileInputStream(new File("/home/admin/test/1.txt"));
		IOUtils.copyBytes(in, out, 2048,true);
	}
	
	public static void mkDir(FileSystem fileSystem) throws IOException{
		fileSystem.mkdirs(new Path(DIR_PATH));
	}
	
	public static void deleteFileFromHadoop(FileSystem fileSystem) throws IOException{
		fileSystem.delete(new Path(DIR_PATH),true);
	}
	
	public static void downloadFile(FileSystem fileSystem) throws IOException{
		FSDataInputStream in = fileSystem.open(new Path(FILE_PATH));
		FileOutputStream out = new FileOutputStream(new File("/home/admin/test2/2.txt"));
		IOUtils.copyBytes(in, out, 2046, true);
	}
	public static void main(String args[]) throws IOException, URISyntaxException{
		FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH),new Configuration());
		mkDir(fileSystem);
		System.out.println("create ok");
		upLoadFile(fileSystem);
//		downloadFile(fileSystem);
//		deleteFileFromHadoop(fileSystem);
	}
}
