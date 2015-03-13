package com.gaogf.phicomm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class operateFile {
	 static Configuration conf;
	 static FileSystem hdfs;
	 /*static {
         String path = "/usr/lib/hadoop/etc/hadoop/";
		 conf.addResource(new Path(path + "core-site.xml"));
		 conf.addResource(new Path(path + "hdfs-site.xml"));
		 conf.addResource(new Path(path + "yarn-site.xml"));
		 conf.addResource(new Path(path + "mapred-site.xml"));
		 try{
			 hdfs = FileSystem.get(conf);
		 }catch (IOException e) {
			 e.printStackTrace();
		 }
	 }
	 */
	 public void createDir(String dir){
		 Path path = new Path(dir);
		 try {
			hdfs.mkdirs(path);
			System.out.println("new dir is \t" + conf.get("fs.default.name" + dir));
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	 
	 public void copyFile(String localFile,String hdfsDes){
		 Path src = new Path(localFile);
		 Path des = new Path(hdfsDes);
		 
		 try {
			hdfs.copyFromLocalFile(src, des);
		} catch (IOException e) {
			e.printStackTrace();
		}
		 try {
			FileStatus files[] = hdfs.listStatus(des);
			System.out.println("Upload to \t" + conf.get("fs.default.name" + des));
			for(FileStatus f : files){
				System.out.println(f.getPath());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	 
	 public void createFile(String fileName,String fileContent){
		 Path des = new Path(fileName);
		 byte [] bytes = fileContent.getBytes();
		 try {
			FSDataOutputStream out = hdfs.create(des);
			out.write(bytes);
			System.out.println("new file \t" + conf.get("fs.default.name" + fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	 
	 public void listFile(String fileName){
		 Path path = new Path(fileName);
		 try {
			FileStatus [] status = hdfs.listStatus(path);
			System.out.println(fileName + "has all files:");
			for(FileStatus ss : status){
				System.out.println(ss.getPath().toString());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	 
	 public void deleteFile(String fileName){
		 Path path = new Path(fileName);
		 try {
			boolean isExits = hdfs.exists(path);
			if(isExits){
				boolean isDel = hdfs.delete(path, true);
				System.out.println(fileName + "delete ? \t" + isDel);
			}else{
				System.out.println(fileName + "exists ? \t" + isExits);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	 }
	 
	 public static void main(String args[]){
		 operateFile of = new operateFile();
		 System.out.println("***********create dir*************");
		 //of.createDir("/test");
		 System.out.println("*************copy file************");
		 of.copyFile("/home/admin/test/1.txt", "hdfs://192.168.1.114/tmp");
		 
	 }
}
