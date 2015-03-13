package com.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.commons.net.ftp.FTPClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileOutputStream;


/**
 * JAVA FTP上传下载 助手类
 * 
 * Get logflie into localhost to preprocess!
 * 
 * @author : Ou yangtao
 */
public class ftpClient {

	/**
	 * FTP上传单个文件测试
	 * @param pathout 
	 * @param pathdown 
	 */
	public static void fileUploadByFtp(String pathup, String pathdown) {
		FTPClient ftpClient = new FTPClient();
		FileInputStream fis = null;

		try {
			ftpClient.connect("222.73.156.30");
			ftpClient.login("ftpuser", "feixun*123");

			File srcFile = new File(pathup);
			fis = new FileInputStream(srcFile);
			// 设置上传目录
			ftpClient.changeWorkingDirectory(pathdown);
			ftpClient.setBufferSize(1024);
			ftpClient.setControlEncoding("GBK");
			// 设置文件类型（二进制）
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.storeFile("201502191400.tar.gz.ef", fis);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("FTP客户端出错！", e);
		} finally {
			IOUtils.closeQuietly(fis);
			try {
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("关闭FTP连接发生异常！", e);
			}
		}
	}

	/**
	 * FTP下载单个文件测试
	 * @param pathdown 
	 * @param pathup 
	 */
	public static void fileDownloadByFtp(String pathup, String pathdown) {
		FTPClient ftpClient = new FTPClient();
		FileOutputStream fos = null;

		try {
			ftpClient.connect("222.73.156.30");
			ftpClient.login("ftpuser", "feixun*123");

			String remoteFileName = pathup;
			// fos = new FileOutputStream("E:/test/test_back_081901.sql");
			fos = new FileOutputStream(pathdown);
			ftpClient.setBufferSize(1024);
			// 设置文件类型（二进制）
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.retrieveFile(remoteFileName, fos);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("FTP客户端出错！", e);
		} finally {
			IOUtils.closeQuietly(fos);
			try {
				ftpClient.disconnect();
			} catch (IOException e) {
				e.printStackTrace();
				throw new RuntimeException("关闭FTP连接发生异常！", e);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		
		/*
		 * up and down By FTP
		 */
//		
//		String pathup = "/201502191400.tar.gz.ef";
//		String pathdown = "/usr/hdp/test/201502191400.tar.gz.ef";
//		//fileUploadByFtp(pathup,pathdown);
//		fileDownloadByFtp(pathup,pathdown);
		
		/*
		 * in and out  By  jiami
		 */
		String pathin = "/usr/hdp/test/201502191400.tar.gz.ef";
		String pathout = "/usr/hdp/test/201502191400.tar.gz";
		String pathend = "/usr/hdp/test";
//		jiami jiami01 = new jiami();
//		jiami01.jiemi(pathin, pathout);
//		jiami01.jieya(pathout, pathend);
//	
		
		/*
		 * send to destinaton  By  chuansong
		 */
		
		String pathsrc = "/usr/hdp/test/test/ok/";
		String pathdsc = "192.168.1.177:/usr/hdp/test/";
		chuansong(pathsrc,pathdsc);
	

	}


	private static void chuansong(String pathsrc, String pathdsc) {
	
		
		try {
			Runtime.getRuntime().exec(new  String[]{"scp","-r",pathsrc,pathdsc});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("-------------------chuansong end------------------------");
		// TODO Auto-generated method stub
		
	}
}
