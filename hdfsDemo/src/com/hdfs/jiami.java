/**
 * JAVA FTP上传下载 助手类
 * 
 * Process  logflie  By jiami!
 * 
 * @author : Ou yangtao
 */

package com.hdfs;

import java.io.IOException;



public class jiami {
	
//	public static void main(String[] args) throws Exception {
//		
//		
//
//		String pathin = "/flume/hadoop-2.6.0.2.2.0.0-2041-src.tar.gz";
//		String pathout = "/flume/hadoop-2.6.0.2.2.0.0-2041-src.tar.g.ef";
//		
//		String pathend = "/flume";
//	
//		//jiami(pathin,pathout);
//		//jiemi(pathout,pathin);
//		
//		jieya(pathin,pathend);
//		System.out.println("ok");
//	
//	}
	
	public  void jieya(String pathin, String pathout) {
		// TODO AutoPubblic-generated method stub
		
		// TODO Auto-generated method stub
				try {
					Runtime.getRuntime().exec(new  String[]{"tar","-xvf", pathin,"-C",pathout});
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				System.out.println("-------------------jieya end------------------------");
				// TODO Auto-generated method stub
		
	}

	public  void jiemi(String pathin, String pathout) {
		// TODO Auto-generated method stub
		try {
			Runtime.getRuntime().exec(new  String[]{"openssl","enc", "-d","-aes-128-cbc", "-in",pathin,"-out",pathout,"-k","123abc"});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("-------------------jiemi end------------------------");
		// TODO Auto-generated method stub
		
	}

	private static void jiami(String pathin, String pathout) {
		try {
			Runtime.getRuntime().exec(new  String[]{"openssl","enc", "-aes-128-cbc", "-in",pathin,"-out",pathout,"-k","123abc"});
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("-------------------jiami end------------------------");
		// TODO Auto-generated method stub
		
	}

	
	
}
