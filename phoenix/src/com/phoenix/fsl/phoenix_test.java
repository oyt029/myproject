package com.phoenix.fsl;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class phoenix_test {

	public static void main(String[] args) throws SQLException {
		Statement stmt = null;
		ResultSet rset = null;
		System.out.println("****************");
		Connection con = DriverManager.getConnection("jdbc:phoenix:master2");
		stmt = con.createStatement();
		System.out.println("----------------");
		stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar)");   //�½��
		stmt.executeUpdate("upsert into test values (1,'Hello')");    //�������
		stmt.executeUpdate("upsert into test values (2,'World1')");   //�������
		                                           //�ύ���
		//PreparedStatement statement = con.prepareStatement("select * from test where mykey=1");    //�����ֶεĲ�ѯ
		PreparedStatement statement = con.prepareStatement("select * from test");                    //ȫ���Ĳ�ѯ
		stmt.executeUpdate("DELETE FROM TEST WHERE mykey=1");                                         //��������    (���������Ա�����Сд�����У��������Դ�д��Ҳ����Сд)*/	
		
		stmt.executeUpdate("upsert into test values (2,'ok')");             //phoenix��û���޸ĵ����䣬ֻ�и��ǣ��������޸ĵĻ���ֻҪ���и��Ǽ��

		con.commit();          

		//PreparedStatement statement = con.prepareStatement("select * from test");              //ȫ���Ĳ�ѯ*/	
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(rset.getString("mycolumn"));
		}
		statement.close();
		con.close();
	}
}
                                                            

