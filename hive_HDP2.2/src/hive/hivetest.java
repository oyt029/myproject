package hive;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import hive.CopyOfhiveDAO;

public class hivetest {
	public static void main(String[] args) {
		
	CopyOfhiveDAO hive = new CopyOfhiveDAO();
	Connection conn = null;
	Statement stmt = null;
	
		try {
			conn = hive.getConn();
		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String tableName;
		tableName = "squidtable";	
		try {
			hive.describeTables(stmt, tableName);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	
}
