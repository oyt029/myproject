package hive;

 
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;  
import java.sql.ResultSet;
import java.sql.SQLException;  
import java.sql.Statement;
public class HiveServer2Client {  
      public static void main(String[] args) throws ClassNotFoundException{  
    	  
    	  try {
    	        Class.forName("org.apache.hive.jdbc.HiveDriver");

    	        Connection conn = DriverManager.getConnection(
    	                "jdbc:hive2://slave31:10000/", "", "");
    	        Statement stmt = conn.createStatement();
    	        String ok;
    	        ok = "ok";
				//createTable(stmt,ok);
    	        showTables(stmt,ok);
    	        stmt.close();
    	        conn.close();
    	    } catch (SQLException se) {
    	        se.printStackTrace();
    	    } catch (Exception e) {
    	        e.printStackTrace();
    	    }
      }
      

  	private static void createTable(Statement stmt, String tableName)
  			throws SQLException {
  		String sql = "create table "
  				+ tableName
  				+ " (key int, value string)  row format delimited fields terminated by '\t'";
  		stmt.executeQuery(sql);
  	}
      
  	private static void showTables(Statement stmt, String tableName)
			throws SQLException {
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running:" + sql);
		ResultSet res = stmt.executeQuery(sql);
		System.out.println("show tables :");
		if (res.next()) {
			System.out.println(res.getString(1));
		}
	}
}  
