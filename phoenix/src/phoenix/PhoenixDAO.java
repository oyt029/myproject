package phoenix;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class PhoenixDAO {  
    private String url = "jdbc:phoenix:master2";  
   private String driver = "org.apache.phoenix.jdbc.PhoenixDriver";  
    private Connection connection = null;  
  
    public PhoenixDAO() {  
        try {  
            Class.forName(driver);  
            connection = DriverManager.getConnection(url,"root","password");  
            System.out.println("Connect HBase success..");  
        } 
        //catch (ClassNotFoundException e) {  
           // e.printStackTrace();  
        catch (SQLException e) {  
            e.printStackTrace();  
        } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  
    }  
  
    public void close() {  
        if (connection != null) {  
            try {  
                connection.close();  
            } catch (Exception e) {  
            } finally {  
                connection = null;  
            }  
        }  
    }  
  
    public void createTable(String sql ) {  
          
        try {  
            Statement statement = connection.createStatement();  
            statement.executeUpdate(sql);  
            connection.commit();  
            System.out.println("create table success: " + sql);  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void deleteTable() {  
        String sql = "drop table if exists pho_test";  
        try {  
            Statement statement = connection.createStatement();  
            statement.executeUpdate(sql);  
            connection.commit();  
            System.out.println("delete table success: " + sql);  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void insertRecord(String sql) {  
        
        try {  
            PreparedStatement statement = connection.prepareStatement(sql);  
            Random random = new Random();  
            for (int i = 1; i <= 100; i++) {  
                statement.setInt(1,i);  
                statement.setString(2,"phoenix_test" + i);  
                statement.setInt(3, random.nextInt(18));  
                statement.setInt(4, random.nextInt(100));  
                statement.setInt(5,random.nextInt(3));  
                statement.execute();  
            }  
            connection.commit();  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
  
    public void selectRecord() {  
        String sql = "select * from NAMECATALOG";  
        ResultSet resultSet = null;  
        try {  
            Statement statement = connection.createStatement();  
            resultSet = statement.executeQuery(sql);  
     
            while (resultSet != null && resultSet.next()) {
            	sql ="UPSERT INTO NAMECATALOG VALUES (" + resultSet.getString(1) + ",'" + resultSet.getString(2)  
                 + "'," + resultSet.getString(3) + ",'" + resultSet.getString(4) + "','" + resultSet.getString(5)+"',3)";
            	//System.out.println(sql);
            	statement.executeUpdate(sql);  
                connection.commit();  
            }  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  finally{
        	try {
				resultSet.close();
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
    }  
    
    
    public void update(String tablename,Object value,int columnnum) {  
        String sql = "select * from " + tablename;  
        ResultSet resultSet = null;  
        try {  
            Statement statement = connection.createStatement();  
            resultSet = statement.executeQuery(sql);  
          
				
                 sql = null;
            	for (int j = 1;  resultSet != null && resultSet.next(); j++) {
              	
            	sql =sql+ resultSet.getString(j);
            	//statement.executeUpdate(sql); 
            	
              
            }  
            	sql = "UPSERT INTO NAMECATALOG VALUES (" +sql + "',3)";
            	System.out.println(sql);
            	  connection.commit();     
                  
        
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  finally{
        	try {
				resultSet.close();
				connection.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	
        }
    }  
    
    
  
    public static void main(String[] args) { 
    	PhoenixDAO client = new PhoenixDAO(); 
    	//client.selectRecord();
//    	File workaround = new File(".");
//		  System.getProperties().put("hadoop.home.dir",workaround.getAbsolutePath());
//		  new File("./bin").mkdirs();
//		  try {
//			new File("./bin/winutils.exe").createNewFile();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//        PhoenixDAO client = new PhoenixDAO();  
          String sql01,sql02,sql03;
          sql01 = "create table IF NOT EXISTS phoenix_test(stuid integer not null primary key,name VARCHAR,age integer,score integer,classid integer)";
          sql02 = "upsert into phoenix_test(stuid,name,age,score,classid) values  (?,?,?,?,?)";
        client.createTable(sql01);
        client.insertRecord(sql02);  
        System.out.println("...............end...................");
       // client.selectRecord(sql03);  
        //client.deleteTable(z );
    	
    	
    	
    	
    }  
}  
