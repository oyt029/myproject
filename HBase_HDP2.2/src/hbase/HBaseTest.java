package hbase;


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HBaseTest {
	public static Configuration conf;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "master,slave3,slave4");
	}
	public static void CreateTable(String tableName){
		System.out.println("Create a new HBase Table");
		
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.tableExists(tableName)){
				System.out.println("Table " + tableName + "exists!");
				System.out.println("Delete the Table" + tableName);
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				System.out.println("Delete " + tableName + " Successful!");
			}
			HTableDescriptor tableDes = new HTableDescriptor(tableName);
			tableDes.addFamily(new HColumnDescriptor("Info"));
			tableDes.addFamily(new HColumnDescriptor("HospitalInfo"));
			admin.createTable(tableDes);
			
			admin.close();
			
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Table " + tableName + " Successful Created!");
	}
	public static void main(String args[]){
		CreateTable("myHBase");
	}
}
