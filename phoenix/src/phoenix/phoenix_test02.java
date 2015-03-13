package phoenix;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class phoenix_test02 {

	public static void main(String[] args) throws SQLException {
		Statement stmt = null;
		ResultSet rset = null;
		System.out.println("****************");
		Connection con = DriverManager.getConnection("jdbc:phoenix:master2");
		stmt = con.createStatement();
		System.out.println("----------------");
		//stmt.execute("CREATE  TABLE if not exists IDOCUMENT1(SERVERID            INTEGER         ,SERVERNAME          VARCHAR         ,LANGUAGECODE        VARCHAR         ,MAXLOGONRETRY       SMALLINT        N,IDOCFLAG            SMALLINT        ,constraint PK_SERVERID PRIMARY KEY   (SERVERID));");
		stmt.execute("CREATE  TABLE if not exists IDOCUMENT2(SERVERID            INTEGER         NOT NULL,SERVERNAME          VARCHAR         NOT NULL,LANGUAGECODE        VARCHAR         NOT NULL,MAXLOGONRETRY       SMALLINT        NOT NULL,IDOCFLAG            SMALLINT        NOT NULL,constraint PK_SERVERID PRIMARY KEY   (SERVERID))");
		//stmt.executeUpdate("CREATE TABLE T (K VARCHAR PRIMARY KEY,V1 VARCHAR,V2 VARCHAR)");   //�½��
		//stmt.executeUpdate("UPSERT INTO T(K,V1) VALUES('a','b')");    //�������
	//	stmt.executeUpdate("upsert into test02 values (2,'World1')");  
		//�������
		con.commit();     
		
		//�ύ���
		//PreparedStatement statement = con.prepareStatement("select * from test02 where mykey=1");    //�����ֶεĲ�ѯ
		PreparedStatement statement = con.prepareStatement("select * from test02");                    //ȫ���Ĳ�ѯ
		stmt.executeUpdate("DELETE FROM test02 WHERE mykey=1");                                         //��������    (���������Ա�����Сд�����У��������Դ�д��Ҳ����Сд)*/	
		
		stmt.executeUpdate("upsert into test02 values (2,'ok')");             //phoenix��û���޸ĵ����䣬ֻ�и��ǣ��������޸ĵĻ���ֻҪ���и��Ǽ��
		
		
		con.commit();          
		
		
	
		PreparedStatement statement1 = con.prepareStatement("select * from test02");              //ȫ���Ĳ�ѯ*/	
		rset = statement1.executeQuery();
		while (rset.next()) {
			System.out.println(rset.getString("mycolumn"));
		}
		statement1.close();
		con.close();
	}
}
