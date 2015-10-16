
import java.text.DecimalFormat;
import java.util.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.inference.OneWayAnova;

public class QueryIIq2{
	public static void main(String[] args){
		
		try {
			
			try {
				
				Class.forName("oracle.jdbc.driver.OracleDriver");
				Connection conn;
				conn = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement state=conn.createStatement();
				String a ="";
				if(args.length==0) {
					
				    //System.out.println(args.length==0);
					a="DESCRIPTION='tumor'";
				}
				else {
					if(args[0].equals("name")){
						
					
						a="NAME='"+args[1]+"'";
						
					}
					if(args[0].equals("type")){
						
						
						a="TYPE='"+args[1]+"'";
						
					}
					if(args[0].equals("description")){
						
						
						a="DESCRIPTION='"+args[1]+"'";
						
					}
					
				}
				
				
				//System.out.println(a);
				String query =	"SELECT DISTINCT(DRUG.TYPE) FROM CLINICAL_FACT, DISEASE, DRUG WHERE DISEASE."+a+" AND DISEASE.DS_ID=CLINICAL_FACT.DS_ID AND CLINICAL_FACT.DR_ID=DRUG.DR_ID";
				ResultSet result = state.executeQuery(query);
				if(args.length==0)
					System.out.println("Types of drugs for patients with tumor:" );
				else 
					System.out.println("Types of drugs for patients with "+args[1]+": ");
				while(result.next()){
					//int id=result.getInt(0);
					String count = result.getString(1);	
					System.out.println(count);			
				}
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
					
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		}
	}
		