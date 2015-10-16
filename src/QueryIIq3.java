
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

public class QueryIIq3{
	public static void main(String[] args){
		
		try {
			
			try {
				
				Class.forName("oracle.jdbc.driver.OracleDriver");
				Connection conn;
				conn = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement state=conn.createStatement();
				String a="";
				if(args.length==0)
					a="NAME = 'ALL'";
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
				
					//a=args[0];
				}
				String query = "SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT CLINICAL_FACT.P_ID FROM CLINICAL_FACT, DISEASE WHERE DISEASE.DS_ID=CLINICAL_FACT.DS_ID AND DISEASE."+a+")) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND CL_ID = 2) AND MU_ID = 1";
				ResultSet result = state.executeQuery(query);
				int ct=0;
				//System.out.println("The mRNA values for patients with "+a+": ");
				if(args.length==0)
					System.out.println("The mRNA values for patients with ALL:" );
				else 
					System.out.println("The mRNA values for patients with "+args[1]+": ");
				while(result.next()){
					
					
					String count = result.getString(1);	
					ct++;
					System.out.println(ct+":"+count);				
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
		
