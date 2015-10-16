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

public class QueryII{
	public static void main(String[] args){
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			try {
				
				Connection conn = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement state=conn.createStatement();
			
				
				
				//Query 1 part I
				String description = "tumor";
				if(args.length!=0)
				{	description=args[0];}
				
				
				System.out.println("Number of patients with "+description+": ");
				String query = "SELECT COUNT(*) FROM CLINICAL_FACT, DISEASE WHERE  DISEASE.DESCRIPTION='"+description+"' AND DISEASE.DS_ID=CLINICAL_FACT.DS_ID";
						
				
				ResultSet result = state.executeQuery(query);
				
				
				while(result.next()){
					
					String count = result.getString(1);
					
					
					System.out.println(count);
					
					
					
				}
				
				//Query 1 part II
				String descr = "leukemia";
				if(args.length>1)
				{
					descr=args[1];
					
				}
				
				System.out.println("Number of patients with "+descr+":  ");
				
				 query = "SELECT COUNT(*) FROM CLINICAL_FACT, DISEASE WHERE  DISEASE.TYPE='"+descr+"' AND DISEASE.DS_ID=CLINICAL_FACT.DS_ID";
				result = state.executeQuery(query);	
				
				while(result.next()){
					
					String count = result.getString(1);	
					System.out.println(count);				
				}
				
				
				//Query 1 part III
				String des = "ALL";
				if(args.length>2)
				{
					des=args[2];
					
				}
				System.out.println("Number of patients with "+des+":  ");
				query ="SELECT COUNT(*) FROM CLINICAL_FACT, DISEASE WHERE  DISEASE.NAME='"+des+"' AND DISEASE.DS_ID=CLINICAL_FACT.DS_ID";
				result = state.executeQuery(query);	
				
				while(result.next()){
					
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
