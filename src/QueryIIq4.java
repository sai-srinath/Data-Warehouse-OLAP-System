
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

public class QueryIIq4{
	public static void main(String[] args){
		
		try {
			
			try {
				
				Class.forName("oracle.jdbc.driver.OracleDriver");
				Connection conn;
				conn = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement state=conn.createStatement();
				
				ArrayList<Integer> list1=new ArrayList<Integer>();
				ArrayList<Integer> list2=new ArrayList<Integer>();
				String a = "";
				if(args.length==0)
					a="NAME = 'ALL'";
				else
				{
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
				String query = "SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE."+a+")) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0012502)";
				ResultSet result = state.executeQuery(query);
				while(result.next()) {
					int tmp = result.getInt(1);
					list1.add(tmp);
				}
				
				double mean1=Mean(list1);
				double var1=Variance(list1,mean1);
				
				query = "SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME <> 'ALL')) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0012502)";
				result = state.executeQuery(query);
				
				while(result.next()) {
					int tmp1 = result.getInt(1);
					list2.add(tmp1);
				}
				
				double mean2=Mean(list2);
				double var2=Variance(list2,mean2);
				
				
				double equalVariance=(((list1.size()-1)*var1)+((list2.size()-1)*var2))/(list1.size()+list2.size()-2);
				double t_statistics=(mean1-mean2)/Math.sqrt(((equalVariance/list1.size()) + (equalVariance/list2.size()) ));
				//System.out.println("The t statistics of the expression values between patients with "+a+" and without "+a+": ");
				if(args.length==0)
					System.out.println("The t statistics of the expression values between patients with ALL and without ALL: " );
				else 
					System.out.println("The t statistics of the expression values between patients with "+args[1]+" and without "+args[1]+": ");
				System.out.println(t_statistics);
				
				
				
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
					
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		}
	
	public static double Mean( ArrayList<Integer> list) {
		double mean=0.0;
		
		for(int i=0;i<list.size();i++)
			mean=mean+list.get(i);
		mean=mean/list.size();
		return mean;
		
	}
	
	public static double Variance( ArrayList<Integer> list,double mean) {
		double var=0.0;
		
		for(int i=0;i<list.size();i++)
			var=var+Math.pow((list.get(i)-mean), 2);
		
		var=var/(list.size()-1);
		return var;
		
	}

	}
		
