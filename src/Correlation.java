
import java.text.DecimalFormat;
import java.util.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;


public class Correlation{
	public static void main(String[] args){
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			try {
				Connection connection = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement statement=connection.createStatement();
				String a="",b="";
				if(args.length==0) {
					a="NAME = 'ALL'";
					b="NAME = 'AML'";
					
				}
				else {
					if(args[0].equals("name")){
						
						
						a="NAME='"+args[1]+"'";
						b="NAME='"+args[2]+"'";
					}
					if(args[0].equals("type")){
						
						
						a="TYPE='"+args[1]+"'";
						b="TYPE='"+args[2]+"'";
						
					}
					if(args[0].equals("description")){
						
						
						a="DESCRIPTION='"+args[1]+"'";
						b="DESCRIPTION='"+args[2]+"'";
						
					}
				
					//a=args[0];
				}
				String query="SELECT CLINICAL_FACT.P_ID, EXP FROM MICROARRAY_FACT, CLINICAL_FACT WHERE MICROARRAY_FACT.S_ID = CLINICAL_FACT.S_ID AND MICROARRAY_FACT.S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE."+a+")) AND MICROARRAY_FACT.PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0007154)";
				
				
				
				
				ResultSet result = statement.executeQuery(query);
				HashMap<String,ArrayList<Double>> All_map = new HashMap<String,ArrayList<Double>>();
				while(result.next()){
					if(!All_map.containsKey(result.getString(1)))
						All_map.put(result.getString(1),new ArrayList<Double>());
					All_map.get(result.getString(1)).add(result.getDouble(2));
					
				}
		
				
			
				query="SELECT CLINICAL_FACT.P_ID, EXP FROM MICROARRAY_FACT, CLINICAL_FACT WHERE MICROARRAY_FACT.S_ID = CLINICAL_FACT.S_ID AND MICROARRAY_FACT.S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE."+b+")) AND MICROARRAY_FACT.PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0007154)";
				result = statement.executeQuery(query);
				
				
				
				HashMap<String,ArrayList<Double>> Aml_map = new HashMap<String,ArrayList<Double>>();
				
				while(result.next()){
					if(!Aml_map.containsKey(result.getString(1)))
						Aml_map.put(result.getString(1),new ArrayList<Double>());
					Aml_map.get(result.getString(1)).add(result.getDouble(2));
					
				}
				
				
				
				
				
				double total_correlation=0;
				double ct=0;
				for(String i:All_map.keySet()){
					for(String j:All_map.keySet()){
						if(i!=j){
							PearsonsCorrelation correlation = new PearsonsCorrelation();
							Double[] iarray = new Double[All_map.get(i).size()];
							iarray = All_map.get(i).toArray(iarray);
							Double[] jarray = new Double[All_map.get(j).size()];
							jarray = All_map.get(j).toArray(jarray);
							double d =correlation.correlation(ArrayUtils.toPrimitive(iarray),ArrayUtils.toPrimitive(jarray));
							
							total_correlation+=d;
							ct++;
							
						}
					}
				}
				double avg_correlation=total_correlation/ct;
				if(args.length==0)
					System.out.println("Avg correlation between two patients with ALL is: "+avg_correlation);
				else
					System.out.println("Avg correlation between two patients with "+args[1]+" is: "+avg_correlation);
				//now between ALL and AML
				double total_aml=0;
				ct=0;
				for(String i:All_map.keySet()){
					for(String j:Aml_map.keySet()){
						PearsonsCorrelation correlation = new PearsonsCorrelation();
						Double[] iarray = new Double[All_map.get(i).size()];
						iarray = All_map.get(i).toArray(iarray);
						Double[] jarray = new Double[Aml_map.get(j).size()];
						jarray = Aml_map.get(j).toArray(jarray);
						double d =correlation.correlation(ArrayUtils.toPrimitive(iarray),ArrayUtils.toPrimitive(jarray));
						
						total_aml+=d;
						ct++;
						
					}
					
				}
				double avgAllAml_correlation=total_aml/ct;
				if(args.length==0)
					System.out.println("Avg correlation between a patient who has ALL and a patient who has AML is: "+avgAllAml_correlation);
				else
					System.out.println("Avg correlation between a patient who has "+args[1]+" and a patient who has "+args[2]+ " is: "+avgAllAml_correlation);
				statement.close();
				connection.close();
				
				
				
				
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