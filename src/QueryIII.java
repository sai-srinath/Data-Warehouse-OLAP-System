import java.text.DecimalFormat;
import java.util.*;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.inference.TTest;
import org.apache.commons.lang3.ArrayUtils;

public class QueryIII{
	public static void main(String[] args){
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection connection;
			try {
				connection = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement statement=connection.createStatement();
				String a="";
				if(args.length==0)
					a="ALL";
				else 
					a=args[0];
				
				String query="SELECT P.UID_Probe, M.EXP FROM MICROARRAY_FACT M, PROBE P, CLINICAL_FACT C WHERE M.PB_ID = P.PB_ID AND M.S_ID = C.S_ID AND C.P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME='"+a+"')";
				
				ResultSet result = statement.executeQuery(query);
				HashMap<String,ArrayList<Double>> all = new HashMap<String,ArrayList<Double>>();
				int ct=0;
				ArrayList<String> all_id = new ArrayList<String>();
				
				
				while(result.next()){
					if(!all.containsKey(result.getString(1)))
						all.put(result.getString(1),new ArrayList<Double>());
					all.get(result.getString(1)).add(result.getDouble(2));
				}
				
				
				
				
				
				
				
				
				
				
				
				HashMap<String,ArrayList<Double>> notall = new HashMap<String,ArrayList<Double>>();
				query="SELECT P.UID_Probe, M.EXP FROM MICROARRAY_FACT M, PROBE P, CLINICAL_FACT C WHERE M.PB_ID = P.PB_ID AND M.S_ID = C.S_ID AND C.P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND NAME <> '"+a+"')";
				result=statement.executeQuery(query);
				ct=0;
				while(result.next()){
					if(!notall.containsKey(result.getString(1)))
						notall.put(result.getString(1),new ArrayList<Double>());
					notall.get(result.getString(1)).add(result.getDouble(2));
					
				}
				
				
				
				
				ArrayList<String> informative = new ArrayList<String>();
				for(String s:all.keySet()){
					TTest testval = new TTest();
					
					Double[] allarray = new Double[all.get(s).size()];
					allarray = all.get(s).toArray(allarray);
					
					Double[] notallarray = new Double[notall.get(s).size()];
					notallarray = notall.get(s).toArray(notallarray);
					
					double value = testval.tTest(ArrayUtils.toPrimitive(allarray), ArrayUtils.toPrimitive(notallarray));
					if(value<0.01)
						informative.add(s);
				}
				
				
				
				
				System.out.println("No of informative genes: "+informative.size());
				
				
				
				
				//Question 2
				query="SELECT * FROM TEST_SAMPLES";
				result = statement.executeQuery(query);
				HashMap<String,ArrayList<Double>> testmap = new HashMap<String,ArrayList<Double>>();
				testmap.put("test1", new ArrayList<Double>());
				testmap.put("test2", new ArrayList<Double>());
				testmap.put("test3", new ArrayList<Double>());
				testmap.put("test4", new ArrayList<Double>());
				testmap.put("test5", new ArrayList<Double>());
				while(result.next()){
					
					if(informative.contains(result.getString(1))){
						
						testmap.get("test1").add(result.getDouble(2));
						testmap.get("test2").add(result.getDouble(3));
						testmap.get("test3").add(result.getDouble(4));
						testmap.get("test4").add(result.getDouble(5));
						testmap.get("test5").add(result.getDouble(6));
						
					
						
					}
					
				}
				
				
				String info="";
				for(String s:informative){
					info=info+s+",";
				}
				
				
				info=info.substring(0, info.length()-1);
				query="SELECT CLINICAL_FACT.P_ID, MICROARRAY_FACT.EXP FROM MICROARRAY_FACT, PROBE,CLINICAL_FACT WHERE MICROARRAY_FACT.PB_ID = PROBE.PB_ID AND MICROARRAY_FACT.S_ID = CLINICAL_FACT.S_ID AND CLINICAL_FACT.P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME = '"+a+"') AND PROBE.UID_PROBE IN("+info+")";
				result = statement.executeQuery(query);
				HashMap<String,ArrayList<Double>> allmap = new HashMap<String,ArrayList<Double>>();
				while(result.next()){
					
					if(!allmap.containsKey(result.getString(1)))
						allmap.put(result.getString(1),new ArrayList<Double>());
					allmap.get(result.getString(1)).add(result.getDouble(2));
					
				}
				
				
				
				query="SELECT CLINICAL_FACT.P_ID, MICROARRAY_FACT.EXP FROM MICROARRAY_FACT, PROBE,CLINICAL_FACT WHERE MICROARRAY_FACT.PB_ID = PROBE.PB_ID AND MICROARRAY_FACT.S_ID = CLINICAL_FACT.S_ID AND CLINICAL_FACT.P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME <> '"+a+"') AND PROBE.UID_PROBE IN("+info+")";
				result = statement.executeQuery(query);
				HashMap<String,ArrayList<Double>> notallmap = new HashMap<String,ArrayList<Double>>();
				while(result.next()){
					
					if(!notallmap.containsKey(result.getString(1)))
						notallmap.put(result.getString(1),new ArrayList<Double>());
					notallmap.get(result.getString(1)).add(result.getDouble(2));
					
				}
				
				
				ArrayList<String> ALL = new ArrayList<String>();
				ArrayList<String> NOTALL = new ArrayList<String>();
				
				
				
				for(String s1:testmap.keySet()){
					int allptr=0;
					int notallptr=0;
					double[] allarray =new double[41];
					double[] notallarray = new double[41];
					//System.out.println("Lengths: "+allarray.length+","+notallarray.length);
					
					PearsonsCorrelation corr = new PearsonsCorrelation();
					Double[] testarr = new Double[testmap.get(s1).size()];
					testarr = testmap.get(s1).toArray(testarr);
					
					
					
					for(String s2:allmap.keySet()){
						
						Double[] tmpall = new Double[allmap.get(s2).size()];
						tmpall = allmap.get(s2).toArray(tmpall);
						
						
						double d =corr.correlation(ArrayUtils.toPrimitive(testarr),ArrayUtils.toPrimitive(tmpall));
						allarray[allptr]=d;
						allptr++;
						//System.out.println("notallptr is: "+allptr);
						
						
						
					}
					
					for(String s3:notallmap.keySet()){
						Double[] tmpnotall = new Double[notallmap.get(s3).size()];
						tmpnotall = notallmap.get(s3).toArray(tmpnotall);
						double d =corr.correlation(ArrayUtils.toPrimitive(testarr),ArrayUtils.toPrimitive(tmpnotall));
						notallarray[notallptr]=d;
						notallptr++;
						//System.out.println("notallptr is: "+notallptr);
						
						
					}
					
					TTest testval = new TTest();
					double val = testval.tTest(allarray, notallarray);
					if(val<0.01)
						ALL.add(s1);
					else
						NOTALL.add(s1);
						
				}
				
				System.out.println("ALL:"+ALL.toString());
				System.out.println("NOTALL:"+NOTALL.toString());
				
				
				
				
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