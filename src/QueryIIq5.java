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

public class QueryIIq5{
	public static void main(String[] args){
		
		try {
			
			try {
				
				Class.forName("oracle.jdbc.driver.OracleDriver");
				Connection conn;
				conn = DriverManager.getConnection(
				        "jdbc:oracle:thin:@//dbod-scan.acsu.buffalo.edu:1521/CSE601_2159.buffalo.edu", "nmanjuna",
				        "cse601");
				Statement state=conn.createStatement();
				ArrayList<Double> ALL= new ArrayList<Double>();
				ArrayList<Double> AML= new ArrayList<Double>();
				ArrayList<Double> ColonTumor= new ArrayList<Double>();
				ArrayList<Double> BreastTumor= new ArrayList<Double>();
				
				
				String query="SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME = 'ALL')) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0010083)";
				ResultSet result = state.executeQuery(query);
				while(result.next()) {
					double tmp = result.getInt(1);
					ALL.add(tmp);
				}
				Double[] ALLarr=new Double[ALL.size()];
				double[] arr1=ArrayUtils.toPrimitive(ALLarr=ALL.toArray(ALLarr));
				
				
				
				query="SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME = 'AML')) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0010083)";
				result = state.executeQuery(query);
				while(result.next()) {
					double tmp = result.getInt(1);
					AML.add(tmp);
				}
				
				Double[] AMLarr=new Double[AML.size()];
				double[] arr2=ArrayUtils.toPrimitive(AMLarr=AML.toArray(AMLarr));
				query="SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME = 'Colon tumor')) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0010083)";
				result = state.executeQuery(query);
				while(result.next()) {
					double tmp = result.getInt(1);
					ColonTumor.add(tmp);
				}
				
				Double[] CTarr=new Double[ColonTumor.size()];
				double[] arr3=ArrayUtils.toPrimitive(CTarr=ColonTumor.toArray(CTarr));
				
				query="SELECT EXP FROM MICROARRAY_FACT WHERE S_ID IN (SELECT S_ID FROM CLINICAL_FACT WHERE P_ID IN (SELECT P_ID FROM CLINICAL_FACT, DISEASE WHERE CLINICAL_FACT.DS_ID = DISEASE.DS_ID AND DISEASE.NAME = 'Breast tumor')) AND PB_ID IN (SELECT PB_ID FROM PROBE, GENE_FACT WHERE PROBE.UID_PROBE = GENE_FACT.UID1 AND GO_ID = 0010083)";
				result = state.executeQuery(query);
				while(result.next()) {
					double tmp = result.getInt(1);
					BreastTumor.add(tmp);
				}
				
				Double[] BTarr=new Double[BreastTumor.size()];
				double[] arr4=ArrayUtils.toPrimitive(BTarr=BreastTumor.toArray(BTarr));
				
				
				
		         HashSet<double[]> hs = new HashSet<double[]>();
		         hs.add(arr1);
		         hs.add(arr2);
		         hs.add(arr3);
		         hs.add(arr4);
		         
		         OneWayAnova f_stat = new OneWayAnova();
		         double val = f_stat.anovaFValue(hs);
		         System.out.println("The F statistics of the expression values among patients with ALL, AML, Colon Tumor, Breast Tumor:" );
		         System.out.println(val);
		        		 
				
				state.close();
				conn.close();
				
				
				
				
				
				
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
		