package org.influxdb.example;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.Query;

import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author StrakarCe
 * @since 07/05/2021
 * @version 1
 * @category Example
 */
public class Android {


	String address = "http://192.168.1.75:8000/"; // put the address IP of your database
	String dbName = "myDatabase";
	String table = "SERIES";
	QueryResult actual;
	Boolean flag = false; 
	InfluxDB con;
	
	public Android() {
		super();
	}
	
	 public void queryExecute(Query query){
	        Thread thread = new Thread(new Runnable() {

	            @Override
	            public void run() {
	                try  {
	                	//InfluxDB connector = InfluxDBFactory.connect(address); // if you want to open every time 
	                    System.out.println("Send the query to the database ..."); // FOR A REAL APP CREATE A LOGGER ;)
	                    List<QueryResult> results = new LinkedList<>();
	                    actual = con.query(query);
	                } catch (Exception e) {
	                    e.printStackTrace();
	                }
	                flag = true; // For simplicity, I use a simple flag to know when the thread have finished 
	            }
	        });

	        thread.start();
	    }

	 	/**
	 	 * It's to open the connexion with the database. In my case I decide to open once, do many query and close. 
	 	 */
	    public void connexion() {
	        con = InfluxDBFactory.connect(address);
	    }
	    /**
	     * It's to close after my  list of query 
	     */
	    public void close() {
	        con.close();
	    }
	        
	    
	    /* 
	     * simple example of how you can create a query 
	     */
	    private void queryLauncher(String query) {
	    	queryExecute(new Query(query, dbName));
	        while(flag!=true) { // ugly method to wait the thread 

	        }
	        flag= false;
	    }
	    public String getEtat() {
	        queryLauncher("select last(value) from PTEC");
	        return actual.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString();
	    }
	    
	    public String getHC() {
	    	queryLauncher("SELECT last(value) FROM HCHC");
	        return actual.getResults().get(0).getSeries().get(0).getValues().get(0).get(1).toString();
	    }
	    
	    // ------------------------- Example when you want to use it ------------
	    /*
	    Android test = new Android();

        refresh.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                test.connexion();
                etat2.setText(test.getEtat());
                hc2.setText(test.getHC());
                hp2.setText(test.getHP());
                prix2.setText(test.getDepense());
                percMens2.setText(test.getPercentageMensuel());
                percTotal2.setText(test.getPercentageTotal());
                test.close();
            }
        });
	    */
}
