package com.convector.kafkaspark.main;

import com.convector.kafkaspark.job.AggregateJob;
import com.convector.kafkaspark.job.HBaseJob;
import com.convector.kafkaspark.job.PrintJob;

/**
 * Created by pyro_ on 18/06/2016.
 */
public class Main {

    public static void main(String[] args ) {
    	try{
    		if(args[0].equals("printjob")){
            	PrintJob.run();        	
            }
            else if (args[0].equals("hbasejob")){
            	HBaseJob.run();
            }
            else if (args[0].equals("aggregate")){
            	AggregateJob.run();
            }
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
        
    }
}
