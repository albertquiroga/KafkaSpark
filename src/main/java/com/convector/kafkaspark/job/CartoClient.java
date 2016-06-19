package com.convector.kafkaspark.job;

import java.io.Serializable;

import com.cartodb.CartoDBClientIF;
import com.cartodb.CartoDBException;
import com.cartodb.impl.ApiKeyCartoDBClient;

public class CartoClient implements Serializable{
	
	private CartoDBClientIF cartoDBCLient;
	
	public CartoClient(){
		try {
			cartoDBCLient = new ApiKeyCartoDBClient("cteixidogalvez", "7977f5eaebeb6279ca4b9224a1f2988f14ee1824");
		} catch (CartoDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String executeQuery(String query) {
		try {
			return cartoDBCLient.executeQuery(query);
		} catch (CartoDBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

}
