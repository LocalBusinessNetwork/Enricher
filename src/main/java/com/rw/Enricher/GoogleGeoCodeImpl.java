package com.rw.Enricher;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.ResourceBundle;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class GoogleGeoCodeImpl extends AgencyBaseImpl {
	
	public static String APIEndPoint = ResourceBundle.getBundle("Enricher").getString("GoogleGeoCode");
	static final Logger log = Logger.getLogger(GoogleGeoCodeImpl.class.getName());

	@Override
	public RWJBusComp EnrichParty(RWJBusComp bc,HttpClient httpclient) throws Exception{
	
		log.trace( "Google EnrichParty: For " + bc.GetFieldValue("_id").toString());
	
		
		//Object latitude = bc.GetFieldValue("latitude_work");
		//Object longitude = bc.GetFieldValue("longitude_work");
		//System.out.println(streetAddress.toString() + " " + city.toString() + " " + state.toString() + " " + postalCode.toString());
		
		String addressQuery = getAddressQuery(getWorkAddressFields(bc));
		
		
		if ( addressQuery != null ) {
				BasicDBObject loc = getCoordinates(addressQuery);
				
				//System.out.println(geo_enrichmentData);
				if (loc != null ) {
					System.out.println(" loc = " + loc.toString());
					bc.SetFieldValue("GeoWorkLocation",loc );
					BasicDBList coordinates = (BasicDBList)loc.get("coordinates");
					if (loc.size() > 0){
						Double lngDbl = loc.getDouble("Longitude");
						Double latDbl = loc.getDouble("Latitude");
						bc.SetFieldValue("longitude_work", lngDbl.toString());
						bc.SetFieldValue("latitude_work", latDbl.toString());
					}
				}

		}
		return bc;
		
	}
	
	private BasicDBObject getWorkAddressFields(RWJBusComp bc){
		BasicDBObject retVal = new BasicDBObject();
		String streetAddress = (String)bc.GetFieldValue("streetAddress1_work");
		String city = (String)bc.GetFieldValue("cityAddress_work");
		String state = (String)bc.GetFieldValue("stateAddress_work");
		String postalCode = (String)bc.GetFieldValue("postalCodeAddress_work");
		if (streetAddress != null && !streetAddress.equals("")){retVal.put("streetAddress", streetAddress);}
		if (city != null && !city.equals("")){retVal.put("city", city);}
		if (state != null && !state.equals("")){retVal.put("state", state);}
		if (postalCode != null && !postalCode.equals("")){retVal.put("postalCode", postalCode);}
		return retVal;
	}
	

	
	public String getAddressQuery(BasicDBObject addressFields){
		String addressQuery = null;
		String postalCode = (addressFields.getString("postalCode") == null)?"":addressFields.getString("postalCode");
		String streetAddress = (addressFields.getString("streetAddress") == null)?"":addressFields.getString("streetAddress");
		streetAddress = streetAddress.split(",")[0];
		streetAddress = streetAddress.replaceAll(".", "");
		String city = (addressFields.getString("city") == null)?"":addressFields.getString("city");
		String state = (addressFields.getString("state") == null)?"":addressFields.getString("state");
		
		System.out.println("GoogleGeoCodeImpl getAddressQuery: " + addressQuery);
		
		if ((postalCode != "") || (city != "" && state != "" && streetAddress != "") ){
			
				addressQuery = "?address="+streetAddress;
				if (city != ""){
					if (!addressQuery.equals("?address=")){addressQuery+="+"+city;}
					else{addressQuery+=city;}
				}
				if (state != ""){
					if (!addressQuery.equals("?address=")){addressQuery+="+"+state;}
					else{addressQuery+=state;}
				}
				if (postalCode != ""){
					if (!addressQuery.equals("?address=")){addressQuery+="+"+postalCode;}
					else{addressQuery+=postalCode;}
				}
				addressQuery+="&sensor=false";
				
				addressQuery = addressQuery.replaceAll(" ","+");
				System.out.println(addressQuery);
		} 
		return addressQuery;
		
	}
	@Override
	public BasicDBObject getEnrichmentData(BasicDBObject inputs){
		return getCoordinates(getAddressQuery(inputs));
	}
	
	public BasicDBObject getCoordinates(String addressQuery){
		
		HttpClient httpclient = new DefaultHttpClient();
		String geo_enrichmentData = EnrichItem(httpclient,addressQuery);
		BasicDBObject loc = null;
		if (geo_enrichmentData != null ) {
			if ( geo_enrichmentData.equals("ZERO_RESULTS") || geo_enrichmentData.equals("QD") ) {
					//bc.SetFieldValue("fc_enriched", geo_enrichmentData);
			}
			else {		
				String latitude = getFieldVal(geo_enrichmentData,"$.results[0].geometry.location.lat",null);
				String longitude = getFieldVal(geo_enrichmentData,"$.results[0].geometry.location.lng",null);
				/*
				BasicDBList coordinates = new BasicDBList();
				try {
					coordinates.add(0, Double.parseDouble(longitude));
					coordinates.add(1, Double.parseDouble(latitude));
					loc = new BasicDBObject();
					loc.put("type", "Point");
					loc.put("coordinates", coordinates );
					System.out.println("coordinate is null " + (coordinates == null));
				} 
				catch(java.lang.NumberFormatException e){
					System.out.println("Couldn't parse google coordinates for address " + addressQuery);
				}
				*/
				
				if (latitude != null && longitude != null && !latitude.equals("") && !longitude.equals("") && !latitude.equals("[]") && !longitude.equals("[]") ){
					try {
						System.out.println("get coords loc.put");
						loc = new BasicDBObject();
						loc.put("Longitude", Double.parseDouble(longitude));
						loc.put("Latitude", Double.parseDouble(latitude));
						
					} 
					catch(java.lang.NumberFormatException e){
						System.out.println("Couldn't parse google coordinates for address " + addressQuery);
					}
				}
				//log.info(getUserId() + ": Mark Geolocaiton " + coordinates.toString());
				
				
			}
		}
		return loc;
	}
	

	
	

	@Override
	public String EnrichItem(HttpClient httpclient, String addressQuery) {

		log.trace( "EnrichItem:  " + addressQuery);

		String retVal = null;

        HttpGet httpget = new HttpGet(APIEndPoint + addressQuery);
		try {
			HttpResponse response = httpclient.execute(httpget);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        int httpStatus = response.getStatusLine().getStatusCode();
	        if (httpStatus == 200) {
	        	retVal = responseHandler.handleResponse(response);
	        }
	        if (httpStatus == 202) {
	    		log.info("There was no data found for " + addressQuery + " on Google, request queued");
	        	retVal = "QD";
	        }
	        else if (httpStatus == 404) {
	    		log.info("There was no data found for " + addressQuery + " on Google");
	    		retVal = "NF";
	        }
	        else if (httpStatus == 500) {
	    		log.info("Google Internal Server Error, report this to Google support");
	        }
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			log.info( "EnrichItem:  " + addressQuery);
			log.debug("Enricher Error: ", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.info( "EnrichItem:  " + addressQuery);
			log.debug("Enricher Error: ", e);
		} finally {
			httpget.releaseConnection();
		}
        return retVal;
	}

}
