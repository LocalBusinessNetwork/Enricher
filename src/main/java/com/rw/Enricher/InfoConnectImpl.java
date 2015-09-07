package com.rw.Enricher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.Set;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class InfoConnectImpl extends AgencyBaseImpl {
	public static String APIEndPoint = ResourceBundle.getBundle("Enricher").getString("InfoConnectPersonAPIURL");
	static final Logger log = Logger.getLogger(InfoConnectImpl.class.getName());

	@Override
	public RWJBusComp EnrichParty(RWJBusComp bc,HttpClient httpclient) throws Exception{

		log.trace( "EnrichParty: For " + bc.GetFieldValue("_id").toString());

		// if this record was already enriched from wp, just return.
		Object enrichedObj = bc.GetFieldValue("wp_enriched"); 
		if ( enrichedObj != null ) {
			String enriched  = enrichedObj.toString();
			if ( enriched.equals("NF") || enriched.equals("YES"))
					return bc;
		}

		// Let us start with NF
		
		try {
			bc.SetFieldValue("wp_enriched", "NF");
		} catch (ParseException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// Rule 1: If this was already sync'd before, use the id based sync.		
		if ( SyncById(bc,httpclient) ) return bc;
		

		// Rule 2: FirstName, LastName and Phone Number are mandatory for dbUsa.		
		String FirstName = bc.GetFieldValue("firstName").toString();
		String LastName = bc.GetFieldValue("lastName").toString();

		if ( FirstName == null || FirstName.isEmpty() || LastName == null || LastName.isEmpty() ) {
        	log.info("First Name or Last Name is missing");
			return bc;
		}

		
		// Rule 3: Without a proper zipcode, wp can't locate the party.
		Set<String> zips = heuristicZip(bc);
		if ( zips.isEmpty() ) {
        	log.info("No Zipcode Match: " + FirstName + " " + LastName);
			return bc;
		}

		JSONObject jsonSearchSpec = new JSONObject();
		jsonSearchSpec.put("FirstName", FirstName);
		jsonSearchSpec.put("LastName", LastName);

		JSONArray zipCodesArray = new JSONArray();
		int i = 0 ;
		int j = zips.size();
		
		String  fc_enrichmentData = null;
		
		for (String zip : zips) {
			zipCodesArray.add(zip);
			i++; j--;
			if ( i >= 99 || j == 0  ) {
				jsonSearchSpec.put("PostalCode", zipCodesArray);
				fc_enrichmentData = EnrichItem(httpclient, jsonSearchSpec.toString());
				if (fc_enrichmentData == null )  break;	// this is non 200 status.			
				JSONArray results = (JSONArray) JsonPath.read(fc_enrichmentData, "$");
				if ( results.size() > 0) {
					
					if ( results.size() > 1 )
						log.info("Multiple Matches found for : " + FirstName + " " + LastName);
				
					for (int k = 0; k < results.size(); k++) {
	        			JSONObject jso = (JSONObject) results.get(k);
	        			
	        			String Address = jso.containsKey("Address")? jso.get("Address").toString() : "";
	        			String City = jso.containsKey("City")? jso.get("City").toString() : "";
	        			String StateProvince = jso.containsKey("StateProvince")? jso.get("StateProvince").toString() : "";
	        			String PostalCode = jso.containsKey("PostalCode")? jso.get("PostalCode").toString() : "";
	        			String Phone = jso.containsKey("Phone")? jso.get("Phone").toString() : "";
	        			
	    	        	log.info(FirstName + "," + LastName + "," 
	    	        			+ Address+ "," + City+ "," + StateProvince+
	    	        			"," + PostalCode + "," + Phone);
		        	}
					
					JSONObject jso = (JSONObject) results.get(0);
        			
        			String Address = jso.containsKey("Address")? jso.get("Address").toString() : "";
        			String City = jso.containsKey("City")? jso.get("City").toString() : "";
        			String StateProvince = jso.containsKey("StateProvince")? jso.get("StateProvince").toString() : "";
        			String PostalCode = jso.containsKey("PostalCode")? jso.get("PostalCode").toString() : "";
        			String Phone = jso.containsKey("Phone")? jso.get("Phone").toString() : "";
        			String AddressCounty = jso.containsKey("County")? jso.get("County").toString() : "";
           			String SyncId = jso.containsKey("Id")? jso.get("Id").toString() : "";
          			String GPSlocation = jso.containsKey("location")? jso.get("location").toString() : "";
          			String AgeRange = jso.containsKey("AgeRange")? jso.get("AgeRange").toString() : "";
        			String AreChildrenPresent = jso.containsKey("AreChildrenPresent")? jso.get("AreChildrenPresent").toString() : "";
        			String Gender = jso.containsKey("Gender")? jso.get("Gender").toString() : "";
        			String HeadOfHousehold = jso.containsKey("HeadOfHousehold")? jso.get("HeadOfHousehold").toString() : "";
        			String HomeValueRange = jso.containsKey("HomeValueRange")? jso.get("HomeValueRange").toString() : "";
        			String IncomeRange = jso.containsKey("IncomeRange")? jso.get("IncomeRange").toString() : "";
        			String IsHomeowner = jso.containsKey("IsHomeowner")? jso.get("IsHomeowner").toString() : "";
           			String MaritalStatus = jso.containsKey("MaritalStatus")? jso.get("MaritalStatus").toString() : "";
           			String WealthFinder = jso.containsKey("WealthFinder")? jso.get("WealthFinder").toString() : "";
           			String YearsInHome = jso.containsKey("YearsInHome")? jso.get("YearsInHome").toString() : "";

          			
        			try {
						bc.SetFieldValue("streetAddress1_work", Address);
		       			bc.SetFieldValue("cityAddress_work", City);
	        			bc.SetFieldValue("stateAddress_work", StateProvince);
	        			bc.SetFieldValue("postalCodeAddress_work", PostalCode);
	        			bc.SetFieldValue("postalCodeAddress_work", PostalCode);
	        			bc.SetFieldValue("county_work", AddressCounty);
						bc.SetFieldValue("wp_enriched", "YES");
						bc.SetFieldValue("GPDlocation", GPSlocation);
						bc.SetFieldValue("INFOCONNECT_SYNC_ID", SyncId);
	        			bc.SetFieldValue("gender", Gender);
	        			bc.SetFieldValue("AgeRange", AgeRange);
	        			bc.SetFieldValue("AreChildrenPresent", AreChildrenPresent);
	        			bc.SetFieldValue("HeadOfHousehold", HeadOfHousehold);
	        			bc.SetFieldValue("HomeValueRange", HomeValueRange);
	        			bc.SetFieldValue("IncomeRange", IncomeRange);
	        			bc.SetFieldValue("IsHomeowner", IsHomeowner);
	        			bc.SetFieldValue("MaritalStatus", MaritalStatus);
	        			bc.SetFieldValue("WealthFinder", WealthFinder);
	        			bc.SetFieldValue("YearsInHome", YearsInHome);

					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
 					
				}
				zipCodesArray.clear();
				i = 0;
				jsonSearchSpec.remove("PostalCode");
				}
			if ( j == 0 ) break;
		}		
		return bc;
	}

	public Boolean SyncById(RWJBusComp bc,HttpClient httpclient) {
	
		Object SyncIdObj = bc.GetFieldValue("INFOCONNECT_SYNC_ID");
		if ( SyncIdObj == null || SyncIdObj.toString().isEmpty() ) return false;
		
		JSONArray IdArray = new JSONArray();
		IdArray.add(SyncIdObj.toString());
		
		JSONObject jsonSearchSpec = new JSONObject();
		jsonSearchSpec.put("Id", IdArray);
		
		String fc_enrichmentData = EnrichItem(httpclient, jsonSearchSpec.toString());
		if (fc_enrichmentData == null )  return false;	// this is non 200 status.			

		JSONArray results = (JSONArray) JsonPath.read(fc_enrichmentData, "$");

		JSONObject jso = (JSONObject) results.get(0);
		
		String Address = jso.containsKey("Address")? jso.get("Address").toString() : "";
		String City = jso.containsKey("City")? jso.get("City").toString() : "";
		String StateProvince = jso.containsKey("StateProvince")? jso.get("StateProvince").toString() : "";
		String PostalCode = jso.containsKey("PostalCode")? jso.get("PostalCode").toString() : "";
		String Phone = jso.containsKey("Phone")? jso.get("Phone").toString() : "";
		
		String FirstName = bc.GetFieldValue("firstName").toString();
		String LastName = bc.GetFieldValue("lastName").toString();

		log.info(FirstName + "," + LastName + "," 
    			+ Address+ "," + City+ "," + StateProvince+
    			"," + PostalCode + "," + Phone);
		
		try {
			bc.SetFieldValue("streetAddress1_work", Address);
   			bc.SetFieldValue("cityAddress_work", City);
			bc.SetFieldValue("stateAddress_work", StateProvince);
			bc.SetFieldValue("postalCodeAddress_work", PostalCode);
			bc.SetFieldValue("homePhone", Phone);
			bc.SetFieldValue("wp_enriched", "YES");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
	}
	
	@Override
	public String EnrichItem(HttpClient httpclient, String query) {
		log.trace( "EnrichItem:  " + query);
		String retVal = null;
        
		HttpPost httppost = new HttpPost(APIEndPoint);
		httppost.addHeader("Content-Type", "application/json");

		try {
			StringEntity se = new StringEntity(query);
			httppost.setEntity(se);

			HttpResponse response = httpclient.execute(httppost);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        
	        int httpStatus = response.getStatusLine().getStatusCode();
       		
	        if (httpStatus == 200) {
	        	retVal = responseHandler.handleResponse(response);
	        }
	        
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} finally {
			httppost.releaseConnection();
		}
        return retVal;
	}			
}
