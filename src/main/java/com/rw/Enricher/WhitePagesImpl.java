package com.rw.Enricher;

import java.io.IOException;
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
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class WhitePagesImpl extends AgencyBaseImpl {
	public static String APIEndPoint = ResourceBundle.getBundle("Enricher").getString("WhitePagesAPIURL");
	static final Logger log = Logger.getLogger(WhitePagesImpl.class.getName());

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

		// 1. Rule 1: LastName is a mandatory field for look up.
		String nameQuery = heuristicName(bc);
		if (nameQuery.isEmpty()) return bc;

		// 2. Rule 2: Without a proper zipcode, wp can't locate the party.
		Set<String> zips = heuristicZip(bc);
		if ( zips.isEmpty() ) return bc;
		String fc_enrichmentData = null;
		try {
			for (String zip : zips) {
				String query = nameQuery + "zip=" + zip; 
				fc_enrichmentData = EnrichItem(httpclient, query);
				
				if( fc_enrichmentData == null )
					fc_enrichmentData = "NF" ;
				
				if ( !fc_enrichmentData.equals("NF") && !fc_enrichmentData.equals("QD") )  {
					
					JSONObject listings = (JSONObject) JsonPath.read(fc_enrichmentData, "$.listings");
					if ( listings.size() == 1 ) {
						// an exact match was found.
						// TODO: mark the party that this is an exact match.
						// So that trulia enrichment could be done with confidence.
					}
					else {
						// TODO :  We need to save top 3 matches. Given an option to the use to correct it 
					}
					
					// pick the first one. this is wp's best guess
					JSONObject address = (JSONObject) JsonPath.read(fc_enrichmentData, "$.listings[0].address");
					
					if ( address != null) { 
						// Let us not overwrite what in there already..
						// User might have had the address noted.
						Object existingValue = bc.GetFieldValue("streetAddress1");
						if ( existingValue != null & existingValue.toString().isEmpty() )
							bc.SetFieldValue("streetAddress1", address.get("fullstreet").toString());
						existingValue = bc.GetFieldValue("cityAddress");
						if ( existingValue != null & existingValue.toString().isEmpty() )
							bc.SetFieldValue("cityAddress", address.get("city").toString());
						existingValue = bc.GetFieldValue("stateAddress");
						if ( existingValue != null & existingValue.toString().isEmpty() )
							bc.SetFieldValue("stateAddress", address.get("state").toString());
						existingValue = bc.GetFieldValue("postalCodeAddress");
						if ( existingValue != null & existingValue.toString().isEmpty() )
							bc.SetFieldValue("postalCodeAddress", address.get("zip").toString());
						existingValue = bc.GetFieldValue("country");
						if ( existingValue != null & existingValue.toString().isEmpty() )
							bc.SetFieldValue("country", address.get("country").toString());
					}
					
					/* TODO: this is not coming in alright. need to debug it.
					String phone = (String) JsonPath.read(fc_enrichmentData, "$.listings[0].phonenumbers[0].fullphone");
					if (phone != null) {
						bc.SetFieldValue("homePhone", phone);
					}
					*/
					
					bc.SetFieldValue("wp_enriched", "YES");
					return bc;
				}
			} 
			// none of the zips were able to find this party in wp.
			bc.SetFieldValue("wp_enriched", fc_enrichmentData);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		}
		return bc;
	}

	@Override
	public String EnrichItem(HttpClient httpclient, String query) {
		log.trace( "EnrichItem:  " + query);
		String retVal = null;
        HttpGet httpget = new HttpGet(APIEndPoint + query);
		try {
			HttpResponse response = httpclient.execute(httpget);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        	retVal = responseHandler.handleResponse(response);
			String code = (String) JsonPath.read(retVal, "$.result.code");
			if ( code.equals("No Data Found") ) {
	    		log.info("There was no data found for " + query + " on White Pages Pro");
				return "NF";
			}
			else if ( code.equals("Error") ) {
				
				// there was error on white pages pro. 
				// We might have exceed the quota for this month.
				// This record can be enriched later.
				
				String message = (String) JsonPath.read(retVal, "$.result.message");
				log.info("Whitepages Pro Error : " + message);
				return "QD";
			}
			
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} finally {
			httpget.releaseConnection();
		}
        return retVal;
	}
			
}
