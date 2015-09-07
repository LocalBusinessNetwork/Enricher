package com.rw.Enricher;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
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
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class FullContactImpl extends AgencyBaseImpl {
	
	public static String APIEndPoint = ResourceBundle.getBundle("Enricher").getString("FullContactPersonAPIURL");
	static final Logger log = Logger.getLogger(FullContactImpl.class.getName());

	@Override
	public RWJBusComp EnrichParty(RWJBusComp bc,HttpClient httpclient){
	
		log.trace( "EnrichParty: For " + bc.GetFieldValue("_id").toString());

		Object emailAddressObject = bc.GetFieldValue("emailAddress");
		if ( emailAddressObject != null ) {
			String emailAddress = emailAddressObject.toString();
			if (emailAddress != null && emailAddress.contains("@")){
				
				// skip those that have already been enhanced.
				Object fc_enrichedObj = bc.GetFieldValue("fc_enriched"); 
				if ( fc_enrichedObj != null ) {
					String fc_enriched  = fc_enrichedObj.toString();
					if ( fc_enriched.equals("NF") || fc_enriched.equals("YES"))
							return bc;
				}

				String fc_enrichmentData = EnrichItem(httpclient, emailAddress);
				try {
				
				if (fc_enrichmentData != null ) {
					if ( fc_enrichmentData.equals("NF") || fc_enrichmentData.equals("QD") ) {
							bc.SetFieldValue("fc_enriched", fc_enrichmentData);
					}
					else {		
						bc = setFieldVal(bc,fc_enrichmentData,"photoUrl","$.photos[0].url",null,true);
						bc = setFieldVal(bc,fc_enrichmentData,"linkedInPage","$.socialProfiles[?].url",Filter.filter(Criteria.where("type").is("linkedin")),true);
						bc.SetFieldValue("fc_enriched", "YES");
					}
				}

				} catch (ParseException e) {
					// TODO Auto-generated catch block
					log.debug("Enricher Error: ", e);
				}
				
			}
		}
		return bc;
		
	}

	@Override
	public String EnrichItem(HttpClient httpclient, String emailAddress) {

		log.trace( "EnrichItem:  " + emailAddress);

		String retVal = null;

        HttpGet httpget = new HttpGet(APIEndPoint + emailAddress);
		try {
			HttpResponse response = httpclient.execute(httpget);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        int httpStatus = response.getStatusLine().getStatusCode();
	        if (httpStatus == 200) {
	        	retVal = responseHandler.handleResponse(response);
	        }
	        if (httpStatus == 202) {
	    		log.info("There was no data found for " + emailAddress + " on FullContact, request queued");
	        	retVal = "QD";
	        }
	        else if (httpStatus == 404) {
	    		log.info("There was no data found for " + emailAddress + " on FullContact");
	    		retVal = "NF";
	        }
	        else if (httpStatus == 500) {
	    		log.info("FullContact Internal Server Error, report this to FullContact support");
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
