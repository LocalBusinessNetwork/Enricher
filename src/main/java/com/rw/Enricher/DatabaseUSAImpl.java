package com.rw.Enricher;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.ResourceBundle;
import java.util.Set;

import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.entity.*;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.params.HttpParams;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class DatabaseUSAImpl extends AgencyBaseImpl {
	public static String APIEndPoint = ResourceBundle.getBundle("Enricher").getString("DbUsaAPIURL");
	static final Logger log = Logger.getLogger(DatabaseUSAImpl.class.getName());

	static final String setSearchSpecURL = "http://www.datairis.co/V1/criteria/search/addall/consumer/?DatabaseType=consumer";
	static final String GetResultsURL = "http://www.datairis.co/V1/search/consumer/?DatabaseType=consumer&Start=1&End=10";
	static final String GetCountURL =  "http://www.datairis.co/V1/search/count/consumer/?DatabaseType=consumer";
	static final String clearSearchSpecURL = "http://www.datairis.co/V1/criteria/search/deleteall/consumer/?DatabaseType=consumer";
		
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

		
		// 1. Rule 1: FirstName, LastName and Phone Number are mandatory for dbUsa.
		
		String FirstName = bc.GetFieldValue("firstName").toString();
		String LastName = bc.GetFieldValue("lastName").toString();

		if ( FirstName == null || FirstName.isEmpty() || LastName == null || LastName.isEmpty() )
			return bc;
		
		// 2. Rule 2: There has to be at least one Zip to find the address
		Set<String> zips = heuristicZip(bc);
		if ( zips.isEmpty() ) return bc;

		// Good, we got all the inputs we need.
		// So, Authenticate dbUsa.
		String TokenID  = GetAuthTokenId(httpclient);
		log.trace( "DatabaseUsa Auth Token:  " + TokenID);

		if ( TokenID.equals("NF") )
			return bc;
		
		// Got the Auth Token..
		
		for (String zip : zips) {
			ClearSearchSpec(httpclient, TokenID);
			SetSearchSpec(httpclient,TokenID, FirstName, LastName, zip);
			int recs = GetRecordCount(httpclient, TokenID);
			if ( recs == 1 ) {
				String fc_enrichmentData = EnrichItem(httpclient, TokenID);
				if ( !fc_enrichmentData.equals("NF") ) {
		        	JSONArray fields = (JSONArray) JsonPath.read(fc_enrichmentData, "$.Response.responseDetails.SearchResult.searchResultRecord[0].resultFields");
		    		log.debug( "DBUsa:EnrichParty: For " + FirstName + " " + LastName + " : " + fields.toString());
        			try {
			        	for ( int i = 0; i < fields.size(); i++ ) {
			        		JSONObject field = (JSONObject) fields.get(i);
			        		String fieldName = field.get("fieldName").toString();
			        		String fieldValue = field.get("fieldValue").toString();
			        		
				        		if (fieldName.equals("Physical_Address")) {
										bc.SetFieldValue("streetAddress1_work", fieldValue);
				        		}
				        		else if (fieldName.equals("Physical_City")) {
									bc.SetFieldValue("cityAddress_work", fieldValue);
				        		}
				        		else if (fieldName.equals("Physical_State")) {
				    				bc.SetFieldValue("stateAddress_work", fieldValue);
				    			        			
				        		}
				        		else if (fieldName.equals("Physical_Zip")) {
				    				bc.SetFieldValue("postalCodeAddress_work", fieldValue);
				        		}
			        		
			        	}
						bc.SetFieldValue("wp_enriched", "YES");
				        bc.SaveRecord();
			        	return bc;
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else {
					try {
			    		log.debug( "DBUsa:EnrichParty: For " + FirstName + " " + LastName +  ": Data Not Found");
						bc.SetFieldValue("wp_enriched", "NF");
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			        bc.SaveRecord();
				}
			}
			else {
				try {
					if ( recs > 0 ) {
						log.debug( "DBUsa:EnrichParty: For " + FirstName + " " + LastName +  ": Dups" + ": Recs =" + Integer.toString(recs));
						bc.SetFieldValue("wp_enriched", "DUPS");
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        bc.SaveRecord();
			}
		}
		return bc;
	}

	@Override
	public String EnrichItem(HttpClient httpclient, String query) {
		log.trace( "EnrichItem:  " + query);


		HttpGet httpget = new HttpGet(GetResultsURL);
		httpget.addHeader("TokenID", query);
	    httpget.addHeader("Accept", "*/*"); 
	    httpget.addHeader("Accept-Encoding", "gzip,deflate,sdch"); 
	    httpget.addHeader("Accept-Language", "en-US,en;q=0.8"); 

        try {

        	HttpResponse response = httpclient.execute(httpget);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        	String jsonStr  = responseHandler.handleResponse(response);

        	JSONObject responseStr = (JSONObject) JsonPath.read(jsonStr, "$.Response");
			String responseCode = responseStr.get("responseCode").toString();
			String responseMessage = responseStr.get("responseMessage").toString();
			if ( responseCode.equals("200") && responseMessage.equals("Success") ) {
				log.trace( "GetRecordCount : Success");
				return jsonStr;
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
        return "NF";
	}

	public void ClearSearchSpec(HttpClient httpclient, String TokenID) {
		log.trace( "ClearSearchSpec");

		HttpDelete httpdelete = new HttpDelete(clearSearchSpecURL);
		httpdelete.addHeader("TokenID", TokenID);
		httpdelete.addHeader("Accept", "*/*"); 
		httpdelete.addHeader("Accept-Encoding", "gzip,deflate,sdch"); 
		httpdelete.addHeader("Accept-Language", "en-US,en;q=0.8"); 

        try {

        	HttpResponse response = httpclient.execute(httpdelete);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        	String jsonStr  = responseHandler.handleResponse(response);

        	JSONObject responseStr = (JSONObject) JsonPath.read(jsonStr, "$.Response");
			String responseCode = responseStr.get("responseCode").toString();
			String responseMessage = responseStr.get("responseMessage").toString();
			if ( responseCode.equals("200") && responseMessage.equals("Success") ) {
				log.trace( "ClearSearchSpec : Success");
			}

        } catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} finally {
			httpdelete.releaseConnection();
		}

	}	

	public int GetRecordCount(HttpClient httpclient, String TokenID) {
		log.trace( "GetRecordCount");

		HttpGet httpget = new HttpGet(GetCountURL);
		httpget.addHeader("TokenID", TokenID);
	    httpget.addHeader("Accept", "*/*"); 
	    httpget.addHeader("Accept-Encoding", "gzip,deflate,sdch"); 
	    httpget.addHeader("Accept-Language", "en-US,en;q=0.8"); 

        try {

        	HttpResponse response = httpclient.execute(httpget);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        	String jsonStr  = responseHandler.handleResponse(response);

        	JSONObject responseStr = (JSONObject) JsonPath.read(jsonStr, "$.Response");
			String responseCode = responseStr.get("responseCode").toString();
			String responseMessage = responseStr.get("responseMessage").toString();
			if ( responseCode.equals("200") && responseMessage.equals("Success") ) {
				log.trace( "GetRecordCount : Success");
				JSONObject responseDetails = (JSONObject) responseStr.get("responseDetails");
				return Integer.parseInt(responseDetails.get("SearchCount").toString());
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
        
        return 0;
		
	}	

	public void SetSearchSpec(HttpClient httpclient, String TokenID, String FirstName, String LastName, String zip) {
		log.trace( "SetSearchSpec");

		HttpPut httpput = new HttpPut(setSearchSpecURL);
		httpput.addHeader("TokenID", TokenID);
		httpput.addHeader("Content-Type", "application/json");
		httpput.addHeader("Accept", "*/*"); 
		httpput.addHeader("Accept-Encoding", "gzip,deflate,sdch"); 
		httpput.addHeader("Accept-Language", "en-US,en;q=0.8"); 

		JSONObject jsonSearchSpec = new JSONObject();
		jsonSearchSpec.put("Physical_Zip", zip);
		jsonSearchSpec.put("First_Name", FirstName);
		jsonSearchSpec.put("Last_Name", LastName);
		
		try {
			StringEntity se = new StringEntity(jsonSearchSpec.toString());
			httpput.setEntity(se);
		} catch (UnsupportedEncodingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
				
        try {

        	HttpResponse response = httpclient.execute(httpput);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        	String jsonStr  = responseHandler.handleResponse(response);

        	JSONObject responseStr = (JSONObject) JsonPath.read(jsonStr, "$.Response");
			String responseCode = responseStr.get("responseCode").toString();
			String responseMessage = responseStr.get("responseMessage").toString();
			if ( responseCode.equals("200") && responseMessage.equals("Success") ) {
				log.trace( "SetSearchSpec : Success");
				JSONObject responseDetails = (JSONObject) responseStr.get("responseDetails");
			}

        } catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.debug("Enricher Error: ", e);
		} finally {
			httpput.releaseConnection();
		}
		
	}	

	public String GetAuthTokenId(HttpClient httpclient) {
		log.trace( "GetAuthTokenId");
		
		String retVal = null; /* EnrichmentFactory.GetCache("DataBaseUsaTokenId");
		
		if ( retVal != null) { 
			Long TokenExpiryTime = Long.parseLong(EnrichmentFactory.GetCache("DataBaseUsaTokenIdExpiration"));
			Long presentTime = System.currentTimeMillis();
			
			if ( (presentTime - TokenExpiryTime ) < 30*60000 ) {
				return retVal;
			}
		}
		*/
		HttpGet httpget = new HttpGet(APIEndPoint);
		httpget.addHeader("SubscriberUsername", "admin");
		httpget.addHeader("SubscriberPassword", "admin");
	    httpget.addHeader("SubscriberID", "8");
	    httpget.addHeader("AccountID", "11");
	    httpget.addHeader("AccountUsername", "ReferralWireAdmin");
	    httpget.addHeader("AccountPassword", "ReferralWireAdmin");
	    httpget.addHeader("AccountDetailRequired", "True"); 
	    httpget.addHeader("Accept", "*/*"); 
	    httpget.addHeader("Accept-Encoding", "gzip,deflate,sdch"); 
	    httpget.addHeader("Accept-Language", "en-US,en;q=0.8"); 
	        
        try {
			HttpResponse response = httpclient.execute(httpget);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        	String jsonStr  = responseHandler.handleResponse(response);
			JSONObject responseStr = (JSONObject) JsonPath.read(jsonStr, "$.Response");
			String responseCode = responseStr.get("responseCode").toString();
			String responseMessage = responseStr.get("responseMessage").toString();
			if ( responseCode.equals("200") && responseMessage.equals("Success") ) {
				JSONObject responseDetails = (JSONObject) responseStr.get("responseDetails");
				retVal = responseDetails.get("TokenID").toString();
				/*
				EnrichmentFactory.Cache("DataBaseUsaTokenId", retVal);
				Long presentTime = System.currentTimeMillis();
				EnrichmentFactory.Cache("DataBaseUsaTokenIdExpiration", presentTime.toString());
				*/
			}
			else  {
				// there was error on white pages pro. 
				// We might have exceed the quota for this month.
				// This record can be enriched later.
				log.info("Database USA API Error : " + responseCode + ":" + responseMessage);
				return "NF";
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
