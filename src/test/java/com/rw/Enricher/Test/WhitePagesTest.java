package com.rw.Enricher.Test;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import au.com.bytecode.opencsv.CSVReader;

import com.mongodb.BasicDBObject;
import com.rw.Enricher.EnrichmentFactory;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RandomDataGenerator;

public class WhitePagesTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() throws Exception {
		
		RWJApplication app = new RWJApplication();
		
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		BasicDBObject query = new BasicDBObject();
		query.put("emailAddress", "phil@referralwiretest.biz"); 
		
        int nRecs = bc.UpsertQuery(query);
        
		CSVReader reader;
		try {
			reader = new CSVReader(new FileReader("src/test/resources/com/rw/Enricher/testdata.csv"));
		
			String[] nextLine;
			nextLine = reader.readNext();

			EnrichmentFactory ef = new EnrichmentFactory();
		   
			int i = 0;

			while ((nextLine = reader.readNext()) != null) {
				i++;
				
				if ( i > 200 ) break;
				if ( i < 100 ) continue;
				
				// nextLine[] is an array of values from the line
				String FirstName =  nextLine[0];
				String LastName = nextLine[1];
				String mobilePhone = nextLine[2];

				bc.SetFieldValue("firstName", FirstName);
				bc.SetFieldValue("lastName", LastName);
				bc.SetFieldValue("mobilePhone", mobilePhone);
				bc.SaveRecord();
	    		ef.EnrichParty(bc.GetFieldValue("_id").toString());

			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

	//@Test
	public void simpleTest() {

		HttpClient httpclient = new DefaultHttpClient();				

		HttpGet httpget = new HttpGet("http://www.datairis.co/V1/auth/subscriber/?AccessToken=t8");        
		
		httpget.addHeader("SubscriberUsername", "admin");
		httpget.addHeader("SubscriberPassword", "admin");
	    httpget.addHeader("SubscriberID", "8");
	    httpget.addHeader("AccountID", "11");
	    httpget.addHeader("AccountUsername", "ReferralWireAdmin");
	    httpget.addHeader("AccountPassword", "ReferralWireAdmin");
	    // httpget.addHeader("Content-Type", "application/json"); 
	    httpget.addHeader("AccountDetailRequired", "True"); 
	    httpget.addHeader("Accept", "*/*"); 
	    httpget.addHeader("Accept-Encoding", "gzip,deflate,sdch"); 
	    httpget.addHeader("Accept-Language", "en-US,en;q=0.8"); 
	    HttpResponse response;
		try {
			response = httpclient.execute(httpget);
			ResponseHandler<String> responseHandler = new BasicResponseHandler();
			String retVal = responseHandler.handleResponse(response);
			
			System.out.println(retVal);
			
		} catch (ClientProtocolException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			httpget.releaseConnection();
		}

	}
}
