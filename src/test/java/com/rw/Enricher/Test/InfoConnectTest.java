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

public class InfoConnectTest {

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
			reader = new CSVReader(new FileReader("src/test/resources/com/rw/Enricher/testdata3.csv"));
		
			String[] nextLine;
			nextLine = reader.readNext();

			EnrichmentFactory ef = new EnrichmentFactory();
		   
			int i = 0;

			while ((nextLine = reader.readNext()) != null) {
				i++;
				
				//if ( i > 150 ) break;
				//if ( i < 100 ) continue;
				
				// nextLine[] is an array of values from the line
				String FirstName =  nextLine[0];
				String LastName = nextLine[1];
				String mobilePhone = nextLine[2];

				bc.SetFieldValue("firstName", FirstName);
				bc.SetFieldValue("lastName", LastName);
				bc.SetFieldValue("mobilePhone", mobilePhone);
				bc.SetFieldValue("wp_enriched", "NO");
				bc.SetFieldValue("INFOCONNECT_SYNC_ID", "");
				
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

	}
