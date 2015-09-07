package com.rw.Enricher;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;

public class AgencyBaseImpl implements Agency {
	
	static final Logger log = Logger.getLogger(AgencyBaseImpl.class.getName());

	@Override
	public RWJBusComp EnrichParty(RWJBusComp bc,HttpClient httpclient) throws Exception{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String EnrichItem(HttpClient httpclient, String query) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public BasicDBObject getEnrichmentData(BasicDBObject inputs) {
		// TODO Auto-generated method stub
		return null;
	}

	RWJBusComp setFieldVal(RWJBusComp bc, String jsonData, String fieldName,String jsonPath,Filter f,boolean conditionalUpdate) {
		log.trace( "setFieldVal: For " + fieldName );

		String fieldVal = getFieldVal(jsonData,jsonPath,f);
		Object existingVal = bc.GetFieldValue(fieldName);

		String lastSix = (fieldVal != null)?fieldVal.substring(fieldVal.length()-6):"null";
		if (conditionalUpdate){
			if (fieldVal != null && fieldVal != "" && lastSix != null && lastSix.equals("141893") == false && lastSix.equals("06dc20") == false && existingVal != null && existingVal.toString().equals("")){
				log.trace("last 6 " + lastSix);
				try {
					bc.SetFieldValue(fieldName, fieldVal);
				} catch (ParseException e) {
					log.debug("Enricher Error: ", e);
				}
			};
		} else {
			try {
				bc.SetFieldValue(fieldName, fieldVal);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		return bc;
	}
	
	String getFieldVal(String jsonData,String jsonPath,Filter f) {
		String fieldVal = null;
		if (jsonData != null && jsonData.equals("") == false && jsonData.equals("NF") == false & jsonData.equals("QD") == false){
			try {
				if (f == null){
					Object results =JsonPath.read(jsonData, jsonPath);
					
					if (results != null){
						if (results.getClass().toString().equals("class java.lang.String")){
							fieldVal = (String)results;
							
						}
						else {
							fieldVal = results.toString();
							
						}
						
					}
					
					//System.out.println("results class = " + results.getClass());
					//ArrayList ary = JsonPath.read(jsonData, jsonPath);
					//fieldVal = ary.get(0).toString();
					
				}
				else {
					ArrayList ary = JsonPath.read(jsonData, jsonPath,f);
					if (ary.size()>0){
						fieldVal = ary.get(0).toString();
					}
					
				}
			} catch (InvalidPathException e) {
				log.debug("Enricher Error: ", e);
			}
			//
			
		}
		return fieldVal;
		
	}
	

	
	public String heuristicName(RWJBusComp bc) {
		// these are mandatory field for white pages.
		Object lastName = bc.GetFieldValue("lastName");
		
		if( lastName == null )	return "";
		
		String query = "lastname=" + lastName.toString() + ";";
		
		// optional search fields
		Object firstName = bc.GetFieldValue("firstName");
		if( firstName != null )
		{
			query = query + "firstname=" + firstName.toString() + ";";
		}

		return query;

	}

	public Set<String> heuristicZip(RWJBusComp bc) throws Exception {
		
		Set<String> Zips = new HashSet<String>();
		
		// 1. See if the party has the zipcode 
		Object zipObject = bc.GetFieldValue("postalCodeAddress_work");
		if( zipObject != null )
		{
			Zips.add(zipObject.toString());
		}

		// 2. Try to find zip from the mobilePhone areacode
		String phone = bc.GetFieldValue("mobilePhone").toString();
		if (!phone.isEmpty()) {
			// map phone area code to a zip code
			Zips.addAll(mapAreaCodeToZips(phone));
		}

		// 3. If this is a contact, try to find them from the parent record.
		String partytype = bc.GetFieldValue("partytype").toString();
		if(partytype.equals("CONTACT") ) {
			try {
				
				String parentId = bc.GetFieldValue("parentId").toString();
				RWJApplication app = new RWJApplication();
				RWJBusComp bc2 = app.GetBusObject("Party").getBusComp("Party");
				BasicDBObject query = new BasicDBObject();
				query.put("_id", new ObjectId(parentId)); 
				int nRecs = bc2.ExecQuery(query);
				
				// Parent of a contact has to be a PARTNER.
				// hence the recursive call. In any case, be defensive.
				
				if ( bc2.GetFieldValue("partytype").toString().equals("PARTNER"))
					Zips.addAll(heuristicZip(bc2));
			}
			catch ( Exception e) {
				log.debug("Enricher Error: ", e);
			}
		}

		return Zips;
	}

	public static Set<String>  getZipMap(String areaCode) throws Exception {
	   	RWJApplication l_app = new RWJApplication();
		RWJBusComp bc = l_app.GetBusObject("Zip").getBusComp("Zip");

		BasicDBObject query = new BasicDBObject();
		
		query.put("npa", areaCode);
		int nRecs = bc.ExecQuery(query);
		
		Set<String> zips = new HashSet<String>();
				
		for ( int i =0; i < nRecs; i++) {
			zips.add(bc.GetFieldValue("zip").toString());
			bc.NextRecord();
		}
		return zips;
	}
	
	public String extractAreaCode(String phone) {
		
		String areaCode= new String();
		for (int i =0; i < phone.length(); i++) {
			char c = phone.charAt(i);
			if ( c >= '0' && c <= '9') {
				areaCode += c;
			}
		}
		
		if ( areaCode.length() == 10)
			return areaCode.substring(0, 3);

		if ( areaCode.length() == 11)
			return areaCode.substring(1, 4);
		
		return null;
	}
	public Set<String> mapAreaCodeToZips(String phone) throws Exception {
		String areacode = extractAreaCode(phone);
		return getZipMap(areacode);
	}
	
}
