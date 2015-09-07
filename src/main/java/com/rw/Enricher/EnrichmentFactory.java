package com.rw.Enricher;

import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSONSerializers;
import com.mongodb.util.ObjectSerializer;
import com.rw.persistence.JedisMT;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.RWJBusObj;
import com.rw.persistence.RWObjectMgr;

public class EnrichmentFactory {

	static final Logger log = Logger.getLogger(EnrichmentFactory.class.getName());
	//public final static String[] Agencies = {"FullContact", "WhitePages"};
	//public final static String[] Agencies = {"FullContact", "InfoConnect"};
	//public final static String[] Agencies = {"InfoConnect"};
	public final static String[] Agencies = {"GoogleGeoCode"};

	static JedisMT jedisMt = new JedisMT();
	
	public Agency CreateAgency(String AgencyName) {
		log.trace( "CreateAgency() : " + AgencyName);
		if ( AgencyName.equals("FullContact"))
			return new FullContactImpl();
		if ( AgencyName.equals("WhitePages"))
			return new WhitePagesImpl();
		if ( AgencyName.equals("DbUsa"))
			return new DatabaseUSAImpl();
		if ( AgencyName.equals("InfoConnect"))
			return new InfoConnectImpl();
		if ( AgencyName.equals("GoogleGeoCode"))
			return new GoogleGeoCodeImpl();
		if ( AgencyName.equals("ComputeMetrics"))
			return new ComputeMetricsImpl();
		return null;
	}

	public boolean EnrichParty(String partyId) throws Exception {
		System.out.println("enrich party for " + partyId);
		log.trace( "EnrichParty: For " + partyId);

		RWJApplication app = new RWJApplication(new RWObjectMgr());
		RWJBusComp bc = app.GetBusObject("Party").getBusComp("Party");
		
		//System.out.println("enrich - is bc null " + (bc == null));
		BasicDBObject query = new BasicDBObject();
		
		if ( partyId != null)
			query.put("_id", new ObjectId(partyId)); 
		
        int nRecs = bc.UpsertQuery(query);
        if (nRecs == 0) return true;
        
		Object emailAddressObject = bc.GetFieldValue("emailAddress");
		String partytype = (String)bc.GetFieldValue("partytype");
		if ( emailAddressObject != null || partytype.equals("BUSINESS")  || partytype.equals("BUSINESS_DIR")) {
	        String emailAddress = (String)emailAddressObject;

	        if ((emailAddress != null && emailAddress.contains("@")) || partytype.equals("BUSINESS") || partytype.equals("BUSINESS_DIR")){
				HttpClient httpclient = new DefaultHttpClient();				
				for (int i = 0; i < Agencies.length; i++) {
					Agency a = CreateAgency(Agencies[i]);
					a.EnrichParty(bc,httpclient);
				}

				try {
					bc.SetFieldValue("lastEnrichDate", new Date());
					bc.SaveRecord();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					log.debug(e.getStackTrace());
				}
				//String partytype = bc.GetFieldValue("partytype").toString();
				if ( partytype.equals("PARTNER")) {
					ObjectSerializer s = JSONSerializers.getStrict();
					String dataout = s.serialize(bc.GetCompositeRecord());
					Cache(partyId,dataout );
				}
	        }	
		}
		return true;
	}
	
	public static void Cache(String token, String data) {
		jedisMt.set("data_" + token, data);	
	}

	public static String GetCache(String token) {
		return jedisMt.get("data_" + token);	
	}
}
