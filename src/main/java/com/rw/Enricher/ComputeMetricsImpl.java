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
import org.json.JSONObject;

import com.jayway.jsonpath.Criteria;
import com.jayway.jsonpath.Filter;
import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.QueryBuilder;
import com.rw.persistence.RWAlgorithm;
import com.rw.persistence.RWJApplication;
import com.rw.persistence.RWJBusComp;
import com.rw.persistence.mongoStore;
import com.rw.repository.ALGORITHM;

public class ComputeMetricsImpl extends AgencyBaseImpl {
	
	static final Logger log = Logger.getLogger(ComputeMetricsImpl.class.getName());

	static final String[] memberScores = {"memberProfileScore", "speakerProfileScore", "ambassadorProfileScore" };
	static final String[] memberBadges = {"GoodMemeberBadge", "PopularSpeakerBadge" };
	static final String[] memberStats = {"totalInvitations", "totalProspects" };
	
	@Override
	public RWJBusComp EnrichParty(RWJBusComp bc,HttpClient httpclient) throws Exception{
	
		log.trace( "Compute Metrics EnrichParty: For " + bc.GetFieldValue("_id").toString());
		
		for ( int i = 0; i < memberScores.length; i++) {
			double score = getScore(bc.currentRecord, memberScores[i]);
			bc.SetFieldValue(memberScores[i],score);
		}
		
		for ( int i = 0; i < memberBadges.length; i++) {
			boolean gotIt = getBadge(bc.currentRecord, memberBadges[i]);
			if ( gotIt ) {
				bc.SetFieldValue(memberBadges[i],"YES");
			}
		}
		
		for ( int i = 0; i < memberStats.length; i++) {
			boolean gotIt = getStats(bc.currentRecord, memberBadges[i]);
			if ( gotIt ) {
				bc.SetFieldValue(memberStats[i],"YES");
			}
		}
		return bc;
		
	}
	
	double getScore(BasicDBObject entity, String met) throws Exception {
		double score = 0L;
		mongoStore m = new mongoStore();
		BasicDBObject query = new BasicDBObject();
		query.put("name", met);
		BasicDBObject ref = (BasicDBObject) m.getColl("rwDQRule").findOne(query);
		RWAlgorithm a = new RWAlgorithm();
		score = a.completeness(entity, ref, ALGORITHM.SUM);
		return score;
	}

	boolean getBadge(BasicDBObject entity, String badge) throws Exception {
		
		// TODO: 
		return false;
	}

	boolean getStats(BasicDBObject entity, String stat) throws Exception {
		return false;
	}

}
