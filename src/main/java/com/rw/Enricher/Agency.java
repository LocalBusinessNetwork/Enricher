package com.rw.Enricher;

import org.apache.http.client.HttpClient;

import com.mongodb.BasicDBObject;
import com.rw.persistence.RWJBusComp;

public interface Agency {
	public String EnrichItem(HttpClient httpclient, String query);
	public RWJBusComp EnrichParty(RWJBusComp bc,HttpClient httpclient) throws Exception;
	public BasicDBObject getEnrichmentData(BasicDBObject inputs);
}
