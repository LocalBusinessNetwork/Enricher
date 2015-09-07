package com.rw.Enricher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
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

public class GooglePlusImpl extends AgencyBaseImpl {
	
	public static String APIEndPoint = ResourceBundle.getBundle("Enricher").getString("GOOGLEPeopleSearchAPIURL");
	static final Logger log = Logger.getLogger(GooglePlusImpl.class.getName());

	@Override
	public String EnrichItem(HttpClient httpclient, String queryStr) {
		String retVal = null;
		HttpGet httpget = null;
		try {
			String url = APIEndPoint + 
				       URLEncoder.encode(queryStr,"UTF-8");
			httpget = new HttpGet(url);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        HttpResponse response = httpclient.execute(httpget);
	        int httpStatus = response.getStatusLine().getStatusCode();
	        if (httpStatus == 200) {
	            retVal = responseHandler.handleResponse(response);
	        }
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (httpget != null )
				httpget.releaseConnection();
		}
		return retVal;
	}

}
