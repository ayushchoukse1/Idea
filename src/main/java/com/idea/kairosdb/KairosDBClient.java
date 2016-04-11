package com.idea.kairosdb;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;

import org.kairosdb.client.Client;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.MetricBuilder;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.response.GetResponse;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.Response;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.kairosdb.client.util.Preconditions.checkNotNullOrEmpty;

public class KairosDBClient extends HttpClient{
	public KairosDBClient(String url) throws MalformedURLException {
		super(url);
		// TODO Auto-generated constructor stub
	}

	private static KairosDBClient kairosClient = null;
	
	public static KairosDBClient getInstance(){
		if(kairosClient == null){
			try {
				kairosClient = new KairosDBClient("http://localhost:8080/");
				//kairosClient = new HttpClient("http://localhost:8080/");
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return kairosClient;
	}
	
	public void postNewData(String json, String url){
		try {
			System.out.println(postData(json, url));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
