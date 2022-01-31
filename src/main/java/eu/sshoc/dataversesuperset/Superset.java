/*
 * The MIT License
 * 
 * Copyright (c) 2021 SSHOC Dataverse
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package eu.sshoc.dataversesuperset;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.catalina.util.URLEncoder;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class Superset {
	
	@Autowired
	private Map<String, String> supersetConfig;
	
	private CloseableHttpClient httpClient;
	private String accessToken;
	
	public Superset() {
		httpClient = HttpClients.createDefault();
	}
	

	public long createDataset(String tableName) throws IOException {
		refreshToken();
		
		HttpPost post = post("dataset/", accessToken);
		JSONObject json = new JSONObject();
		json.put("database", supersetConfig.get("database-id"));
		json.put("schema", supersetConfig.get("schema"));
		json.put("table_name", tableName);
		StringEntity entity = new StringEntity(json.toString(), "UTF-8");
		entity.setContentType("application/json");
		post.setEntity(entity);
		
		try (CloseableHttpResponse response = httpClient.execute(post)) {
			json = getJson(response, HttpStatus.SC_CREATED);
			return json.getLong("id");
		}
	}
	
	public long findDataset(String tableName) throws IOException {
		refreshToken();
		
		JSONObject json = new JSONObject();
		json.put("columns", new JSONArray(List.of("id")));
		json.put("filters", new JSONArray(List.of(new JSONObject(
				Map.of("col", "table_name",
						"opr", "eq",
						"value", tableName)))));
		HttpGet get = get("dataset/?q=" + new URLEncoder().encode(json.toString(), Charset.forName("UTF-8")));
		
		try (CloseableHttpResponse response = httpClient.execute(get)) {
			json = getJson(response, HttpStatus.SC_OK);
			if (json.getInt("count") == 1) {
				return json.getJSONArray("result").getJSONObject(0).getLong("id");
			}
			return -1;
		}
	}

	public Map<String, String> findChartUrls(long datasourceId) throws IOException {
		refreshToken();

		JSONObject json = new JSONObject();
		json.put("columns", new JSONArray(List.of("slice_name", "url")));
		json.put("filters", new JSONArray(List.of(new JSONObject(
				Map.of("col", "datasource_id",
						"opr", "eq",
						"value", datasourceId)))));
		json.put("order_column", "last_saved_at");
		json.put("order_direction", "desc");
		json.put("page", 0);
		json.put("page_size", 20);
		HttpGet get = get("chart/?q=" + new URLEncoder().encode(json.toString(), Charset.forName("UTF-8")));

		Map<String, String> chartToUrl = new LinkedHashMap<>();
		try (CloseableHttpResponse response = httpClient.execute(get)) {
			json = getJson(response, HttpStatus.SC_OK);
			JSONArray resultArray = json.getJSONArray("result");
			for (int i = 0; i < resultArray.length(); i++) {
				chartToUrl.put(resultArray.getJSONObject(i).getString("slice_name"),
						resultArray.getJSONObject(i).getString("url") + "&standalone=1");
			}
			return chartToUrl;
		}

	}

	private HttpPost post(String path, String bearer) {
		HttpPost post = new HttpPost(URI.create(supersetConfig.get("uri")).resolve("api/v1/" + path));
		post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearer);
		post.setHeader(HttpHeaders.ACCEPT, "application/json");
		return post;
	}
	
	private HttpGet get(String path) {
		HttpGet get = new HttpGet(URI.create(supersetConfig.get("uri")).resolve("api/v1/" + path));
		get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
		get.setHeader(HttpHeaders.ACCEPT, "application/json");
		return get;
	}


	private void refreshToken() throws IOException {
		// TODO check if current access token has expired
		HttpPost post = post("security/refresh", supersetConfig.get("refresh-token"));
		try (CloseableHttpResponse response = httpClient.execute(post)) {
			JSONObject json = new JSONObject(EntityUtils.toString(response.getEntity()));
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				throw new IOException(
						"refresh token problem: " + response.getStatusLine() + " " + json.get("message"));
			}
			accessToken = json.getString("access_token");
		}
	}


	private JSONObject getJson(CloseableHttpResponse response, int expectedStatus) throws IOException {
		JSONObject json;
		if (response.getEntity().getContentType().getValue().startsWith("application/json")) {
			json = new JSONObject(EntityUtils.toString(response.getEntity()));
		} else {
			json = new JSONObject(Map.of("message", EntityUtils.toString(response.getEntity())));
		}
		if (response.getStatusLine().getStatusCode() != expectedStatus) {
			throw new IOException(
					"dataset creation problem: " + response.getStatusLine() + " " + json.get("message"));
		}
		return json;
	}
}
