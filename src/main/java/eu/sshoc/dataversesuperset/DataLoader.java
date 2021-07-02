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
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.http.HeaderElement;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;

import eu.sshoc.dataversesuperset.DataInfo.ColumnInfo;
import eu.sshoc.dataversesuperset.DataInfo.ColumnType;
import eu.sshoc.dataversesuperset.DataInfo.ValueParser;

@Component
public class DataLoader {
	
	@Autowired
	private Logger logger;
	
	private CloseableHttpClient httpClient;
	
	public DataLoader() {
		httpClient = HttpClients.createDefault();
	}
	
	public void loadMetadata(DataInfo dataInfo) throws IOException {
		HttpGet httpGet = new HttpGet(dataInfo.fileUrl);
		try (CloseableHttpResponse response = httpClient.execute(httpGet)) {
			int statusCode = response.getStatusLine().getStatusCode();
			HttpEntity entity = response.getEntity();
			if (statusCode != HttpStatus.OK.value() || entity == null || entity.getContentType() == null) {
				logger.error("{}: status code {}", dataInfo.fileUrl, statusCode);
				throw new ResponseStatusException(HttpStatus.NOT_FOUND, "file not found");
			}
			String fileName = "unknown-file.tab";
			for (HeaderElement element : response.getFirstHeader("Content-Disposition").getElements()) {
				NameValuePair param = element.getParameterByName("filename");
				if (param != null)
					fileName = param.getValue();
			}
			dataInfo.fileName = fileName;
			dataInfo.fileSize = FileUtils.byteCountToDisplaySize(entity.getContentLength());
			
			try (CSVParser parser = createParser(entity)) {
				analyzeColumns(parser, dataInfo);
			}
		}
	}
	
	public CloseableHttpResponse openCsv(DataInfo dataInfo) throws IOException {
		HttpGet httpGet = new HttpGet(dataInfo.fileUrl);
		CloseableHttpResponse response = httpClient.execute(httpGet);
		try {
			int statusCode = response.getStatusLine().getStatusCode();
			HttpEntity entity = response.getEntity();
			if (statusCode != HttpStatus.OK.value() || entity == null || entity.getContentType() == null)
				throw new IOException("status code: " + statusCode);
			return response;
		} catch (IOException e) {
			response.close();
			throw e;
		}
	}
	
	public CSVParser createParser(HttpEntity entity) throws IOException {
		String contentType = entity.getContentType().getValue();
		if (contentType.startsWith("text/tab-separated-values"))
			return CSVFormat.TDF.withFirstRecordAsHeader().parse(new InputStreamReader(entity.getContent()));
		logger.warn("received content type: {}", contentType);
		throw new ResponseStatusException(HttpStatus.NOT_FOUND, "file not valid");
	}

	private void analyzeColumns(CSVParser csvParser, DataInfo dataInfo) {
		List<CSVRecord> records = new ArrayList<>();
		Iterator<CSVRecord> it = csvParser.iterator();
		int rowLimit = 500;
		while (it.hasNext() && rowLimit --> 0) {
			records.add(it.next());
		}
		
		List<String> columns = csvParser.getHeaderNames();
		for (String columnName : columns) {
			ColumnType columnType = ColumnType.TEXT;
			for (ValueParser<?> valParser : DataInfo.VALUE_PARSERS.values()) {
				boolean allMatch = records.stream()
						.map(r -> r.get(columnName))
						.allMatch(v -> !StringUtils.hasLength(v) || valParser.matches(v));
				if (allMatch) {
					columnType = valParser.columnType;
					break;
				}
			}
			dataInfo.columns.add(new ColumnInfo(columnName, columnType));
		}
	}
}
