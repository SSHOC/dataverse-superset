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

import eu.sshoc.dataversesuperset.readers.Reader;
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
import org.springframework.web.server.ResponseStatusException;

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
			dataInfo.reader = Reader.createReader(entity, httpClient);

			dataInfo.reader.analyzeColumns(entity, dataInfo);
		}
	}
}
