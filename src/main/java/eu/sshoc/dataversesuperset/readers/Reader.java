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
package eu.sshoc.dataversesuperset.readers;

import eu.sshoc.dataversesuperset.DataInfo;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public abstract class Reader implements Iterator<List<String>>, Closeable {
	
	protected static Logger logger = LoggerFactory.getLogger(Reader.class);
	
	protected HttpEntity entity;
	
	protected DataInfo dataInfo;
	
	protected List<String> columns = new ArrayList<>();
	
	protected Reader(DataInfo dataInfo, HttpEntity entity) {
		this.entity = entity;
		this.dataInfo = dataInfo;
	}
	
	private static final Set<String> CSV_TAB_CONTENT_TYPES =
			new HashSet<>(Arrays.asList("text/tab-separated-values", "text/comma-separated-values",
					"text/semicolon-separated-values"));
	private static final Set<String> EXCEL_CONTENT_TYPES = new HashSet<>(
			Arrays.asList("application/xls", "application/xlsx", "application/vnd.ms-excel",
					"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"));
	private static final Set<String> OPEN_DOCUMENT_CONTENT_TYPES = new HashSet<>(Arrays.asList("application/ods"));

	public static Reader createReader(DataInfo dataInfo, HttpEntity entity) throws IOException {
		String contentType = entity.getContentType().getValue();
		Reader reader;

		if (CSV_TAB_CONTENT_TYPES.stream().anyMatch(contentType::startsWith)) {
			reader = new CSVReader(dataInfo, entity);
		} else if (EXCEL_CONTENT_TYPES.stream().anyMatch(contentType::startsWith)) {
			reader = new ExcelReader(dataInfo, entity);
		} else if (OPEN_DOCUMENT_CONTENT_TYPES.stream().anyMatch(contentType::startsWith)) {
			reader = new ODSReader(dataInfo, entity);
		} else {
			logger.warn("received content type: {}", contentType);
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "file not valid");
		}
		reader.initIterator();
		return reader;
	}
	
	protected abstract void initIterator() throws IOException;
	
	public List<String> getColumns() {
		return columns;
	}
}
