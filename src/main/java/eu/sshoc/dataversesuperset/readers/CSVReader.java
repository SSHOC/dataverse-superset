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
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CSVReader extends Reader {
	
	private CSVParser csvParser;
	private Iterator<CSVRecord> it;
	
	protected CSVReader(DataInfo dataInfo, HttpEntity entity) {
		super(dataInfo, entity);
	}
	
	@Override
	public boolean hasNext() {
		return it.hasNext();
	}
	
	@Override
	public List<String> next() {
		List<String> cells = new ArrayList<>();
		Iterator<String> itCol = it.next().iterator();
		for (int i = 0; itCol.hasNext() && i < columns.size(); i++) {
			cells.add(itCol.next());
		}
		return cells;
	}
	
	@Override
	protected void initIterator() throws IOException {
		String contentType = entity.getContentType().getValue();
		if (contentType.startsWith("text/tab-separated-values")) {
			csvParser = CSVFormat.TDF.withFirstRecordAsHeader().parse(new InputStreamReader(entity.getContent()));
		} else if (contentType.startsWith("text/comma-separated-values")) {
			csvParser = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(new InputStreamReader(entity.getContent()));
		} else if (contentType.startsWith("text/semicolon-separated-values")) {
			csvParser = CSVFormat.EXCEL.withDelimiter(';').withFirstRecordAsHeader().parse(new InputStreamReader(entity.getContent()));
		} else {
			logger.warn("received content type: {}", contentType);
			throw new ResponseStatusException(HttpStatus.NOT_FOUND, "file not valid");
		}
		it = csvParser.iterator();
		columns = csvParser.getHeaderNames();
	}
	
	@Override
	public void close() throws IOException {
		csvParser.close();
	}
}
