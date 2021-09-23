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

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.sshoc.dataversesuperset.DataInfo;
import eu.sshoc.dataversesuperset.DataLoader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

import eu.sshoc.dataversesuperset.DataInfo.ColumnInfo;
import eu.sshoc.dataversesuperset.DataInfo.ColumnType;
import eu.sshoc.dataversesuperset.DataInfo.ValueParser;
import org.springframework.web.server.ResponseStatusException;

public class CSVReader extends Reader {

	protected CSVReader(CloseableHttpClient httpClient) {
		super(httpClient);
	}

	@Override
	public void analyzeColumns(HttpEntity entity, DataInfo dataInfo) throws IOException {
		try (CSVParser csvParser = createCSVParser(entity)) {
			List<CSVRecord> records = new ArrayList<>();
			Iterator<CSVRecord> it = csvParser.iterator();
			Integer rowsToRead = ANALYZE_ROW_LIMIT;
			while (it.hasNext() && rowsToRead-- > 0) {
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

	@Override
	public List<Object[]> createDBInserts(DataInfo dataInfo, DataLoader dataLoader) throws IOException {
		List<Object[]> rows = new ArrayList<>();
		try (CloseableHttpResponse response = openFile(dataInfo);
				CSVParser csv = createCSVParser(response.getEntity())) {
			Iterator<CSVRecord> it = csv.iterator();
			int columnCount = dataInfo.columns.size();
			while (it.hasNext()) {
				CSVRecord record = it.next();
				Object[] row = new Object[columnCount];
				for (int i = 0; i < columnCount; i++) {
					ColumnType type = dataInfo.columns.get(i).type;
					row[i] = DataInfo.VALUE_PARSERS.get(type).parse(record.get(i));
				}
				rows.add(row);
			}
		}
		return rows;
	}

	private CSVParser createCSVParser(HttpEntity entity) throws IOException {
		String contentType = entity.getContentType().getValue();
		if (contentType.startsWith("text/tab-separated-values"))
			return CSVFormat.TDF.withFirstRecordAsHeader().parse(new InputStreamReader(entity.getContent()));
		else if (contentType.startsWith("text/comma-separated-values"))
			return CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(new InputStreamReader(entity.getContent()));
		throw new ResponseStatusException(HttpStatus.NOT_FOUND, "file not valid");
	}
}
