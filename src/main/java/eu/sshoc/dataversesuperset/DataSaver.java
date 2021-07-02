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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import eu.sshoc.dataversesuperset.DataInfo.ColumnType;

@Component
public class DataSaver {
	
	private final static Map<ColumnType, String> DB_TYPES = Map.of(
			ColumnType.BOOLEAN, "boolean",
			ColumnType.INTEGER, "bigint",
			ColumnType.FLOATING, "numeric(32,32)",
			ColumnType.DATE, "date",
			ColumnType.TIME, "time",
			ColumnType.DATETIME, "timestamp",
			ColumnType.TEXT, "text");
	
	@Autowired
	JdbcTemplate jdbcTemplate;
	@Autowired
	DataLoader dataLoader;
	@Autowired
	Superset superset;
	
	@Transactional
	public String createTable(DataInfo dataInfo) throws IOException {
		String tableName = dataInfo.getName();
		deleteTable(tableName);
		String createTable = "CREATE TABLE " + tableName + " ("
				+ dataInfo.columns.stream().map(c -> c.name + " " + DB_TYPES.get(c.type))
						.collect(Collectors.joining(", "))
				+ ")";
		jdbcTemplate.execute(createTable);
		
		String insertValues = "INSERT INTO " + tableName + " VALUES ("
				+ dataInfo.columns.stream().map(c -> "?").collect(Collectors.joining(", "))
				+ ")";
		try (CloseableHttpResponse response = dataLoader.openCsv(dataInfo);
				CSVParser csv = dataLoader.createParser(response.getEntity())) {
			Iterator<CSVRecord> it = csv.iterator();
			List<Object[]> rows = new ArrayList<>();
			int batchLimit = 9000;
			int columnCount = dataInfo.columns.size();
			while (it.hasNext()) {
				CSVRecord record = it.next();
				Object[] row = new Object[columnCount];
				for (int i = 0; i < columnCount; i++) {
					ColumnType type = dataInfo.columns.get(i).type;
					row[i] = DataInfo.VALUE_PARSERS.get(type).parse(record.get(i));
				}
				rows.add(row);
				if (rows.size() % batchLimit == 0 || !it.hasNext()) {
					jdbcTemplate.batchUpdate(insertValues, rows);
				}
			}
		}
		return tableName;
	}
	
	public void deleteTable(String tableName) {
		jdbcTemplate.execute("DROP TABLE IF EXISTS " + tableName);
	}
	
}
