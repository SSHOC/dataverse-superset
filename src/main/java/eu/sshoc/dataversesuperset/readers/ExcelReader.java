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
import eu.sshoc.dataversesuperset.DataLoader;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.*;

public class ExcelReader extends Reader {

	protected Map<Integer, String> cellNoToName;

	protected ExcelReader(CloseableHttpClient httpClient) {
		super(httpClient);
	}

	@Override
	public void analyzeColumns(HttpEntity entity, DataInfo dataInfo) throws IOException {
		extractColumns(dataInfo, readDocument(dataInfo, entity.getContent()), cellNoToName);
	}

	@Override
	public List<Object[]> createDBInserts(DataInfo dataInfo, DataLoader dataLoader) throws IOException {
		List<Object[]> rows = new ArrayList<>();

		try (CloseableHttpResponse response = openFile(dataInfo)) {
			int columnCount = dataInfo.columns.size();
			for (List<String> record : readDocument(dataInfo, response.getEntity().getContent())) {
				Object[] row = new Object[columnCount];
				for (int i = 0; i < columnCount; i++) {
					DataInfo.ColumnType type = dataInfo.columns.get(i).type;
					row[i] = DataInfo.VALUE_PARSERS.get(type).parse(record.get(i));
				}
				rows.add(row);
			}
		}
		return rows;
	}

	protected List<List<String>> readDocument(DataInfo dataInfo, InputStream is) throws IOException {
		Workbook myWorkBook = null;
		if (dataInfo.fileName.endsWith(".xlsx")) {
			myWorkBook = new XSSFWorkbook(is);
		} else if (dataInfo.fileName.endsWith(".xls")) {
			myWorkBook = new HSSFWorkbook(is);
		}

		Iterator<Sheet> sheetIt = myWorkBook.sheetIterator();

		Sheet mySheet = sheetIt.next();

		Iterator<Row> rowIterator = mySheet.iterator();

		//first row - column names
		cellNoToName = new HashMap<>();
		if (rowIterator.hasNext()) {
			Iterator<Cell> cellIterator = rowIterator.next().cellIterator();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				cellNoToName.put(cell.getColumnIndex(), getCellValue(cell));
			}
		}

		List<List<String>> records = new ArrayList<>();
		//iterates through rows
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();
			Iterator<Cell> cellIterator = row.cellIterator();

			List<String> cells = new ArrayList<>();
			while (cellIterator.hasNext()) {
				cells.add(getCellValue(cellIterator.next()));
			}
			records.add(cells);
		}
		myWorkBook.close();
		return records;
	}

	private static String getCellValue(Cell cell) {
		switch (cell.getCellType()) {
		case STRING:
		case FORMULA:
			return cell.getStringCellValue();
		case NUMERIC:
			if (cell.getNumericCellValue() == Math.floor(cell.getNumericCellValue())) {
				return Integer.toString((int) Math.floor(cell.getNumericCellValue()));
			}
			return Double.toString(cell.getNumericCellValue());
		case BOOLEAN:
			return Boolean.toString(cell.getBooleanCellValue());
		case ERROR:
			return Byte.toString(cell.getErrorCellValue());
		default:
			return "";
		}
	}
}
