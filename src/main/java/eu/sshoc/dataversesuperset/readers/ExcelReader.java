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
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.IOException;
import java.util.Iterator;

public class ExcelReader extends Reader {

	private Workbook myWorkBook = null;
	private Iterator<Row> rowIterator;

	protected ExcelReader(DataInfo dataInfo, HttpEntity entity) {
		super(dataInfo, entity);
	}

	@Override
	public boolean hasNext() {
		return rowIterator.hasNext();
	}

	@Override
	public String[] next() {
		Row row = rowIterator.next();
		Iterator<Cell> cellIterator = row.cellIterator();

		String[] cells = new String[columns.size()];
		for (int i = 0; i < columns.size() && cellIterator.hasNext(); i++) {
			cells[i] = getCellValue(cellIterator.next());
		}
		return cells;
	}

	@Override
	protected void initIterator() throws IOException {
		if (dataInfo.fileName.endsWith(".xlsx")) {
			myWorkBook = new XSSFWorkbook(entity.getContent());
		} else if (dataInfo.fileName.endsWith(".xls")) {
			myWorkBook = new HSSFWorkbook(entity.getContent());
		}

		Iterator<Sheet> sheetIt = myWorkBook.sheetIterator();
		Sheet mySheet = sheetIt.next();
		rowIterator = mySheet.iterator();

		//first row - column names
		if (rowIterator.hasNext()) {
			Iterator<Cell> cellIterator = rowIterator.next().cellIterator();
			while (cellIterator.hasNext()) {
				Cell cell = cellIterator.next();
				columns.add(getCellValue(cell));
			}
		}
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
