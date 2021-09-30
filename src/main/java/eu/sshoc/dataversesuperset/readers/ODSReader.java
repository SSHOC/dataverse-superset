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
import org.jopendocument.dom.ODPackage;
import org.jopendocument.dom.ODValueType;
import org.jopendocument.dom.spreadsheet.MutableCell;
import org.jopendocument.dom.spreadsheet.SpreadSheet;

import java.io.IOException;

public class ODSReader extends ExcelReader {

	private static final int SHEET_NUMBER = 0;

	private SpreadSheet spreadsheet;
	private int nRowIndex;

	protected ODSReader(DataInfo dataInfo, HttpEntity entity) {
		super(dataInfo, entity);
	}

	@Override
	public boolean hasNext() {
		return !spreadsheet.getSheet(SHEET_NUMBER).getCellAt(0, nRowIndex).isEmpty();
	}

	@Override
	public String[] next() {
		MutableCell cell;
		String[] cells = new String[columns.size()];
		for (int i = 0; ; i++) {
			cell = spreadsheet.getSheet(SHEET_NUMBER).getCellAt(i, nRowIndex);
			if (cell.isEmpty())
				break;
			cells[i] = getCellValue(cell);
		}
		nRowIndex++;
		return cells;
	}

	@Override
	protected void initIterator() throws IOException {
		spreadsheet = new ODPackage(entity.getContent()).getSpreadSheet();
		MutableCell cell;

		//first row - column names
		for (int nColIndex = 0; ; nColIndex++) {
			cell = spreadsheet.getSheet(SHEET_NUMBER).getCellAt(nColIndex, 0);
			if (cell.isEmpty())
				break;
			columns.add(nColIndex, getCellValue(cell));
		}
		nRowIndex = 1;
	}

	private static String getCellValue(MutableCell cell) {
		return ODValueType.forObject(cell.getValue()).format(cell.getValue());
	}
}
