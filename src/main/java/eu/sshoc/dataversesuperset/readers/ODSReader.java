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
import java.util.ArrayList;
import java.util.List;

public class ODSReader extends Reader {
	
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
	public List<String> next() {
		List<String> cells = new ArrayList<>();
		for (int i = 0; i < columns.size(); i++) {
			MutableCell<?> cell = spreadsheet.getSheet(SHEET_NUMBER).getCellAt(i, nRowIndex);
			if (!cell.isEmpty())
				cells.add(getCellValue(cell));
		}
		nRowIndex++;
		return cells;
	}
	
	@Override
	protected void initIterator() throws IOException {
		spreadsheet = new ODPackage(entity.getContent()).getSpreadSheet();
		
		//first row - column names
		for (int nColIndex = 0; ; nColIndex++) {
			MutableCell<?> cell = spreadsheet.getSheet(SHEET_NUMBER).getCellAt(nColIndex, 0);
			if (cell.isEmpty())
				break;
			columns.add(nColIndex, getCellValue(cell));
		}
		nRowIndex = 1;
	}
	
	private static String getCellValue(MutableCell<?> cell) {
		return ODValueType.forObject(cell.getValue()).format(cell.getValue());
	}
	
	@Override
	public void close() throws IOException {
		//don't need to close anything
	}
}
