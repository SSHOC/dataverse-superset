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
import org.apache.http.impl.client.CloseableHttpClient;
import org.jopendocument.dom.ODPackage;
import org.jopendocument.dom.ODValueType;
import org.jopendocument.dom.spreadsheet.MutableCell;
import org.jopendocument.dom.spreadsheet.SpreadSheet;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ODSReader extends ExcelReader {

    private static final int SHEET_NUMBER = 0;

    protected ODSReader(CloseableHttpClient httpClient) {
        super(httpClient);
    }

    @Override
    protected List<List<String>> readDocument(DataInfo dataInfo, InputStream is) throws IOException {
        ODPackage pack = new ODPackage(is);
        SpreadSheet spreadsheet = pack.getSpreadSheet();
        MutableCell cell = null;

        //first row - column names
        cellNoToName = new HashMap<Integer, String>();
        for(int nColIndex = 0;; nColIndex++)
        {
            cell = spreadsheet.getSheet(SHEET_NUMBER).getCellAt(nColIndex, 0);
            if(cell.isEmpty()) break;
            cellNoToName.put(nColIndex, getCellValue(cell));
        }

        List<List<String>> records = new ArrayList<>();
        //iterates through rows
        for(int nRowIndex = 1;; nRowIndex++)
        {
            List<String> cells = new ArrayList<>();
            for(int nColIndex = 0;; nColIndex++)
            {
                cell = spreadsheet.getSheet(SHEET_NUMBER).getCellAt(nColIndex, nRowIndex);
                if(cell.isEmpty()) break;
                cells.add(getCellValue(cell));
            }
            if(cell.getX() == 0) break;
            records.add(cells);
        }
        return records;
    }

    private static String getCellValue(MutableCell cell) {
        return ODValueType.forObject(cell.getValue()).format(cell.getValue());
    }
}
