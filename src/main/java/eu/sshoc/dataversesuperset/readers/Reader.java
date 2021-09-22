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
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class Reader {

    protected CloseableHttpClient httpClient;

    protected Reader(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public static Reader createReader(HttpEntity entity, CloseableHttpClient httpClient) {
        String contentType = entity.getContentType().getValue();
        if (contentType.startsWith("text/tab-separated-values") || contentType.startsWith("text/comma-separated-values")) {
            return new CSVReader(httpClient);
        } else if(contentType.startsWith("application/xls") || contentType.startsWith("application/xlsx")) {
            return new ExcelReader(httpClient);
        } else if(contentType.startsWith("application/ods")) {
            return new ODSReader(httpClient);
        }
        return null;
    }

    protected void extractColumns(DataInfo dataInfo, List<List<String>> records, Map<Integer, String> cellNoToName) {
        Collection<String> columns = cellNoToName.values();
        for (int i = 0; i < cellNoToName.size(); i++) {
            final int column = i;
            DataInfo.ColumnType columnType = DataInfo.ColumnType.TEXT;
            for (DataInfo.ValueParser<?> valParser : DataInfo.VALUE_PARSERS.values()) {
                boolean allMatch = records.stream()
                        .map(r -> r.get(column))
                        .allMatch(v -> !StringUtils.hasLength(v) || valParser.matches(v));
                if (allMatch) {
                    columnType = valParser.columnType;
                    break;
                }
            }
            dataInfo.columns.add(new DataInfo.ColumnInfo(cellNoToName.get(i), columnType));
        }
    }

    protected CloseableHttpResponse openFile(DataInfo dataInfo) throws IOException {
        HttpGet httpGet = new HttpGet(dataInfo.fileUrl);
        CloseableHttpResponse response = httpClient.execute(httpGet);
        try {
            int statusCode = response.getStatusLine().getStatusCode();
            HttpEntity entity = response.getEntity();
            if (statusCode != HttpStatus.OK.value() || entity == null || entity.getContentType() == null)
                throw new IOException("status code: " + statusCode);
            return response;
        } catch (IOException e) {
            response.close();
            throw e;
        }
    }

    public abstract void analyzeColumns(HttpEntity entity, DataInfo dataInfo) throws IOException;

    public abstract List<Object[]> createDBInserts(DataInfo dataInfo, DataLoader dataLoader) throws IOException;
}
