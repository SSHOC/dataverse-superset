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

import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataInfo {
	
	public final String siteUrl;
	public final String fileId;
	public final String fileUrl;
	public String fileName;
	public String fileSize;
	public List<ColumnInfo> columns = new ArrayList<>();
	
	public volatile long datasetId;
	
	public volatile Status status = Status.READY;
	public volatile String error;
	
	public DataInfo(String siteUrl, String fileId, String fileUrl) {
		this.siteUrl = siteUrl;
		this.fileId = fileId;
		this.fileUrl = fileUrl;
	}
	
	public String getName() {
		return getName(fileUrl);
	}
	
	public static String getName(String fileUrl) {
		return "dataverse_" + DigestUtils.md5DigestAsHex((fileUrl).getBytes()).substring(0, 10);
	}
	
	public static class ColumnInfo {
		public String name;
		public ColumnType type;
		
		public ColumnInfo(String name, ColumnType type) {
			this.name = name;
			this.type = type;
		}
	}
	
	public enum ColumnType {
		BOOLEAN,
		INTEGER,
		FLOATING,
		DATE,
		TIME,
		DATETIME,
		TEXT
	}
	
	public enum Status {
		READY,
		IN_PROGRESS,
		COMPLETE,
		ERROR
	}
	
	public static Map<ColumnType, ValueParser<?>> VALUE_PARSERS = Stream.of(
			new ValueParser<>(ColumnType.BOOLEAN, Boolean::valueOf,
					v -> "true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v)),
			new ValueParser<>(ColumnType.INTEGER, Integer::valueOf),
			new ValueParser<>(ColumnType.FLOATING, Double::valueOf),
			new ValueParser<>(ColumnType.DATE, Date::valueOf),
			new ValueParser<>(ColumnType.TIME, Time::valueOf),
			new ValueParser<>(ColumnType.DATETIME, Timestamp::valueOf),
			new ValueParser<>(ColumnType.TEXT, String::valueOf))
			.collect(Collectors.toMap(p -> p.columnType, p -> p, (u, v) -> u, LinkedHashMap::new));
	
	public static class ValueParser<T> {
		Predicate<String> matcher;
		Function<String, T> converter;
		public final ColumnType columnType;
		
		public ValueParser(ColumnType columnType, Function<String, T> converter) {
			this(columnType, converter, v -> {
				try {
					converter.apply(v);
					return true;
				} catch (Exception e) {
					return false;
				}
			});
		}
		
		public ValueParser(ColumnType columnType, Function<String, T> converter, Predicate<String> matcher) {
			this.columnType = columnType;
			this.converter = converter;
			this.matcher = matcher;
		}
		
		public boolean matches(String value) {
			return StringUtils.hasText(value) && matcher.test(value);
		}
		
		public T parse(String value) {
			return converter.apply(value);
		}
	}
}
