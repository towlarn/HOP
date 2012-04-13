package org.apache.hadoop.examples;

public class PunctuationFilter {
	
	public static final String FILTER = "\"?!()[];:";

	public static String filter(String s){
		return s.replaceAll("[\"?!()\\[\\];:]", "");
	}
	
}
