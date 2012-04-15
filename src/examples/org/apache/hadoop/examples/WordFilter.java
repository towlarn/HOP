package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashSet;

public class WordFilter {
	
	public static final String PUNCTUATION_FILTER_REGEX = "[\"?!()\\[\\];,]";
	
	public static final String WORDLIST = "/home/towlarn/hop/filtered_words.txt"; // word_stem.txt
	private static WordFilter _instance = null;

	public static WordFilter getInstance() {
		if(_instance == null) {
			_instance = new WordFilter();
		}
		return _instance;
	}
	

	/*  instance variables  */

	// list of common words
	final Collection<String> wlist;
	
	protected WordFilter() {
		wlist = new HashSet<String>();
		parsefile();
	}
	
	private void parsefile(){
		try{
			FileInputStream fstream = new FileInputStream(WORDLIST);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;

			while ((line = br.readLine()) != null){
				wlist.add(line);
			}
			in.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public boolean contains(String word){
		return wlist.contains(word);
	}
	
	public static String filterPunctuation(String s){
		String out = s.replaceAll(PUNCTUATION_FILTER_REGEX, "");
		while (out.endsWith(".") || out.endsWith(":"))
			out = out.substring(0, out.length()-1);
		return out;
	}
	
	public static void main(String[] args){
		System.out.println(filterPunctuation("he[]llo.")+"\n");
		WordFilter wf = WordFilter.getInstance();
		for (String s : wf.wlist){
			System.out.println(s);
		}
		System.out.println(wf.wlist.size());
		System.out.println("\n"+wf.contains("the"));
		System.out.println(wf.contains("the "));
		System.out.println(wf.contains("them"));
		System.out.println(wf.contains("the2"));
	}
	
}
