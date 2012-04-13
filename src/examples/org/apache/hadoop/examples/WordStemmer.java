package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordStemmer {

	public static final String WORDLIST = "word_stem_small.txt"; // word_stem.txt
	private static WordStemmer _instance = null;

	public static WordStemmer getInstance() {
		if(_instance == null) {
			_instance = new WordStemmer();
		}
		return _instance;
	}

	/*  instance variables  */

	// maps from: 	word -> root_word
	// e.g. 		dogs -> dog
	private final HashMap<String, String> map;

	protected WordStemmer() {
		map = new HashMap<String, String>();
		parsefile();
	}

	private void parsefile(){
		try{
			FileInputStream fstream = new FileInputStream(WORDLIST);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line;

			String word = "";
			while ((line = br.readLine()) != null){
				if(line.startsWith(" ")){
					String[] split = line.split(",");
					for(int i = 0; i < split.length; i++){
						String s = formatString(split[i].trim());
						map.put(s, word);
					}
				}
				else{
					word = formatString(line);
				}
			}
			in.close();
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	// parse strings with format:
	// thumping -> [thump]
	private String formatString(String s){
		Pattern p = Pattern.compile("(.*)( ->)");
		Matcher m = p.matcher(s);
		if (!m.find() || m.groupCount() < 2){
			return s;
		}
		return m.group(1);
	}
	
	public Map<String, String> getMap(){
		return map;
	}
	
	public static void main(String[] args){
		WordStemmer ws = WordStemmer.getInstance();
		for(Map.Entry<String, String> entry : ws.getMap().entrySet()){
			System.out.println(entry.getKey() + ":\t"+entry.getValue());
		}
	}
}