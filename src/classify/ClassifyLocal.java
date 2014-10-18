package classify;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

import utils.StringDoubleList;
import utils.StringDoubleList.StringDouble;
import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;

public class ClassifyLocal {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {

		ClassifyLocal m = new ClassifyLocal();
		
		//load probability dictionary
		InputStream is = m.getClass().getResourceAsStream("probabilities");
		BufferedReader br = new BufferedReader(new InputStreamReader(is)); //Open text
		HashMap<String, HashMap<String, Double>> labelScores = new HashMap<String, HashMap<String, Double>>();
		String data;
		while ((data = br.readLine()) != null){
        	String[] labelIndices = data.split("\t");
        	StringDoubleList list = new StringDoubleList();
        	list.readFromString(labelIndices[1].toString());
        	//System.out.println(list.toString());
        	HashMap<String, Double> temp = new HashMap<String, Double>();
			for (StringDouble index : list.getIndices()) {    			
				String lemma = index.getString();
				Double prob = index.getValue();
				temp.put(lemma, prob);
			}
			labelScores.put(labelIndices[0], temp);
		}
		br.close();
		
		//get numbers for test data	
		InputStream is2 = m.getClass().getResourceAsStream("test_data");
		BufferedReader br2 = new BufferedReader(new InputStreamReader(is2)); //Open text

		FileWriter fw = new FileWriter("results.txt");
		
		String line;
		while ((line = br2.readLine()) != null){
        	String[] labelIndices = line.split("\t");
        	String name = labelIndices[0].toString();
    		StringIntegerList lemmaCounts = new StringIntegerList();
        	lemmaCounts.readFromString(labelIndices[1].toString());
        	TreeMap<Double,String> guesses = new TreeMap<Double,String>(Collections.reverseOrder());			

	        for (String label : labelScores.keySet()) {
	        	HashMap<String, Double> scores = labelScores.get(label);
	        	Double prob = 0.0; 
	        	//Double prob = Math.log( scores.get("__LABEL__") ); //include label weighting
	        	for (StringInteger index : lemmaCounts.getIndices()) {	
					String lemma = index.getString();
					if (scores.containsKey(lemma)) {
						Integer freq = index.getValue();
						prob += freq * Math.log(scores.get(lemma));
					}
	        	}	        	
	        	guesses.put(prob, label);
	        }
			String[] top3 = new String[3];
			Integer i = 0;
	        for (Double prob: guesses.keySet()){
	        	if (i < 3) {
	        		top3[i] = guesses.get(prob);
	        		i++;
	        	} else {
	        		fw.write(name + " : " + StringUtils.join(top3, ", "));
	        		fw.write("\n");
	        		break;
	        	}
	        }	
		}
	}		
}