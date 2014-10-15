package classify;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;

import utils.StringDoubleList;
import utils.StringIntegerList;
import utils.StringIntegerList.StringInteger;

public class scratch {


	public static void main(String[] args) 
			throws IOException, ClassNotFoundException, InterruptedException {

		Map<String, Integer> professionCount = new HashMap<String, Integer>();
		professionCount.put("The Devil", 30);
		
		Text LemmaList = new Text("<pp,1>,<inform,1>,<addit,1>,<hall,1>,<behalf,1>,<johnson,1>,<lee,23>,<constabl,1>,<pamphlet,1>");
		Text profession = new Text("The Devil");
	
		StringIntegerList list = new StringIntegerList();
		list.readFromString(LemmaList.toString());
		
		Map<String, Double> lemmaProb = new HashMap<String, Double>();
		for (StringInteger stringInt : list.getIndices()) {
			String lemma = stringInt.getString();
			Integer count = stringInt.getValue();	
			//System.out.println(lemma);
			double probability = (double)count / professionCount.get(profession.toString());
			lemmaProb.put(lemma, probability);
		}
		
		StringDoubleList sdl = new StringDoubleList(lemmaProb);
		System.out.println(sdl);

		//context.write(profession, sdl);

		
	}
	
}
