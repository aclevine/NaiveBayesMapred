package engineer;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class scoreDevSet {
	
	public static HashMap<String, Set<String>> titleProfessions = new HashMap<String, Set<String>>();
	
	public static void main(String[] args) throws Exception {

		// load gold standard
		scoreDevSet m = new scoreDevSet();		

		InputStream is = m.getClass().getResourceAsStream("dev_test_gold.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line;
        while ((line = br.readLine()) != null) {
        	String[] keyvalue = line.split("\\s+:\\s+");
        	String title = keyvalue[0];
        	String[] profList = keyvalue[1].split(", ");
        	//System.out.println(title);
        	Set<String> professions = new HashSet<String>(Arrays.asList(profList));
        	titleProfessions.put(title, professions);
        }
        br.close();

        // load and test results
        Integer goodCount = 0;
        Integer totalCount = 0;
		InputStream is2 = m.getClass().getResourceAsStream("dev_results.txt");
		BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
		String line2;
        while ((line2 = br2.readLine()) != null) {
        	String[] keyvalue = line2.split("\\s+:\\s+");
        	String title = keyvalue[0];
        	String[] profList = keyvalue[1].split(", ");
        	//System.out.println(title);
        	if (titleProfessions.containsKey(title)) {
        		totalCount ++;
        		Set<String> gold = titleProfessions.get(title);
        		// if any matches, entire record is counted as a good hit
        		for (String profession: profList){
        			if (gold.contains(profession)) {
        				goodCount ++;
        				break;
        			}
        		}
        	}
        }
        br2.close();
        
        System.out.println(goodCount.toString());
        System.out.println(totalCount.toString());        
        Double p = (double)goodCount / totalCount;
        System.out.println(p.toString());        
        
	}
}
