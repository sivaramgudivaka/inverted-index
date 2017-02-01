import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;


public class QueryManager {

	public static void main(String[] args) throws Exception {
		
		String dirPath = "/home/sivaram/eclipse/files/AP_DATA/";
		String stopWordsPath = "/home/sivaram/eclipse/files/AP_DATA/stoplist.txt";		

		HashSet<String> stopWords = new HashSet<String>(getStopWords(stopWordsPath));
		
		//read catalog from disk and deserialize
		File toRead = new File(dirPath+"output/CATALOG");
        FileInputStream fis = new FileInputStream(toRead);
        ObjectInputStream ois = new ObjectInputStream(fis);
        HashMap<String, FinalTuple> catalog = (HashMap<String, FinalTuple>)ois.readObject();
        
       //read doc_len_map from disk and deserialize
       toRead = new File(dirPath+"output/doc_len_map");
       fis = new FileInputStream(toRead);
       ois = new ObjectInputStream(fis);
       HashMap<Integer, Integer> doc_len_map = (HashMap<Integer, Integer>)ois.readObject();
       
       //read id_to_docname_map from disk and deserialize
       toRead = new File(dirPath+"output/id_to_docname_map");
       fis = new FileInputStream(toRead);
       ois = new ObjectInputStream(fis);
       HashMap<Integer, String> id_to_docname_map = (HashMap<Integer, String>)ois.readObject();
       
       //read index
       File index = new File(dirPath+"output/INDEX");
       
       //read queryfile and generate qMap
       HashMap<String, ArrayList<String>> qMap = new HashMap<String, ArrayList<String>>() ;
       File query = new File(dirPath+"query_desc.51-100.short.txt");

       PorterStemmer stemmer = new PorterStemmer();
       List<String> lines = FileUtils.readLines(query);
       String[] terms;
       for(String line: lines)
       {
    	   String qNo = line.substring(0, 3).trim();
    	   if(qNo.endsWith("."))
           	qNo = qNo.substring(0, qNo.length()-1);
    	   terms = line.substring(4).trim().split(" ");
    	   ArrayList<String> qTermSet = new ArrayList<String>();
    	   ArrayList<String> terms_as_list = new ArrayList<String>(Arrays.asList(terms));
    	   terms_as_list.removeAll(stopWords);
    	   for(String term: terms_as_list){
    		   String term2 = stemmer.stem(term.toLowerCase());
    		   qTermSet.add(term2);
    	   }
    	   qMap.put(qNo, qTermSet);
       }
       
       //run models
       tf_idf(qMap, catalog, index, id_to_docname_map, doc_len_map);
       okapi_bm25(qMap, catalog, index, id_to_docname_map, doc_len_map);
       unigram_laplace(qMap, catalog, index, id_to_docname_map, doc_len_map);
       proximity_search(qMap, catalog, index, id_to_docname_map, doc_len_map);
	}
	
	public static List<String> getStopWords(String stopWordsFile) throws IOException{
		return FileUtils.readLines(new File(stopWordsFile));
	}
	
	public static void tf_idf(HashMap<String, ArrayList<String>> qMap, 
			HashMap<String, FinalTuple> catalog, File index, HashMap<Integer, String> id_to_docname_map,
			 HashMap<Integer, Integer> doc_len_map) throws Exception{
		int doc_freq;
		int D = 84678;
		double avg_doc_len = 263.537884693;
		for(String qNo: qMap.keySet())
		 {
			 HashMap<String, Double> doc_score_map = new HashMap<String, Double>();
			 for(String qTerm: qMap.get(qNo))
			 {
				 double term_score = 0;//okapi_tf(w,d)
				 HashMap<Integer, String[]> tifo = getDocsAndPos(qTerm, catalog, index);
				 //now get docno, tf for the current term
				 //run retrieval model here and get score for each term
				 doc_freq = tifo.size();
				 for (int docid : tifo.keySet())
				 {	
					 String docNo = id_to_docname_map.get(docid);
				     int term_freq = tifo.get(docid).length;
				     int doc_len = doc_len_map.get(docid);
				     term_score = Math.log(D/doc_freq)*(term_freq/(term_freq+0.5+(1.5*doc_len/avg_doc_len)));
				     if(doc_score_map.containsKey(docNo))
				    	 doc_score_map.put(docNo, doc_score_map.get(docNo)+term_score);
				     else
				    	 doc_score_map.put(docNo, term_score);
				}
			 }
			 //calculate results by sorting docs
			writeToFile(qNo, "tf_idf.txt", doc_score_map);
		}
		System.out.println("retrieval finished using tf-idf");
	}
	
	public static void okapi_bm25(HashMap<String, ArrayList<String>> qMap, 
			HashMap<String, FinalTuple> catalog, File index, HashMap<Integer, String> id_to_docname_map,
			 HashMap<Integer, Integer> doc_len_map) throws Exception{
		int doc_freq;
		int D = 84678;
		double k1 = 1.2;
		float k2 = 100;
		double b = 0.75;
		double avg_doc_len = 263.537884693;
		for(String qNo: qMap.keySet())
		 {
			 HashMap<String, Double> doc_score_map = new HashMap<String, Double>();
			 for(String qTerm: qMap.get(qNo))
			 {
				 double term_score = 0;
				 HashMap<Integer, String[]> tifo = getDocsAndPos(qTerm, catalog, index);
				 int tf_in_query = Collections.frequency(qMap.get(qNo), qTerm);
				 //now get docno, tf for the current term
				 //run retrieval model here and get score for each term
				 doc_freq = tifo.size();
				 tf_in_query = 1;
				 for (int docid : tifo.keySet())
				 {	
					 String docNo = id_to_docname_map.get(docid);
				     int term_freq = tifo.get(docid).length;
				     int doc_len = doc_len_map.get(docid);
				     double term1 = Math.log((D+0.5)/(doc_freq+0.5));
				     double term2 = (1+k1)*term_freq/(term_freq+(k1*((1-b)+(b*doc_len/avg_doc_len))));
				     double term3 = (1+k2)*tf_in_query/(tf_in_query+k2);
				     term_score = term1 * term2 * term3;
				     if(doc_score_map.containsKey(docNo))
				    	 doc_score_map.put(docNo, doc_score_map.get(docNo)+term_score);
				     else
				    	 doc_score_map.put(docNo, term_score);
				}
			 }
			 //calculate results by sorting docs
			writeToFile(qNo, "okapi_bm25.txt", doc_score_map);
		}
		System.out.println("retrieval finished using okapi_bm25");
	}
	
	public static void unigram_laplace(HashMap<String, ArrayList<String>> qMap, 
			HashMap<String, FinalTuple> catalog, File index, HashMap<Integer, String> id_to_docname_map,
			 HashMap<Integer, Integer> doc_len_map) throws Exception{
		
		int vocabulary = catalog.size();
		HashMap<String, HashMap<Integer, String[]>> qTermPostings = new HashMap<String, HashMap<Integer, String[]>>();

		for(String qNo: qMap.keySet())//per line
		 {
			 HashMap<String, Double> doc_score_map = new HashMap<String, Double>();
			 HashSet<Integer> CumltvMap = new HashSet<Integer>();
			 
			 
			 //generate cumulative hashmap for all the qTerms
			 for(String qTerm: qMap.get(qNo))//qterms				 
			 {
				 if(qTermPostings.containsKey(qTerm))
					 continue;
				 HashMap<Integer, String[]> hm = getDocsAndPos(qTerm, catalog, index);
				 qTermPostings.put(qTerm, hm);
				 CumltvMap.addAll(hm.keySet());
			 }				 
			 
			 //calculate scores now
			 for (int docid : CumltvMap)
			  {
				 String docNo = id_to_docname_map.get(docid);
				 double term_score = 0;
				 for(String qTerm: qMap.get(qNo))
				 {	
					 HashMap<Integer, String[]> tifo = qTermPostings.get(qTerm);
					 int term_freq = 0;
					 if(tifo.containsKey(docid))
						 term_freq = tifo.get(docid).length;
					 
				     double doc_len = doc_len_map.get(docid);
				     term_score = Math.log((term_freq+1)/(doc_len+vocabulary));
				     if(doc_score_map.containsKey(docNo))
				    	 doc_score_map.put(docNo, doc_score_map.get(docNo)+term_score);
				     else
				    	 doc_score_map.put(docNo, term_score);
				}
			 }
			 //calculate results by sorting docs
			writeToFile(qNo, "unigram_laplace.txt", doc_score_map);
		}
		System.out.println("retrieval finished using unigram_laplace");
	}
	
	public static void proximity_search(HashMap<String, ArrayList<String>> qMap, 
			HashMap<String, FinalTuple> catalog, File index, HashMap<Integer, String> id_to_docname_map,
			 HashMap<Integer, Integer> doc_len_map) throws Exception{
		
		int vocabulary = catalog.size();
		int C = 1500;
		HashMap<String, HashMap<Integer, String[]>> qTermPostings = new HashMap<String, HashMap<Integer, String[]>>();

		for(String qNo: qMap.keySet())//per line
		 {
			 HashMap<String, Double> doc_score_map = new HashMap<String, Double>();
			 HashSet<Integer> CumltvMap = new HashSet<Integer>();			 
			 HashMap<Integer, ArrayList<String[]>> docIdPos = new HashMap<Integer, ArrayList<String[]>>();
			 
			 //generate cumulative hashmap for all the qTerms
			 for(String qTerm: qMap.get(qNo))//qterms				 
			 {
				 if(qTermPostings.containsKey(qTerm))
					 continue;
				 HashMap<Integer, String[]> hm = getDocsAndPos(qTerm, catalog, index);
				 qTermPostings.put(qTerm, hm);
				 CumltvMap.addAll(hm.keySet());
			 }		 
			 
			 //calculate scores now
			 for (int docid : CumltvMap)
			  {
				 for(String qTerm: qMap.get(qNo))
				 {	
					 HashMap<Integer, String[]> tifo = qTermPostings.get(qTerm);
					 if(tifo.containsKey(docid))//qTerm exists in docid
					 {
						 if(docIdPos.containsKey(docid))
							 docIdPos.get(docid).add(tifo.get(docid));//append pos's
						 else{
							 ArrayList<String[]> temp = new ArrayList<String[]>();
							 temp.add(tifo.get(docid));
							 docIdPos.put(docid, temp);//create map and insert pos's
						 }
					 }
				 }
			  }
			 
			 for(int docid: docIdPos.keySet())
			 {
				 int numTerms = docIdPos.get(docid).size();
				 if(numTerms<2)
					 continue;
				 String docNo = id_to_docname_map.get(docid);
				 double doc_len = doc_len_map.get(docid);
				 double doc_score = (C - getLeastSpan(docid, docIdPos.get(docid))) * numTerms / (doc_len + vocabulary);
				 doc_score_map.put(docNo, doc_score);
			 }

			 //calculate results by sorting docs
			writeToFile(qNo, "proximity.txt", doc_score_map);
		}
		System.out.println("retrieval finished using proximity model");
	}	
	
	
	public static HashMap<Integer, String[]> getDocsAndPos(String term, HashMap<String, FinalTuple> catalog, File index) throws Exception{
		HashMap<Integer, String[]> docsAndPos = new HashMap<Integer, String[]>();
		long start = catalog.get(term).getStart();
		long size = catalog.get(term).getSize();
		RandomAccessFile raf = new RandomAccessFile(index, "rw");
		raf.seek(start);
		byte[] buffer = new byte[(int)size];
		raf.read(buffer);
		String[] docs = new String(buffer).trim().split(" ");
		for(String doc: docs)
		{
			String[] temp = doc.split("=");
			String[] pos = temp[1].split(",");
			docsAndPos.put(Integer.parseInt(temp[0]), pos);
		}
		raf.close();
		return docsAndPos;
	}
	
	public static Map sortByValue(Map unsortMap) {	 
		List list = new LinkedList(unsortMap.entrySet());
	 
		Collections.sort(list, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Comparable) ((Map.Entry) (o2)).getValue())
							.compareTo(((Map.Entry) (o1)).getValue());
			}
		});
	 
		Map sortedMap = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
	
	public static void writeToFile(String qNo, String fileName, HashMap<String, Double> doc_score_map) throws IOException {
		//initialize writers and buffers
		File file = new File("/home/sivaram/eclipse/files/AP_DATA/output/"+fileName);

		
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		raf.seek(raf.length());	
       
		//start writing
		 int i = 1;
		 Map set = sortByValue(doc_score_map);
		 for(Object key: set.keySet()){
			 if(i > 1000)
				 break;
			 raf.writeBytes(qNo + " Q0 " + key.toString() + " " + i + " "  + set.get(key) + " Exp"+"\n");
			 i++;
		 }	 
		 
		//close open buffers
		raf.close();
	}
	
	public static int getLeastSpan(int docid, ArrayList<String[]> rows) throws Exception
	{
		HashSet<Integer> deadArray = new HashSet<Integer>();
		HashMap<Integer, Integer> cur_pos = new HashMap<Integer, Integer>();
		
		HashSet<Integer> range = new HashSet<Integer>();
		ArrayList<Integer> window = new ArrayList<Integer>();
		
		int span;
		int min_array = -1;
		int  min_window = 0 , max_window;
		int i = 0;
		
		while(true)
		{		
			boolean b = true;
			if(i != 0)
			{
				//if all the arrays are dead, exit
				for(int j = 0; j < rows.size(); j++)
					b = (b && (cur_pos.get(j)==rows.get(j).length-1));
				
				if(b)
					break;
				
				if(deadArray.contains(min_array))
				{
					int temp = 99999;
					for(int j = 0; j < rows.size(); j++)
					{
						if(deadArray.contains(j))
							continue;
						//possible candidate
						if(Integer.parseInt(rows.get(j)[cur_pos.get(j)]) <= temp)
						{
							temp = Integer.parseInt(rows.get(j)[cur_pos.get(j)]);
							min_array = j;
						}
					}
					
					window.remove(new Integer(temp));
					window.add(Integer.parseInt(rows.get(min_array)[cur_pos.get(min_array)+1]));
					cur_pos.put(min_array, cur_pos.get(min_array)+1);
				}
				else
				{		
					int temp_min = Integer.parseInt(rows.get(min_array)[cur_pos.get(min_array)+1]);//5
					//move the window 1 step right
					//1. remove current min from window
					window.remove(new Integer(min_window));
					//2. add new element to window
					window.add(Integer.parseInt(rows.get(min_array)[cur_pos.get(min_array)+1]));
					
					cur_pos.put(min_array, cur_pos.get(min_array)+1);
					
					for(int j = 0; j < rows.size(); j++)
					{
						if(cur_pos.get(j)==rows.get(j).length-1)
							deadArray.add(j);
					}
					
					//update min_array
					for(int j : cur_pos.keySet())
					{
						if(Integer.parseInt(rows.get(j)[cur_pos.get(j)]) <= temp_min)
						{
							temp_min = Integer.parseInt(rows.get(j)[cur_pos.get(j)]);
							min_array = j;
						}
					}
				}
			}
			
			if(i==0)				
			//fill window
			for(int j = 0; j < rows.size(); j++)
			{
				int pos = Integer.parseInt(rows.get(j)[0]);
				window.add(pos);
				cur_pos.put(j, 0);
				if(Collections.min(window) == pos)
					min_array = j;//index of array having least pos value.
			}
			
			min_window = Collections.min(window);
			max_window = Collections.max(window);			
			span = max_window-min_window;
			
			if(span == rows.size()-1)
				return span;
			else
				range.add(span);				
			
			//remove dead end arrays
			for(int j = 0; j < rows.size(); j++)
			{
				if(cur_pos.get(j)==rows.get(j).length-1)
					deadArray.add(j);
			}
			i++;
		}
		return Collections.min(range);
	}
}
