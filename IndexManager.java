
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.*;


class Tuple implements Serializable{
	private int fNo;
	private long start;
	private long size;
	
	public Tuple(int fNo, long start, long size) {
		this.fNo = fNo;
		this.start = start;
		this.size = size;
	}
	
	public String toString(){
		return fNo + "," + start + "," + size;
	}

	public int getFname() {
		return fNo;
	}
	
	public long getStart() {
		return start;
	}

	public long getSize() {
		return size;
	}
}

public class IndexManager {

	public static void main(String[] args) throws Exception {
		// queries, stopwords file paths
		String stopWordsPath = "/home/sivaram/eclipse/files/AP_DATA/stoplist.txt";
		String dirPath = "/home/sivaram/eclipse/files/AP_DATA/ap89_collection/";
		
		//
		HashSet<String> stopWords = new HashSet<String>(getStopWords(stopWordsPath));
		stopWords.add("");
		stopWords.add(".");
		HashMap<Integer, ArrayList<String>> docsAndPos;
		HashMap<String, HashMap<Integer, ArrayList<String>>> inv_index = new HashMap<String, HashMap<Integer, ArrayList<String>>>();
		HashMap<Integer, Integer> doc_len_map = new HashMap<Integer, Integer>();
		File[] files = getDirFiles(dirPath);//get dir files
		String[] docs;
		String docNo;
		String docText;
		ArrayList<String> tokens;
		ArrayList<String> term_pos;
		
		HashMap<String, Integer> doc_id_map = new HashMap<String, Integer>();
	
		int count = 0;
		int fileNo = 1;
		int docid = 0;
		
		for(int f = 0; f < files.length; f++)
		{
			File file = files[f];
			docs = getDocs(file);
			for(int d = 0; d< docs.length; d++)
			{
				String doc = docs[d];
				docNo = StringUtils.substringBetween(doc, "<DOCNO>", "</DOCNO>").trim();
				doc_id_map.put(docNo, docid);
				docText = String.join("", StringUtils.substringsBetween(doc, "<TEXT>", "</TEXT>"));	
				docText = docText.toLowerCase().replaceAll("[^a-z0-9\\s+\\.-]", "");
				tokens = new ArrayList<String>(Arrays.asList(docText.split("[\\s+-]")));
				tokens = tokenize(tokens);
				tokens.removeAll(stopWords);
				doc_len_map.put(docid, tokens.size());
				int pos = 1;
				//put each term in to inv_index
				for(String term: tokens)
				{	
					if(inv_index.containsKey(term))//old term?
					{
						if(inv_index.get(term).containsKey(docid))//term repeats in same doc?
							inv_index.get(term).get(docid).add(""+pos);
						else//term repeats in another doc?
						{
							term_pos = new ArrayList<String>();
							term_pos.add(""+pos);
							inv_index.get(term).put(docid, term_pos);
						}
					}
					else//first look of term:)
					{
						term_pos = new ArrayList<String>();
						term_pos.add(""+pos);
						docsAndPos = new HashMap<Integer, ArrayList<String>>();
						docsAndPos.put(docid, term_pos);
						inv_index.put(term, docsAndPos);
					}
					++pos;
				}
				count++;
				if(count == 1000||(d==docs.length-1 && f==files.length-1))//dump hashmap to disk
				{
					count = 0;
					writeMapsToFile(inv_index, fileNo);
					++fileNo;
					inv_index = new HashMap<String, HashMap<Integer, ArrayList<String>>>();//free memory
					System.out.println("1 file written");
				}
				++docid;
			}
		}
		
		//serialize doc_len_map
		File map = new File("/home/sivaram/eclipse/files/AP_DATA/output/doc_len_map");
	    FileOutputStream fos = new FileOutputStream(map);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(doc_len_map);
        oos.flush();
        oos.close();
        fos.close();
		
       //serialize doc_id_map
  	   map = new File("/home/sivaram/eclipse/files/AP_DATA/output/doc_id_map");
  	   fos = new FileOutputStream(map);
       oos = new ObjectOutputStream(fos);
       oos.writeObject(doc_id_map);
       oos.flush();
       oos.close();
       fos.close();
       
       HashMap<Integer, String> reverse = new HashMap<Integer, String>();//docid to docno
       for(String docno: doc_id_map.keySet()){
			reverse.put(doc_id_map.get(docno), docno);
		}
       
      //serialize id_to_docname_map
  	   map = new File("/home/sivaram/eclipse/files/AP_DATA/output/id_to_docname_map");
  	   fos = new FileOutputStream(map);
       oos = new ObjectOutputStream(fos);
       oos.writeObject(reverse);
       oos.flush();
       oos.close();
       fos.close();
	}
	
	public static ArrayList<String> tokenize(ArrayList<String> tokens)
	{
		PorterStemmer stemmer = new PorterStemmer();
		String term;
		for(int i = 0;i<tokens.size();i++)
		{
			term = tokens.get(i).trim();
			while(true)
			{
				if(term.endsWith("."))
					term = term.substring(0, term.length()-1);
				else
					break;
			}
				tokens.set(i, stemmer.stem(term));
		}
		return tokens;
	}
	
	public static File[] getDirFiles(String dirPath){
		File file = new File(dirPath);
		File[] files = file.listFiles();
		System.out.println("read "+files.length+" files --successful");
		return files;
	}

	public static List<String> getStopWords(String stopWordsFile) throws IOException{
		return FileUtils.readLines(new File(stopWordsFile));
	}
	
	public static String[] getDocs(File file) throws IOException{
		 String strFromFile = FileUtils.readFileToString(file);//read file contents to string
		 return StringUtils.substringsBetween(strFromFile, "<DOC>", "</DOC>");
	 }
	
	public static String processString(HashMap<Integer, ArrayList<String>> map)
	{
		String read = "";
		for(int docid: map.keySet()){
			read += docid + "=" + String.join(",", map.get(docid)) + " ";
		}
		return read;
	}
	
	public static void writeMapsToFile(HashMap<String, HashMap<Integer, ArrayList<String>>> inv_index, int fNo) throws Exception{
		HashMap<String, Tuple> ct = new HashMap<String, Tuple>();//catalog
		String outPutPath = "/home/sivaram/eclipse/files/AP_DATA/output/";
		//open file
		RandomAccessFile raf = new RandomAccessFile(new File(outPutPath+"index/index"+fNo), "rw");
		for(String term: inv_index.keySet())
		{
			//get current position of file pointer
			long start = raf.getFilePointer();
			raf.writeBytes(processString(inv_index.get(term)));
			long end = raf.getFilePointer();
			//create catalog
			ct.put(term, new Tuple(fNo, start, end-start));
		}
		
		raf.close();
		
		//serialize catalog
		File catalog = new File(outPutPath+"cat/ct"+fNo);
	    FileOutputStream fos = new FileOutputStream(catalog);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(ct);
        oos.flush();
        oos.close();
        fos.close();
	}
}
