
import java.io.*;
import java.io.ObjectInputStream.GetField;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;

class FinalTuple implements Serializable{
	private long start;
	private long size;
	
	public FinalTuple(long start, long size) {
		this.start = start;
		this.size = size;
	}
	
	public String toString(){
		return start + "," + size;
	}
	
	public long getStart() {
		return start;
	}

	public long getSize() {
		return size;
	}
}

public class Merge {
	static String dirPath = "/home/sivaram/eclipse/files/AP_DATA/output/";
	static HashMap<String, Tuple> catalog;
	static HashMap<String, FinalTuple> catalogCumulative = new HashMap<String, FinalTuple>();
	static File index = new File(dirPath+"INDEX");
	static File index1 = new File(dirPath+"INDEX1");
	static File[] catFiles = getDirFiles(dirPath+"cat/");
	File[] indexFiles = getDirFiles(dirPath+"index/");
	static RandomAccessFile index_raf;
	static RandomAccessFile new_index_raf;
	static RandomAccessFile cur_index_raf;
	static long start;
	static long end;
	static long size;
	static byte[] buffer;//read in to this from file

	public static void main(String[] args) throws Exception {
		//start merging		
		index_raf = new RandomAccessFile(index, "rw");
		int f = 0;
		
		for(File file: catFiles)
		{		
			new_index_raf = new RandomAccessFile(index1, "rw");
			//read catalog from disk and deserialize
			catalog = readCatalogFile(file);
			HashSet<String> toRemove = new HashSet<String>();
			for(String term: catalog.keySet())
			{//unique terms
				if(!catalogCumulative.containsKey(term)){
					
					//read offsets and get the inv for term
					String read = getTermPosting(term, catalog);
					
					//write to final inv index
					start = new_index_raf.getFilePointer();
					new_index_raf.writeBytes(read+" ");
					end = new_index_raf.getFilePointer();
					
					//update final catalog
					catalogCumulative.put(term, new FinalTuple(start, end-start));
					
					toRemove.add(term);
				}
			}
			
			//delete terms from catalog
			for(String term: toRemove){
				catalog.remove(term);
			}
			
			++f;
			
			if(f==1)
			{
				//close pointers
				new_index_raf.close();
				
				//delete index
				index.delete();
				
				//rename index1 to index
				index1.renameTo(index);
				
				continue;
			}
			
			//now deal with common terms
			for(String term: catalogCumulative.keySet())
			{				
				if(catalog.containsKey(term)){
					
					String read_from_cur = getTermPosting(term, catalog);
					String read_from_cum = getFinalTermPosting(term, catalogCumulative);
					String read = read_from_cum + " " + read_from_cur + " ";
				
					//write to final inv index
					start = new_index_raf.getFilePointer();
					new_index_raf.writeBytes(read);
					end = new_index_raf.getFilePointer();
				
					//update final catalog
					catalogCumulative.put(term, new FinalTuple(start, end-start));
				}
				
				else if(!toRemove.contains(term)) 
				{
					//read offsets and get the inv for term
					String read = getFinalTermPosting(term, catalogCumulative);
					
					//write to final inv index
					start = new_index_raf.getFilePointer();
					new_index_raf.writeBytes(read + " ");
					end = new_index_raf.getFilePointer();
					
					//update final catalog
					catalogCumulative.put(term, new FinalTuple(start, end-start));
				}
			}
			
			//close pointers
			new_index_raf.close();
			
			//delete index
			index.delete();
			
			//rename index1 to index
			index1.renameTo(index);
			
			System.out.println(f+" files done");
		}	
		
		//serialize cat
		File catalog = new File("/home/sivaram/eclipse/files/AP_DATA/output/CATALOG");
	    FileOutputStream fos = new FileOutputStream(catalog);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(catalogCumulative);
        oos.flush();
        oos.close();
        fos.close();
    
        //write cat to file
        RandomAccessFile raf = new RandomAccessFile(new File(dirPath+"cat_readable/CATALOG"),"rw");
		for(String term: catalogCumulative.keySet())
			raf.writeBytes(term+"="+catalogCumulative.get(term).toString()+"\r");
		raf.close();
	}
	
	@SuppressWarnings("unchecked")
	public static HashMap<String, Tuple> readCatalogFile(File inputFile) throws Exception
	{
        FileInputStream fis = new FileInputStream(inputFile);
        ObjectInputStream ois = new ObjectInputStream(fis);
        HashMap<String, Tuple> ct = (HashMap<String, Tuple>)ois.readObject();
        return ct;
	}
	
	public static String getTermPosting(String term, HashMap<String, Tuple> catalog) throws Exception
	{
		String read;
		size = catalog.get(term).getSize();
		buffer = new byte[(int)size];
		start = catalog.get(term).getStart();
		String fname = ""+catalog.get(term).getFname();
		cur_index_raf = new RandomAccessFile(new File(dirPath+"index/index"+fname), "rw");
		cur_index_raf.seek((int)start);
		cur_index_raf.read(buffer);
		read = new String(buffer).replaceAll("[\\t\\n\\r]+","").trim();
		cur_index_raf.close();
		return read;
	}
	
	public static String getFinalTermPosting(String term, HashMap<String, FinalTuple> catalog) throws Exception
	{
		String read;
		size = catalogCumulative.get(term).getSize();
		buffer = new byte[(int)size];
		start = catalog.get(term).getStart();
		cur_index_raf = new RandomAccessFile(new File(dirPath+"INDEX"), "rw");
		cur_index_raf.seek((int)start);
		cur_index_raf.read(buffer);
		read = new String(buffer).replaceAll("[\\t\\n\\r]+","").trim();
		cur_index_raf.close();
		return read;
	}
	
	
	
	public static File[] getDirFiles(String dirPath){
		File file = new File(dirPath);
		File[] files = file.listFiles();
		System.out.println("read "+files.length+" files --successful");
		return files;
	}

}
