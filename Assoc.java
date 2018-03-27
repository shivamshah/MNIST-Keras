import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;


class set_obj {
	int[] items;
	double support;
	
	double getSupport(){
		return this.support;
	}
	int[] getItems(){
		return this.items;
	}
	public set_obj(int[] i, double sup){
		items = i;
		support = sup;
	}
	
}

class rule {
	int[] lhs_set;
	int[] rhs_set;
	double sup_lr;
	double sup_rr;
	double sup_rule;
	double confidence_r;
	double lift_r;
	
	
	public rule(int[] lhs, int[] rhs, double sl, double sr, double srule){
		lhs_set = lhs;
		rhs_set = rhs;
		sup_lr = sl;
		sup_rr = sr;
		sup_rule = srule;
		confidence_r = sup_rule/sup_lr ;
		lift_r = sup_rule/(sup_lr*sup_rr);
	}
	
	public rule(){
		this.lhs_set = null;
		this.rhs_set = null;
		this.sup_lr = 0;
		this.sup_rr = 0;
		this.sup_rule = 0;
		this.confidence_r = 0 ;
		this.lift_r = 0;
	}
	
	public void setRule(int[] lhs, int[] rhs, double srule, double cf, double lf){
		this.lhs_set = lhs;
		this.rhs_set = rhs;
		this.sup_rule = srule;
		this.confidence_r = cf;
		this.lift_r = lf;
		this.sup_lr = this.sup_rule/this.confidence_r;
		this.sup_rr = this.sup_rule/(this.sup_lr*this.lift_r);
		
	}
	
}
public class Assoc {

	
	static List<Set<Integer>> data = new ArrayList<Set<Integer>>();  
	static double minSupport = .1;
	static int numTxns = 0;
	static int numUniqueItems = 0;
	static Set<Integer> uniqueItems = new HashSet<Integer>();
	static double avgItemPerTxn = 0;
	static double minlift = 1;
	static double minConfidence = .1;
	static HashMap<Integer,Double> TicketSize = new HashMap<Integer,Double>();
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		 	
//		String  p = "C:/Users/ss45797/workspace/Association/Datasets/retail_data.csv";
		String p1 = "C:/Users/ss45797/Documents/BestBuy/MRE/Data/input-cob.csv";
		
//		String p1 = "C:/Users/ss45797/Documents/BestBuy/MRE/Data/temp1.csv";
		// s c l --> v1 (1,1,1)  v2 (1,50,2) v3(1,1,1) v4(1,20,1)
		long SS = System.currentTimeMillis();
		File file = new File("sample10_v4.csv");
		FileOutputStream fis = null;
		try {
			fis = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrintStream out = new PrintStream(fis);
		System.setOut(out);

		long startTime = System.currentTimeMillis();
		readCSV_1(p1);
		long endTime = System.currentTimeMillis();
		long seconds = (endTime - startTime);
		
		
		numTxns = data.size();
		System.out.println(numTxns);
		
		//String p4 = "C:/Users/ss45797/Documents/BestBuy/MRE/codes/c4 112/ts.txt";
		//readTickets(p4);
		
		System.out.println("reading time= "+seconds);
		
		int k = 1;
		
		startTime = System.currentTimeMillis();
		List<set_obj> f1 = frequentItemset1();
		//printObjs(f1);
		int old_size = 0;
		int curr_size = f1.size();
		k++;
		endTime = System.currentTimeMillis();
		seconds = (endTime - startTime);
		System.out.println("time for creating "+(k-1)+" size set = "+  seconds);
		
		System.out.println("Num of Account ==> " + numTxns);
		System.out.println("Num of unique Items ==> " + numUniqueItems);
		System.out.println("Avg Items per txn ==> " + avgItemPerTxn);
		
		while(curr_size > old_size ){
			startTime = System.currentTimeMillis();
			f1.addAll(frequentItemsetFromPrevious(f1,k));
			//printObjs(f1);
			old_size = curr_size;
			curr_size = f1.size();
			k++;
			endTime = System.currentTimeMillis();
			seconds = (endTime - startTime);
			System.out.println("time for creating "+(k-1)+" size set = "+  seconds);
		}
		printObjs(f1);
		System.out.println("Process Completed, no set exist for k > " + (k-2));
		
		List<rule> Rules = new ArrayList<rule>();
		
		Rules = getRules(f1);
		printRules(Rules);
		long EE = System.currentTimeMillis();
		
		
		
		seconds = (EE - SS);
		
		
		System.out.println("time for whole process = "+  seconds);
	}
	
	

	
	private static void printRules(List<rule> rules) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("rulesWithGT10TS.txt", "UTF-8");
		
		int l = rules.size();
		for(int i=0;i<l;i++){
			rule r = rules.get(i);
			for(int k = 0;k<r.lhs_set.length;k++){
				writer.print(r.lhs_set[k]+" ");		
			}
			writer.print("$ ");
			for(int k = 0;k<r.rhs_set.length;k++){
				writer.print(r.rhs_set[k]+" ");		
			}
			writer.print("$ " + r.sup_rule + " $ " + r.confidence_r + " $ "+r.lift_r);
			if(i<(l-1)){
			writer.println();}
		}
		writer.close();
		
	}




	private static List<rule> getRules(List<set_obj> f1) {
		
		int L = f1.size();
		List<rule> R = new ArrayList<rule>();
		
		for(int i=0;i<L;i++){
			for(int j = 0; j<L;j++){
				if(i==j){}
				else if(!subArray(f1.get(i).getItems(),f1.get(j).getItems())){
					double sl = 1.0 * f1.get(i).support;
					double sr = 1.0 * f1.get(j).support;
					
					int[] rl = new int[f1.get(i).getItems().length + f1.get(j).getItems().length];
					int k=0;
					for(k=0;k<f1.get(i).getItems().length;k++){
						rl[k] = f1.get(i).getItems()[k];
					}
					for(int k1=0;k1<f1.get(j).getItems().length;k1++){
						rl[k1+k] = f1.get(j).getItems()[k1];
					}
					
					for(k = 0;k<L;k++){
						if(compareArray(rl,f1.get(k).getItems())){
							rule e = new rule(f1.get(i).getItems(),f1.get(j).getItems(),f1.get(i).getSupport(),f1.get(j).getSupport(),f1.get(k).getSupport());
							if(e.confidence_r >= minConfidence && e.lift_r >= minlift && e.sup_rule >= minSupport){
							R.add(e);
							}
							break;
						}
					}
				}
			}
		}
		
		return R;
		
		
		
		
		
	}

    
	private static boolean compareArray(int[] s1, int[] s2) {
	    int l1 =  s1.length;
	    int l2 =  s2.length;
	    Arrays.sort(s1);
	    Arrays.sort(s2);
	    if(l1==l2){
	    	for(int i=0;i<l1;i++){
	    		if(s1[i]!=s2[i]){return false; }
	    	}
	    	return true;
	    }
	    
	    else{
		return false;
	    }
		
	}
	private static boolean subArray(int[] s1, int[] s2) {
	    int l1 =  s1.length;
	    int l2 =  s2.length;
	    if(l1==0 || l2==0) {
	    	return true;
	    }
	    
	    Arrays.sort(s1);
	    Arrays.sort(s2);
	    int i1 = 0;
	    int i2 = 0;
	    
	    while (i1<l1 && i2<l2){
	    	if(s1[i1] == s2[i2]) {return true;}
	    	else if(s1[i1] > s2[i2]) {
	    		i2++;
	    	}
	    	else{i1++;}	
	    }
	    
		return false;
	}
    
	

	private static void printObjs(List<set_obj> f1) {
		int len = f1.size();
		for(int i=0;i<len;i++){
			set_obj Obj = f1.get(i);
			int lt = Obj.items.length;
			for(int j=0;j<lt;j++){
				System.out.print(Obj.items[j] + " , ");
			}
			System.out.print(" with support = "+ Obj.support);
			System.out.println();
		}
		System.out.println("DONE PRINTING");
	}




	private static List<set_obj> freqSet(List<int[]> candidateSet) {
		
		List<set_obj> freqSets = new ArrayList<set_obj>();
		
		int lenS = candidateSet.size();
		for(int i=0;i<lenS;i++){
			int lenT = numTxns;
			set_obj Obj = new set_obj(candidateSet.get(i),0);
							
			
			int[] itemI = Obj.getItems(); 
			int lenI = itemI.length;
			int sup = 0;
			for(int t=0;t<lenT;t++){
				Set<Integer> dt =  data.get(t);
				boolean flag = true;
				for(int j=0;j<lenI;j++){
					if(!dt.contains(itemI[j])){
						flag = false;
						break;
					}
				}
				if(flag == true){
					sup++ ;
				}
			}
			
			
			if(sup>=minSupport*numTxns){
				Obj.support = (1.0 *sup/numTxns);
				
				freqSets.add(Obj);
			}
			
		}
		
		
		return freqSets;
		
		
		
	}
	
	



	private static List<set_obj> frequentItemset1() {
		Set<Integer> L1 = new HashSet<Integer>();
		
		List<int[]> finalL1 = new ArrayList<int[]>();
		int len = data.size();
		int total_item = 0;
		for(int i=0;i<len;i++){
			
			Set<Integer> DI = data.get(i);
			total_item = total_item + DI.size();
			
			for (Iterator<Integer> iter = DI.iterator(); iter.hasNext(); ){ 
				int item = iter.next();
				if(L1.contains(item)){
					
				}
				else
				{
					//if(TicketSize.get(item)>= ){
					L1.add(item);	//}
				}
			    if (!uniqueItems.contains(item)){
			    	uniqueItems.add(item);
			    }
				
				
			}
			
		}
		numUniqueItems = uniqueItems.size();
		avgItemPerTxn = (total_item)/numTxns;
		
		
//		Iterator itx = L1.iterator();
		for (Iterator<Integer> itx = L1.iterator(); itx.hasNext(); ){
//		while(itx.hasNext()){
			int[] im = {itx.next()};
			finalL1.add(im);
		    
		}
		
		return freqSet(finalL1);
	}

	private static void readCSV(String path)
	{
		try{
			Scanner inputStream = new Scanner(new FileReader(path));
			
			while (inputStream.hasNextLine()) {
				
				String line = inputStream.nextLine();
				
				Scanner lineScanner = new Scanner(line);
				
				
				
				while(lineScanner.hasNext()){
					
					String[] st = lineScanner.next().split(",");
					int lnt = st.length;
					
					Set<Integer> setItems = new HashSet<Integer>();
					for(int i=0;i<lnt;i++){
						int Ix = Integer.parseInt(st[i]);
						if(!setItems.contains(Ix)){
							setItems.add(Ix);
						}
						
					}
					data.add(setItems);
						
						
				}
				lineScanner.close();
				
				
			}inputStream.close();
		}
		catch (FileNotFoundException e){
			e.printStackTrace();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("readCSV Done");
		
		
		
	}
	
		
	private static void readCSV_1(String path)
	{
		try{
			Scanner inputStream = new Scanner(new FileReader(path));
			
			String line = inputStream.nextLine();
			line = inputStream.nextLine();// special treatment to deal with row 1 where column names are defined
			String[] st = line.split(",");
			long cust_id = Long.parseLong(st[0]);
			long curr_id = cust_id;
			Set<Integer> setItems = new HashSet<Integer>();
			setItems.add(Integer.parseInt(st[1]));
			
			while (inputStream.hasNextLine()) {
				
				line = inputStream.nextLine();
				st = line.split(",");
				cust_id = Long.parseLong(st[0]);
				if(curr_id == cust_id){
					setItems.add(Integer.parseInt(st[1]));
				}
				else{
					data.add(setItems);
					setItems = new HashSet<Integer>();
					curr_id = cust_id;
					
					setItems.add(Integer.parseInt(st[1]));
				}
				
			}
			data.add(setItems);
			inputStream.close();
		}
		catch (FileNotFoundException e){
			e.printStackTrace();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("readCSV Done");
		
		
		
	}
	
	private static void printData(){
		int len = data.size();
		for(int i=0;i<len;i++){
			Iterator iter = data.get(i).iterator();
			while (iter.hasNext()) {
			    System.out.print(iter.next()+ ", ");
			}
			 System.out.println();
		}
		
	}
	
	//itemsetCount will represent new freq itemset size
	private static List<set_obj> frequentItemsetFromPrevious(List<set_obj> Lkp,int itemsetCount){
		int len = Lkp.size();
		List<int[]> relevantSet = new ArrayList<int[]>();
		List<int[]> CandidateSet = new ArrayList<int[]>();
		
		for(int i=0;i<len;i++){
			set_obj Obj = new set_obj(Lkp.get(i).items,0);
			int len1 = Obj.items.length;
			if(len1 == itemsetCount-1)
			{
				relevantSet.add(Obj.items);
			}
		}
		
		
		int lr = relevantSet.size();
		
		int sizeLastItemSet = itemsetCount-1;
		for(int i=0;i<lr;i++){
			for(int j=i+1;j<lr;j++){
				boolean flag = true;
				for(int x=0;x<sizeLastItemSet-1;x++){
					if(relevantSet.get(i)[x] == relevantSet.get(j)[x]){
						flag = true;
					}
					else {flag = false; break;}
				}
				if (flag == true && ((relevantSet.get(i)[sizeLastItemSet-1]) != relevantSet.get(j)[sizeLastItemSet-1])){
					int[] newCandidate1 = new int[itemsetCount];
					
					for(int x=0;x<itemsetCount-1;x++){
						newCandidate1[x] = relevantSet.get(i)[x];
						
					}
					newCandidate1[itemsetCount-1] = relevantSet.get(j)[sizeLastItemSet-1];
					 
					CandidateSet.add(newCandidate1);
					
				}
				
			}
		}
		
		List<set_obj> freqSetFromCk = freqSet(CandidateSet);
		
		/*for(int i = 0;i<freqSetFromCk.size();i++)
		{
			for(int j = 0;j<itemsetCount;j++){
				System.out.print(freqSetFromCk.get(i).items[j] + " " +  freqSetFromCk.get(i).support);
			}
			System.out.println();
			System.out.println("Set" + (i+1));
		}*/
		
		return freqSetFromCk;
		
		
		
	}
	private static void readTickets(String p1) {
		try{
			Scanner inputStream = new Scanner(new FileReader(p1));
			String line = inputStream.nextLine();//to ignore header
			
			while (inputStream.hasNextLine()) {
				
				line = inputStream.nextLine();
				String[] st = line.split(",");
				
				TicketSize.put(Integer.parseInt(st[0]),Double.parseDouble(st[1]));
				
			}
			
			
		}
		catch (FileNotFoundException e){
			e.printStackTrace();	
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		
		System.out.println("read tickets Done");
		
		
	}
	
	

}

