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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.lang.Math;

class datapoint {
	long acct_id;
	HashSet<Integer> items; 
	
	public datapoint(){
		acct_id = -1;
		items = null;
	}
	
	public datapoint(long a, HashSet<Integer> itms){
		this.acct_id = a;
		this.items = itms;
	}
	
}
class acct_id_comp implements Comparator<datapoint>
{
    public int compare(datapoint o1, datapoint o2)
    {
       return o1.acct_id<o2.acct_id?-1:o1.acct_id>o2.acct_id?1:0;
   }
}
class tuple{
	int item;
	double count;
	int N;
	public tuple(){
		item = -1;
		count = 0;
		N = 0;
	}
	public tuple(int i, double c, int n){
		this.item = i;
		this.count = c;
		this.N = n;
	}
}

class tuple_count implements Comparator<tuple>
{
    public int compare(tuple o1, tuple o2)
    {
       return o1.count>o2.count?-1:o1.count<o2.count?1:0;
   }
}

class tupleT{
	int cid;
	double value;
	
	public tupleT(){
		cid = -1;
		value = 0.0;
	}
	public tupleT(int i, double c){
		this.cid = i;
		this.value = c;
	}
}
class datapointPost {
	long acct_id;
	ArrayList<tuple> recomms; 
	
	public datapointPost(){
		acct_id = -1;
		recomms = null;
	}
	
	public datapointPost(long a, ArrayList<tuple> r){
		this.acct_id = a;
		this.recomms = r;
	}
	
}

class acct_id_comp1 implements Comparator<datapointPost>
{
    public int compare(datapointPost o1, datapointPost o2)
    {
       return o1.acct_id<o2.acct_id?-1:o1.acct_id>o2.acct_id?1:0;
   }
}


class accuracyPoint {
	long acct_id;
	ArrayList<Integer> hitRank; 
	
	public accuracyPoint(){
		acct_id = -1;
		hitRank = null;
	}
	
	public accuracyPoint(long a, ArrayList<Integer> ranks){
		this.acct_id = a;
		this.hitRank = ranks;
	}
	
}

public class Recommend {

	
	
	static List<rule> Rls = new ArrayList<rule>();
	static List<datapoint> dataSet = new ArrayList<datapoint>();
	static List<datapoint> dataSetPost = new ArrayList<datapoint>();
	static int TopN = 10;
	static double threshold_c = .20;
	static HashMap<Integer,Double> TicketSize = new HashMap<Integer,Double>();
	static double[] impact = new double[TopN];
	
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		System.out.println("aaya");
		String p1 = "C:/Users/ss45797/Documents/BestBuy/MRE/Data/rulesWithGT10TS.txt";
		String p2 = "C:/Users/ss45797/Documents/BestBuy/MRE/Data/input-cob.csv";
		String p3 = "C:/Users/ss45797/Documents/BestBuy/MRE/Data/input-post.csv";
		//String p4 = "C:/Users/ss45797/Documents/BestBuy/MRE/codes/c4 112/ts.txt";
		readRules(p1);
		//readTickets(p4);
		//printRules(Rls);
		readData(p2);
		//printData(dataSet);
		List<datapointPost> recommendationData = recommendation();
		//printRD(recommendationData);
		readData_1(p3);
		System.out.println("read");
		//printData(dataSetPost);
		//System.out.println(dataSet.size());
		//System.out.println(dataSetPost.size());
		//mergeData(dataSetPost,recommendationData);
		
		System.out.println(dataSetPost.size()+"    post data");
		List<datapointPost> topReco = prune(recommendationData); 
		printRD(topReco);
		recommendationData = new ArrayList<datapointPost>();
		
		
		System.out.println(topReco.size()+"    top reco");
		ArrayList<accuracyPoint> AP = mergeData(dataSetPost,topReco);
		
		printAccuracy1(AP);
		getAccuracy(AP);
		System.out.println(recommendationData.size() + "   recoos");
		for(int i=0;i<TopN;i++){
			System.out.print(impact[i]+",");
		}
	}
	
	private static void getAccuracy(ArrayList<accuracyPoint> aP) {
		int[] res = new int[TopN];
		int len = aP.size();
		for(int i=0;i<len;i++){
			for(int j=0;j<aP.get(i).hitRank.size();j++){
				res[aP.get(i).hitRank.get(j)]++ ;
			}
			
		}
		for(int i=0;i<TopN;i++){
			System.out.print(res[i]+" $ ");
		}
		System.out.println(len);
		
	}

	private static void printAccuracy(ArrayList<accuracyPoint> aP) {
		int len = aP.size();
		
		for(int i=0;i<len;i++){
			System.out.print(aP.get(i).acct_id + "$");
			for(int j=0;j<aP.get(i).hitRank.size();j++){
				System.out.print(aP.get(i).hitRank.get(j)+"$");
			}
			System.out.println();
		}
		
	}
	private static void printAccuracy1(ArrayList<accuracyPoint> aP) throws FileNotFoundException, UnsupportedEncodingException {
		int len = aP.size();
		PrintWriter writer = new PrintWriter("accu.txt", "UTF-8");
		writer.println("MYBBY2,rank,hit");
		
		for(int i=0;i<len;i++){
			for(int j=0;j<aP.get(i).hitRank.size();j++){
				if(aP.get(i).hitRank.get(j)<=9){
				writer.println(aP.get(i).acct_id + ","+(aP.get(i).hitRank.get(j)+1)+","+"1");
				
				}
			}
			
		}
		writer.close();
		
	}

	private static List<datapointPost> prune(List<datapointPost> recommendationData) {
		
		List<datapointPost> pruneReco = new ArrayList<datapointPost>(); 
		int len = recommendationData.size();
		int covered = 0;
		for(int i=0;i<len;i++){
			long acc = recommendationData.get(i).acct_id;
			ArrayList<tuple> tl = recommendationData.get(i).recomms;
			int l = tl.size();
			Collections.sort(tl,new tuple_count());
			ArrayList<tuple> tl1 = new ArrayList<tuple>(tl.subList(0, min(TopN,l)));
			if(tl1.size()>0) {covered++;}
			pruneReco.add(new datapointPost(acc,tl1));
		}
		
		
		System.out.println("accts sales active "+len +"   accts got recommendation "+covered);
		return pruneReco;
	}

	private static int min(int topN2, int l) {
		if (topN2 > l) { return l;}
		
		return topN2;
	}

	private static ArrayList<accuracyPoint> mergeData(List<datapoint> realD, List<datapointPost> recoD) {
		ArrayList<accuracyPoint> AP = new ArrayList<accuracyPoint>();
		Collections.sort(realD, new acct_id_comp());
		Collections.sort(recoD, new acct_id_comp1());
		int lenReal = realD.size();
		int lenReco = recoD.size();
		int iReal = 0;
		int iReco = 0;
		while(iReal<lenReal && iReco<lenReco){
			
			
			if((iReal<lenReal && iReco<lenReco) &&  realD.get(iReal).acct_id == recoD.get(iReco).acct_id){
				ArrayList<Integer> iResult = new ArrayList<Integer>();
				int ld = realD.get(iReal).items.size();
				int lr = recoD.get(iReco).recomms.size();
				for(int ix=0;ix<lr;ix++){
					if(realD.get(iReal).items.contains(recoD.get(iReco).recomms.get(ix).item)){
						iResult.add(ix);
						impact[ix] = 0;//impact[ix] + TicketSize.get(recoD.get(iReco).recomms.get(ix).item);
					}
				}
				AP.add(new accuracyPoint(realD.get(iReal).acct_id,iResult));
				iReal++;
				iReco++;
			}
			if((iReal<lenReal && iReco<lenReco) &&  realD.get(iReal).acct_id > recoD.get(iReco).acct_id){
				iReco++;
			}
			if((iReal<lenReal && iReco<lenReco) &&  realD.get(iReal).acct_id < recoD.get(iReco).acct_id){
				iReal++;
			}
		}
		return AP;
		
		
	}

	private static void printData(List<datapoint> dataSetPost2) {
		for(int i=0;i<dataSetPost2.size();i++){
			System.out.print(dataSetPost2.get(i).acct_id+" ");
			for (Iterator<Integer> iter = dataSetPost2.get(i).items.iterator(); iter.hasNext(); ){ 
				System.out.print(iter.next()+" ");
			}
			System.out.println();
		}
		
	}

	private static void readData_1(String path)
	{
		try{
			Scanner inputStream = new Scanner(new FileReader(path));
			
			String line = inputStream.nextLine();
			line = inputStream.nextLine();// special treatment to deal with row 1 where column names are defined
			String[] st = line.split(",");
			long cust_id = Long.parseLong(st[0]);
			long curr_id = cust_id;
			HashSet<Integer> setItems = new HashSet<Integer>();
			setItems.add(Integer.parseInt(st[1]));
			
			while (inputStream.hasNextLine()) {
				
				line = inputStream.nextLine();
				st = line.split(",");
				cust_id = Long.parseLong(st[0]);
				if(curr_id == cust_id){
					setItems.add(Integer.parseInt(st[1]));
				}
				else{
					datapoint d = new datapoint(curr_id,setItems);
					
					dataSetPost.add(d);
					setItems = new HashSet<Integer>();
					curr_id = cust_id;
					
					setItems.add(Integer.parseInt(st[1]));
				}
				
			}
			datapoint d = new datapoint(curr_id,setItems);
			dataSetPost.add(d);
//			here
			inputStream.close();
		}
		catch (FileNotFoundException e){
			e.printStackTrace();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("read data Done");
		
		
		
	}
	

	private static void printRD(List<datapointPost> dta) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("reco.txt", "UTF-8");
		writer.println("MYBBY2,rank,class");
		for(int i=0;i<dta.size();i++){
			for(int j=0;j<min(dta.get(i).recomms.size(),10);j++){
				writer.println(dta.get(i).acct_id+","+(j+1)+","+dta.get(i).recomms.get(j).item);
				
					
			}
			
		}
		writer.close();
		
	}

	private static List<datapointPost> recommendation() {

		List<datapointPost> rD = new ArrayList<datapointPost>();
		int num_accts = dataSet.size();
		int num_rule = Rls.size();
		
		for(int id = 0;id<num_accts;id++){
			long cust_id = dataSet.get(id).acct_id;
			ArrayList<tuple> rcos = new ArrayList<tuple>();
			for(int r = 0;r<num_rule;r++){
				
				if(Rls.get(r).confidence_r >= threshold_c){
				if(isContained(Rls.get(r).lhs_set,dataSet.get(id).items)){
					if(!isContained(Rls.get(r).rhs_set,dataSet.get(id).items)){
						
						HashSet<Integer> rc = removeSet(dataSet.get(id).items,Rls.get(r).rhs_set);
						for(int i=0;i<rcos.size();i++){
							if(rc.contains(rcos.get(i).item)){
								 
								//rcos.get(i).count =  ((rcos.get(i).count)*(rcos.get(i).N) + (Rls.get(r).confidence_r))/(rcos.get(i).N + 1);
								rcos.get(i).count =  ((rcos.get(i).count)*(rcos.get(i).N) + (Rls.get(r).lift_r))/(rcos.get(i).N + 1);
								//rcos.get(i).count = max(rcos.get(i).count ,Rls.get(r).confidence_r);
								//rcos.get(i).count = (rcos.get(i).count + Rls.get(r).sup_rr);
								//rcos.get(i).count ++;
								rcos.get(i).N++;
								
								rc.remove(rcos.get(i).item);
							}
						}
						
						for (Iterator<Integer> iter = rc.iterator(); iter.hasNext(); ){ 
							int item = iter.next();
							//tuple tp = new tuple(item, (Rls.get(r).confidence_r),1);
							tuple tp = new tuple(item, (Rls.get(r).lift_r),1);
							//tuple tp = new tuple(item, (Rls.get(r).confidence_r),1);
							//tuple tp = new tuple(item, (Rls.get(r).sup_rr),1);
							//tuple tp = new tuple(item, 1.0,1);
							rcos.add(tp);
						}
					
					}
				}
				}
			}
			rD.add(new datapointPost(cust_id,rcos));
		}
		
		return rD;
	}

	private static double max(double a, double b) {
		if(a>b){return a;}
		return b;
	}

	private static HashSet<Integer> removeSet(HashSet<Integer> items,int[] rhs_set) {
		int l = items.size();
		int la = rhs_set.length;
		HashSet<Integer> ixt = new HashSet<Integer>();
		
		for(int it=0;it<la;it++){
			if(!items.contains(rhs_set[it])){
				ixt.add(rhs_set[it]);
			}
		}
		
		
		return ixt;
	}

	private static boolean isContained(int[] lhs_set, HashSet<Integer> items) {
		int l = lhs_set.length;
		
		for(int i=0;i<l;i++){
			if(!items.contains(lhs_set[i])){
				return false;
			}
		}
		
		return true;
	}

	private static void printRules(List<rule> rules) {
		int l = rules.size();
		for(int i=0;i<l;i++){
			rule r = rules.get(i);
			for(int k = 0;k<r.lhs_set.length;k++){
				System.out.print(r.lhs_set[k]+" ");		
			}
			System.out.print(" $ ");
			for(int k = 0;k<r.rhs_set.length;k++){
				System.out.print(r.rhs_set[k]+" ");		
			}
			System.out.print(" $ " + r.sup_rule + " $ " + r.confidence_r + " $ "+r.lift_r + " $ "+r.sup_lr+ " $ "+r.sup_rr );
			System.out.println();
		}
		
		
	}

	private static void readRules(String path)
	{
		try{
			Scanner inputStream = new Scanner(new FileReader(path));
			
			
			while (inputStream.hasNextLine()) {
				
				String line = inputStream.nextLine();
				System.out.println(line);
				String[] st = line.split(" ");
				int len = st.length;
				int i=0;
				while(!st[i].equals("$")){
					i++;
				}
				int lhs_len = i;
				i++;
				while(!st[i].equals("$")){
					i++;
				}
				int rhs_len = i - lhs_len - 1;
				System.out.println(lhs_len + "   " + rhs_len);
				i++;
				double sp = Double.parseDouble(st[i]);
				i++;i++;
				double cf = Double.parseDouble(st[i]);
				i++;i++;
				double lf = Double.parseDouble(st[i]);
				int l[] = new int[lhs_len];
				for(int j=0;j<lhs_len;j++){
					l[j] = Integer.parseInt(st[j]);
				}
				int r[] = new int[rhs_len];
				for(int j=0;j<rhs_len;j++){
					r[j] = Integer.parseInt(st[j+lhs_len+1]);
				}
				rule R = new rule();
				R.setRule(l,r,sp,cf,lf);
				Rls.add(R);
				
			}
			
			
		}
		catch (FileNotFoundException e){
			e.printStackTrace();	
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		
		System.out.println("read rules Done");
		
		
		
	}
	
	private static void readData(String path)
	{
		try{
			Scanner inputStream = new Scanner(new FileReader(path));
			
			String line = inputStream.nextLine();
			line = inputStream.nextLine();// special treatment to deal with row 1 where column names are defined
			String[] st = line.split(",");
			long cust_id = Long.parseLong(st[0]);
			long curr_id = cust_id;
			HashSet<Integer> setItems = new HashSet<Integer>();
			setItems.add(Integer.parseInt(st[1]));
			
			while (inputStream.hasNextLine()) {
				
				line = inputStream.nextLine();
				st = line.split(",");
				cust_id = Long.parseLong(st[0]);
				if(curr_id == cust_id){
					setItems.add(Integer.parseInt(st[1]));
				}
				else{
					datapoint d = new datapoint(curr_id,setItems);
					
					dataSet.add(d);
					setItems = new HashSet<Integer>();
					curr_id = cust_id;
					
					setItems.add(Integer.parseInt(st[1]));
				}
				
			}
			datapoint d = new datapoint(curr_id,setItems);
			dataSet.add(d);
//			here
			inputStream.close();
		}
		catch (FileNotFoundException e){
			e.printStackTrace();	
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("read data Done");
		
		
		
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