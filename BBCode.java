import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

public class BBCode {

	public static class MyMapper extends Mapper<LongWritable, Text, Text,Text> {

        private static short[][]  array = new short[14000000][];
		private static short[][]  results = new short[14000000][];
		private Text outputKey = new Text();
        private Text outputValue = new Text();
		protected void setup(Context context) throws java.io.IOException, InterruptedException {
			Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			for( Path p : files) {
				if(p.getName().equals("MR_MR_test1_1502_1504.txt")) {
				System.out.println(" in test ");
				int counter = 0;
				long Start = System.nanoTime();
				{
					BufferedReader reader = new BufferedReader (new FileReader(p.toString()));
					String line = reader.readLine();
					while(line!=null) {
						String[] itr = line.split(",");
						if(true){
							results[Integer.parseInt(itr[0])] = new short[itr.length - 1];
							//System.out.println(results[Integer.parseInt(itr[0])].length);

							for(int u = 0; u < itr.length - 1; u++) { results[Integer.parseInt(itr[0])][u] =  Short.parseShort(itr[u + 1]);
								//System.out.println(results[Integer.parseInt(itr[0])][u]);
							 }
						}
						context.progress();
						counter++;
						line = reader.readLine();
					   }
					   long End = System.nanoTime();
					reader.close();
					System.out.println(End - Start);
					System.out.println("READING DONE test made");

				}
				}
				if(p.getName().equals("MR_MR_train1_1401_1412.txt")) {
				System.out.println(" in train ");
				int counter = 0;
				long Start = System.nanoTime();
				{
					BufferedReader reader = new BufferedReader (new FileReader(p.toString()));
					String line = reader.readLine();
					while(line!=null) {
						String[] itr = line.split(",");

						if( itr.length - 1 > 0 ){
							//System.out.println(itr.length - 1);
							array[Integer.parseInt(itr[0])] = new short[itr.length - 1];


							for(int u = 0; u < itr.length - 1; u++) { array[Integer.parseInt(itr[0])][u] =  Short.parseShort(itr[u + 1]);
							//System.out.println(array[Integer.parseInt(itr[0])][u]);

							}
						}
						context.progress();
						counter++;
						line = reader.readLine();
					   }
					   long End = System.nanoTime();
					reader.close();
					System.out.println(End - Start);
					System.out.println("READING DONE train made");

				}
				}

            }
        }
        public void map(LongWritable key , Text value, Context context) throws java.io.IOException, InterruptedException {
			// value looks like - saurav, 1, 34, 56, 78
			// token looks like - {saurav, 1, 34, }
			// thisCust looks like - {1, 34, 56 }

			long Start = System.nanoTime();

			String row = value.toString();
			String[] tokens = row.split(",");
			int size = tokens.length - 1;
			short[] thisCust = new short[size];
			for(int i = 0 ; i < size; i++) {
				thisCust[i] = Short.parseShort(tokens[i+1]);
			}
			int[] thisCustResults = new int[10000];

			boolean inTest = true;

			if(results[Integer.parseInt(tokens[0])] != null){
				for(int i = 0; i < results[Integer.parseInt(tokens[0])].length; i++) {
					thisCustResults[ results[Integer.parseInt(tokens[0])][i] ] = 1;
				}
			} else inTest = false;

			//System.out.println(inTest);
			//System.out.println(tokens[0]);

			int[] productsNotBeenThere = new int[10000];
			int[] productsBeenThere = new int[10000];

			for(int i = 0; i < 14000000; i++){
				if(array[i] != null) {

					int similarity = 0; int indexThisCust = 0;	int indexArray = 0;

					while( indexThisCust < thisCust.length && indexArray < array[i].length){
						if(thisCust[indexThisCust]  < array[i][indexArray]) { indexThisCust++;   }
						else {
							if(thisCust[indexThisCust]  > array[i][indexArray]) { indexArray++;   }
							else{
								  indexThisCust++; indexArray++; similarity++;
								}
						}
					}
					//System.out.println("similarity " + similarity);
					indexThisCust = 0; indexArray = 0;

					if(similarity > 3) {

						while( indexThisCust < thisCust.length && indexArray < array[i].length){
							if(thisCust[indexThisCust]  < array[i][indexArray]) { indexThisCust++;   }
							else {
								if(thisCust[indexThisCust]  > array[i][indexArray]) {
									productsNotBeenThere[array[i][indexArray]]+=similarity;
									indexArray++;
								}
								else{
									productsBeenThere[array[i][indexArray]]+=similarity;
									indexThisCust++; indexArray++;
								}
							}
						}
						while(indexArray < array[i].length) {
							productsNotBeenThere[array[i][indexArray]]+=similarity;
							indexArray++;
						}
					}
				}
			}


			TreeMap<Integer, List<Integer>> map2 = new TreeMap<Integer, List<Integer>>();
			for(int y = 0; y < productsNotBeenThere.length; y++) {
				List<Integer> ind = map2.get(productsNotBeenThere[y]);
				if(ind == null){
					ind = new ArrayList<Integer>();
					map2.put(productsNotBeenThere[y], ind);
				}
				ind.add(y);
			}
			int howmany2 = 1;
			outer1:
			for(Integer t : map2.descendingKeySet()){
				if(t.intValue() == 0 ) break;
				for(Integer g: map2.get(t)){
					context.write( new Text( tokens[0] + "-" + "NBT" ), new Text( String.valueOf(howmany2)+ "-" + String.valueOf(g) + "-" +
					(inTest ? thisCustResults[g]:0 ) ) );
					howmany2++;
					if(howmany2 > 100) break outer1;
			    }
			}


			TreeMap<Integer, List<Integer>> map3 = new TreeMap<Integer, List<Integer>>();
			for(int y = 0; y < productsBeenThere.length; y++) {
				List<Integer> ind = map3.get(productsBeenThere[y]);
				if(ind == null){
					ind = new ArrayList<Integer>();
					map3.put(productsBeenThere[y], ind);
				}
				ind.add(y);
			}
			howmany2 = 1;
			outer1:
			for(Integer t : map3.descendingKeySet()){

				if(t.intValue() == 0 ) break;
				for(Integer g: map3.get(t)){

					context.write( new Text( tokens[0] + "-" + "BT" ), new Text( String.valueOf(howmany2)+ "-" + String.valueOf(g) + "-" +
					(inTest ? thisCustResults[g]:0 ) ) );
					howmany2++;
					if(howmany2 > 100) break outer1;
			    }
			}


			long End = System.nanoTime();
			System.out.println(End - Start);


		}
    }
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Job job = new Job();
			job.setJarByClass(BBCode.class);
			//job.setJar("units.jar");
			job.setJobName("BBCode");
			job.setNumReduceTasks(0);
			try{
			DistributedCache.addCacheFile(new URI("/data/gcbbbyxn/work/shivam/co_mre_1401_1504/dc/MR_MR_train1_1401_1412.txt"), job.getConfiguration());
			DistributedCache.addCacheFile(new URI("/data/gcbbbyxn/work/shivam/co_mre_1401_1504/dc/MR_MR_test1_1502_1504.txt"), job.getConfiguration());
			}
			catch(Exception e) { System.out.println(e); }

			job.setMapperClass(MyMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			job.waitForCompletion(true);


		}
}
