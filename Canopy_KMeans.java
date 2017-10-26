package homework1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import homework1.SupporterClass;

public class Canopy_KMeans {

	public static class canopiesMapper extends Mapper<LongWritable, Text, Text, Text> {

		private HashMap<String, String> cc = new HashMap<>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String v = value.toString();
			String[] v_array = v.split("[ \t]");

			String itemID = v_array[0];
			String userIDRatingString = v_array[1];

			if (cc.containsKey(itemID))
				return;

			if (cc.isEmpty()) {
				cc.put(itemID, userIDRatingString);
				return;
			}

			ArrayList<String> list2 = SupporterClass.ListOfUsers(userIDRatingString);
			boolean shouldBeAdded = true;
			for (String k : cc.keySet()) {
				String users = cc.get(k);
				ArrayList<String> list1 = SupporterClass.ListOfUsers(users);

				if (SupporterClass.getUsersCnt(list1, list2) >= 8) {
					shouldBeAdded = false;
					break;
				}
			}

			if (shouldBeAdded)
				cc.put(itemID, userIDRatingString);

		}

		@Override
		protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			for (String k : cc.keySet()) {
				String users = cc.get(k);

				context.write(new Text("1"), new Text(k + "===" + users));
			}

			super.cleanup(context);
		}

	}

	public static class canopiesReducer extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			HashMap<String, String> canopyCenters = new HashMap<>();
			for (Text value : values) {

				String[] v = value.toString().split("===");

				String itemID = v[0];
				String userIDRatingString = v[1];

				if (canopyCenters.isEmpty()) {
					canopyCenters.put(itemID, userIDRatingString);
					continue;
				}

				ArrayList<String> list2 = SupporterClass.ListOfUsers(userIDRatingString);
				boolean shouldBeAdded = true;
				for (String k : canopyCenters.keySet()) {
					String users = canopyCenters.get(k);
					ArrayList<String> list1 = SupporterClass.ListOfUsers(users);

					if (SupporterClass.getUsersCnt(list1, list2) >= 8) {
						shouldBeAdded = false;
						break;
					}
				}

				if (shouldBeAdded)
					canopyCenters.put(itemID, userIDRatingString);
			}

			for (String k : canopyCenters.keySet())
				context.write(new Text(k), new Text(canopyCenters.get(k)));

		}

	}
	
	public static class ptocMapper extends Mapper<LongWritable, Text, Text, Text> {

		private int count;

		private String itemID;
		private String userID;
		private String score;

		public ptocMapper() {
			count = 0;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			count++;
			String v = value.toString();

			if (value.find("product/productId:") >= 0)
				itemID = v.split(" ")[1];
			else if (value.find("review/userId:") >= 0)
				userID = v.split(" ")[1];
			else if (value.find("review/score:") >= 0)
				score = v.split(" ")[1];

			if (count == 11) {
				context.write(new Text(itemID), new Text(String.format("(%s,%s)", userID, score)));
			}
		}

	}

	public static class ptocReducer extends Reducer<Text, Text, Text, Text> {

		private static final String FileNamePath = "canopies/part-r-00000";
		HashMap<String, String> canopy = null;

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			canopy = SupporterClass.readFile(FileNamePath);

			super.setup(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String sum = new String();

			ArrayList<String> userList = new ArrayList<>();
			for (Text text : values) {
				String v = text.toString();
				userList.add(v.split(",")[0]);

				sum = sum + text.toString() + ";";
			}

			ArrayList<String> canopyList = new ArrayList<>();

			for (String k : canopy.keySet()) {
				ArrayList<String> canopyListofUsers = new ArrayList<>();

				String v = canopy.get(k);
				String[] v_arr = v.split(";");
				for (int i = 0; i < v_arr.length; i++) {
					if (v_arr[i].trim().length() == 0)
						continue;
					canopyListofUsers.add(v_arr[i].trim().split(",")[0]);
				}

				if (SupporterClass.getUsersCnt(canopyListofUsers, userList) >= 2)
					canopyList.add(k);

			}
			
			if(!canopyList.isEmpty()) {
				sum = sum + "\t{";
				for (String t : canopyList)
					sum = sum + t + "-";
				sum = sum + "}";
	
				context.write(key, new Text(sum));
			}
		}

	}
	
private static final String fileNamePathKMeans = "product2canopy/part-r-00000";
	
	public static class K_Means4Mapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private HashMap<String, HashMap<String, Double>> allCanopies = new HashMap<>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			HashMap<String, String> allRecords = SupporterClass.readFile(fileNamePathKMeans);
			
			for(String k : allRecords.keySet()) {
				allCanopies.put(k, SupporterClass.UserIDRating(allRecords.get(k)));
			}
			
			super.setup(context);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("\t");
			
			String itemID = v[0];
			String userIDRatingString = v[1];
			String canopiesString = v[2].substring(1, v[2].length()-2);
			
			String[] loadedMovie_canopyList = canopiesString.split("-");
			HashMap<String, Double> loadedMovie_scoreVector = SupporterClass.UserIDRating(userIDRatingString);
			
			double maxCosine = -1;
			String maxCosine_Center = new String("");
			
			for(int i=0;i<loadedMovie_canopyList.length;i++) {
				String c = loadedMovie_canopyList[i];
				
				if(allCanopies.containsKey(c)) {
					HashMap<String, Double> hm = allCanopies.get(c);
					
					double cs = SupporterClass.sim(hm, loadedMovie_scoreVector);
					if(cs > maxCosine) {
						maxCosine = cs;
						maxCosine_Center = c;
					}
				}
			}
			
			context.write(new Text(maxCosine_Center), new Text(itemID + ";" + userIDRatingString));
		}


	}

	public static class K_MeansReducer extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, HashMap<String, Double>> Canopies = new HashMap<>();
		
		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			HashMap<String, String> allRecords = SupporterClass.readFile(fileNamePathKMeans);
			
			for(String k : allRecords.keySet()) {
				Canopies.put(k, SupporterClass.UserIDRating(allRecords.get(k)));
			}
			super.setup(context);
		}

		public void reduce(Text centerID, Iterable<Text> itemsInCluster, Context context) throws IOException, InterruptedException {
			HashMap<String, Double> userID_avg = new HashMap<>();
			HashMap<String, Double> userID_count = new HashMap<>();
			for (Text t : itemsInCluster) {
				String[] v = t.toString().split(";");
				String itemID = v[0];
				String userIDRatingString = v[1];
				
				HashMap<String, Double> extractUserIDRating = SupporterClass.UserIDRating(userIDRatingString);
				
				for(String k : extractUserIDRating.keySet()) {
					if(userID_avg.containsKey(k)) {
						double sum = userID_avg.get(k) * userID_count.get(k) + extractUserIDRating.get(k);
						double count = userID_count.get(k) + 1;
						userID_avg.put(k, sum / count);
						userID_count.put(k, count);
					} else {
						userID_avg.put(k, 0.0);
						userID_count.put(k, 0.0);
					}
				}
			}
			
			String str = new String();
			for(String k : userID_avg.keySet()) {
				str = str + ";" + String.format("%.3f", userID_avg.get(k));
			}
			
			context.write(centerID, new Text(str));
		}

	}
	
	
private static final String FileNamePath = "final/part-r-00000";
	
	public static class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		private ArrayList<HashMap<String, Double>> centers = new ArrayList<HashMap<String,Double>>();

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			centers = SupporterClass.readCenterFileSeq(FileNamePath);
			
			super.setup(context);
		}
		
		private String convertUserIDRatingToString(HashMap<String, Double> m) {
			String str = new String();
			
			for(String userID : m.keySet()) {
				str = str + String.format("{%s,%.3f};", userID, m.get(userID));
			}
			
			return str;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] v = value.toString().split("\t");
			
			String itemID = v[0];
			String userIDRatingString = v[1];
			
			HashMap<String, Double> loadedMovie_scoreVector = SupporterClass.UserIDRating(userIDRatingString);
			
			for(HashMap<String, Double> center : centers) {
				double cs = SupporterClass.sim(center, loadedMovie_scoreVector);
				
				String centerID = convertUserIDRatingToString(center);
				String loadedMovieStr = convertUserIDRatingToString(loadedMovie_scoreVector);
				
				context.write(new Text(centerID), new Text(String.format("%.3f---%s", cs, loadedMovieStr)));
			}
			
		}


	}

	public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
		
		private HashMap<String, Integer> newCenters = new HashMap<String, Integer>();
		
		public void reduce(Text centerID, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double maxScore = -1;
			String newCenter = new String("");
			for(Text t : values) {
				String[] v = t.toString().split("---");
				
				double score = Double.valueOf(v[0]);
				if(score > maxScore) {
					score = maxScore;
					newCenter = v[1];
				}
			}
			
			if(!newCenters.containsKey(newCenter))
				newCenters.put(newCenter, 1);
		}
		
		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			
			for(String nc : newCenters.keySet()) 
				context.write(new Text("1"), new Text(nc));
			
			super.cleanup(context);
		}

	}


}
