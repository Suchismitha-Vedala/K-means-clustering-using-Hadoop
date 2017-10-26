package homework1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import homework1.Canopy_KMeans.canopiesMapper;
import homework1.Canopy_KMeans.canopiesReducer;
import homework1.DataProcessing.DataProcessingMapper;
import homework1.DataProcessing.DataProcessingReducer;
import homework1.Canopy_KMeans.FinalMapper;
import homework1.Canopy_KMeans.FinalReducer;
import homework1.Canopy_KMeans.K_Means4Mapper;
import homework1.Canopy_KMeans.K_MeansReducer;

public class HomeWorkExecute extends Configured implements Tool {

	public int dataProcessing(String inputFile, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(new Configuration());

		job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, 11);

		System.out.format("--- NumLinesPerSplit: %d\n", NLineInputFormat.getNumLinesPerSplit(job));

		job.setJarByClass(HomeWorkExecute.class);

		job.setMapperClass(DataProcessingMapper.class);

		job.setReducerClass(DataProcessingReducer.class);

		job.setInputFormatClass(NLineInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFile));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public int canopySelector(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());

		job.setJarByClass(HomeWorkExecute.class);

		job.setMapperClass(canopiesMapper.class);

		job.setReducerClass(canopiesReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public int productCanopyAssigner(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(new Configuration());

		job.getConfiguration().setInt(NLineInputFormat.LINES_PER_MAP, 11);

		System.out.format("--- NumLinesPerSplit: %d\n", NLineInputFormat.getNumLinesPerSplit(job));

		job.setJarByClass(HomeWorkExecute.class);

		job.setMapperClass(Canopy_KMeans.ptocMapper.class);

		job.setReducerClass(Canopy_KMeans.ptocReducer.class);

		job.setInputFormatClass(NLineInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	public int kMeans(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());

		job.setJarByClass(HomeWorkExecute.class);

		job.setMapperClass(K_Means4Mapper.class);

		job.setReducerClass(K_MeansReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public int execute(String inputFolder, String outputFolder) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());

		job.setJarByClass(HomeWorkExecute.class);

		job.setMapperClass(FinalMapper.class);

		job.setReducerClass(FinalReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputFolder));

		FileOutputFormat.setOutputPath(job, new Path(outputFolder));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(1);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	@Override
	public int run(String[] args) throws Exception {
		for (int i = 0; i < args.length; i++) {
			System.out.println("--- Arg " + Integer.toString(i) + " : " + args[i]);
		}

		String hostName = java.net.InetAddress.getLocalHost().getHostName();

		String datapath = "";

		if (hostName.toLowerCase().indexOf("whale") >= 0)
			datapath = "/cloudc52/";

		dataProcessing(args[0], datapath + "dataget/");
		canopySelector(datapath + "dataget/", datapath + "canopies/");
		productCanopyAssigner(args[0], datapath + "product2canopy/");
		kMeans(datapath + "product2canopy/", datapath + "final/");
		execute(datapath + "dataget/", args[1]);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new HomeWorkExecute(), args);
	}

}
