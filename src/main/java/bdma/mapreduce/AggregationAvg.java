package bdma.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Set;

public class AggregationAvg extends JobMapReduce {

	public static class AggregationAvgMapper extends Mapper<Text, Text, Text, DoubleWritable> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Getting configuration parameters:
			String groupBy = context.getConfiguration().getStrings("groupBy")[0];
			String agg = context.getConfiguration().getStrings("agg")[0];
			// Getting the lines taping into the CSV file structure:
			String[] arrayValues = value.toString().split(",");
			String groupByValue = Utils.getAttribute(arrayValues, groupBy);
			double aggValue = Double.parseDouble(Utils.getAttribute(arrayValues, agg));
			// We output the type - col pairs per line:
			context.write(new Text(groupByValue), new DoubleWritable(aggValue));
		}
	}

	public static class AggregationAvgReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			// Simply keeping track of the number of items per type and their value to compute the mean:
			double sum = 0;
			int len = 0;
			for (DoubleWritable value : values) {
				sum += value.get();
				len += 1;
			}
			context.write(key, new DoubleWritable(sum/len));
		}
	}

	public AggregationAvg() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "AggregationAvg");
		AggregationAvg.configureJob(job, this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

	public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(AggregationAvg.class);
		// Setting the Mapper Class and its outputs:
		job.setMapperClass(AggregationAvgMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		// We cannot apply a Combiner Class, since we would lose the track of 'how many items
		// we have processed so far' (the denominator of an Average operation). An alternative
		// would be to keep track of the actual number of items and save it in the output of
		// a helper function we may create as 'Combiner', but the easiest and seamless way to
		// perform such an operation is just to avoid using a Combiner Class to compute the
		// average per attribute value.

		// Setting the Reducer Class:
		job.setReducerClass(AggregationAvgReducer.class);
		// The output will be Text as well:
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		// The files the job will read from/write to:
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));
		// Setting the parameter we are going to retrieve from Map and Reduce functions:
		job.getConfiguration().setStrings("groupBy", "type");
		job.getConfiguration().setStrings("agg", "col");
    }
}
