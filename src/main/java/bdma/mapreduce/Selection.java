package bdma.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Selection extends JobMapReduce {

	public static class SelectionMapper extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// Getting the attribute and the attribute value we are filtering based on:
			String type = context.getConfiguration().getStrings("filterBy")[0];
			String filterFrom = context.getConfiguration().getStrings("value")[0];
			// Getting the value arrays using the comma, as we are dealing with a CSV format:
			String[] arrayValues = value.toString().split(",");
			// Getting the value from the position of the attribute 'type':
			String typeValue = Utils.getAttribute(arrayValues, type);
			// Simply outputting the found type and the whole comma-separated values:
			if (typeValue.equals(filterFrom)) {
				// Although we would be repeating the value of the input Sequence File Key due to the format
				// of the file itself, which stores the value of the Key within the Value part as well,
				// we decide to keep the input Key as Output Key, in order to keep the Sequence File format
				// both in the input file and in the output file, so that another MapReduce Job might be
				// chained after this Selection and making use of the Sequence File Key as well:
				context.write(new Text(key), new Text(String.join(",", arrayValues)));
			}
		}
	}

	public Selection() {
		this.input = null;
		this.output = null;
	}
	
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		// Define the new job and the name it will be given
		Job job = Job.getInstance(configuration, "Selection");
		configureJob(job,this.input, this.output);
	    // Let's run it!
	    return job.waitForCompletion(true);
	}

    public static void configureJob(Job job, String pathIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {
        job.setJarByClass(Selection.class);
		// Setting Mapper parameters:
		job.setMapperClass(SelectionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// We are not going to use reducer class, since we can perform the filter operation
		// straightforwardly in the Mapper Class, thus avoiding the transfer of lines from
		// the input file over the network (main bottleneck) that we are not going to output.

		// We define the output (same as Map Output):
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// The files the job will read from/write to:
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(pathIn));
		FileOutputFormat.setOutputPath(job, new Path(pathOut));
		// These are the parameters that we are sending to the job
		job.getConfiguration().setStrings("filterBy", "type");
		job.getConfiguration().setStrings("value", "type_1");
    }
}
