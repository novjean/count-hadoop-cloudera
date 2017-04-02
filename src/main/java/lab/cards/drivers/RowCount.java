package lab.cards.drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import lab.cards.drivers.reducers.NoKeyRecordCountReducer;
import lab.cards.mappers.RecordMapper;

/*
 * select count(1) from <table_name>
 */
public class RowCount extends Configured implements Tool {

	public static void main(String[] args) throws Exception{
		//new RowCount() is where the run method is defined
		int exitCode = ToolRunner.run(new RowCount(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		//Creating a job object which get the Hadoop cluster configuration
		//the text is the description of the job
		Job job = Job.getInstance(getConf(),
				"Row Count using built in mappers and reducers");
		
		//Specify the class where the mapper and reducer function is written
		//getClass() will give RowCount
		job.setJarByClass(getClass());	
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//Setting up the Mapper
		job.setMapperClass(RecordMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);	//Customized serializable
		
		//Setting up the Reducer
		job.setReducerClass(NoKeyRecordCountReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileOutputFormat.setOutputPath(job,	new Path(args[1]));		
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
