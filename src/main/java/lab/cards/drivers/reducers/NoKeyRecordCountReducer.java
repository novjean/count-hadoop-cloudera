package lab.cards.drivers.reducers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Inut of reducer should match the output types of mapper
public class NoKeyRecordCountReducer extends
	Reducer<Text, IntWritable, NullWritable, IntWritable>{

	@Override
	public void reduce(Text key, Iterable<IntWritable> records, Context context) 
		throws IOException, InterruptedException{
		int sum = 0;
		
		for(IntWritable record : records){
			sum += record.get();
		}
		 
		context.write(NullWritable.get(), new IntWritable(sum));
	}
}
