package lab.cards.mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Mapper<input key type, input value type, output key type, output value type>
public class RecordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	//The first two input parameters for map
	//should match the input parameters mentioned 
	//in Mapper Generics mentioned top
	@Override
	public void map(LongWritable key, Text record, Context context) 
		throws IOException, InterruptedException {
		context.write(new Text("count"), new IntWritable(1));
	}
	
}
