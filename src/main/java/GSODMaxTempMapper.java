import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Arpit on 2/20/2015.
 */
public class GSODMaxTempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private GSODTempParser parser = new GSODTempParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getyear()), new FloatWritable(parser.getAirTemperature()));
        }
    }
}
