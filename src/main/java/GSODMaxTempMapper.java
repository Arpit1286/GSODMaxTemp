import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class GSODMaxTempMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private GSODTempParser parser = new GSODTempParser();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        parser.parse(value);
        if (parser.isValidTemperature()) {
            context.write(new Text(parser.getYear()), new FloatWritable(parser.getAirTemperature()));
        }
    }
}
