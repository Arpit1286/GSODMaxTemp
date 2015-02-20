import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by Arpit on 2/20/2015.
 */
public class GSODMaxTemp extends Configured implements Tool {

    @Override
    public int run(String args[]) throws Exception {
        if (args.length != 2) {
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }

        Job job = new Job(getConf(), "GSODMaxTemp");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(GSODMaxTempMapper.class);
        job.setReducerClass(GSODMaxTempReducer.class);
        job.setCombinerClass(GSODMaxTempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);



        return job.waitForCompletion(true) ? 0 : 1;
     }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GSODMaxTemp(), args);
        System.exit(exitCode);
    }
}
