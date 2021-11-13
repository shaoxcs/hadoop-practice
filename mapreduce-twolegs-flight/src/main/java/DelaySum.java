import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DelaySum {

  public static void main(String[] args) throws Exception {
    System.setProperty("hadoop.home.dir", "/");
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "delay sum");
    job.setJarByClass(DelaySum.class);
    job.setMapperClass(FlightDataMapper.class);
    job.setReducerClass(AverageDelayReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass(CSVInputFormat.class);
    CSVInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}