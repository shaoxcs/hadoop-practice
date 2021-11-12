import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SelectedLetterMapper extends Mapper<Object, Text, Text, IntWritable> {

  private static final Set<Character> SELECTED_INITIALS = new HashSet<>(Arrays.asList(
      'm', 'n', 'o', 'p', 'q'));

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String s = itr.nextToken();
      if (!SELECTED_INITIALS.contains(s.toLowerCase().charAt(0))) {
        continue;
      }
      word.set(s);
      context.write(word, one);
    }
  }
}