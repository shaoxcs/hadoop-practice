import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PerTaskTallyMapper extends Mapper<Object, Text, Text, IntWritable> {

  private static final Set<Character> SELECTED_INITIALS = new HashSet<>(
      Arrays.asList('m', 'n', 'o', 'p', 'q'));

  private Text word;
  private Map<String, Integer> map;

  @Override
  protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context) {
    this.map = new HashMap<>();
    this.word = new Text();
  }

  @Override
  protected void cleanup(Mapper<Object, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException {
    for (String k : map.keySet()) {
      word.set(k);
      context.write(word, new IntWritable(map.get(k)));
    }
  }

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    while (itr.hasMoreTokens()) {
      String s = itr.nextToken();
      if (!SELECTED_INITIALS.contains(s.toLowerCase().charAt(0))) {
        continue;
      }
      map.put(s, map.getOrDefault(s, 0) + 1);
    }
  }
}