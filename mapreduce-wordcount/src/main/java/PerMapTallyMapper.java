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

public class PerMapTallyMapper extends Mapper<Object, Text, Text, IntWritable> {

  private static final Set<Character> SELECTED_INITIALS = new HashSet<>(
      Arrays.asList('m', 'n', 'o', 'p', 'q'));

  private Text word = new Text();

  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr = new StringTokenizer(value.toString());
    Map<String, Integer> map = new HashMap<>();
    while (itr.hasMoreTokens()) {
      String s = itr.nextToken();
      if (!SELECTED_INITIALS.contains(s.toLowerCase().charAt(0))) {
        continue;
      }
      map.put(s, map.getOrDefault(s, 0) + 1);
    }
    for (String k : map.keySet()) {
      word.set(k);
      context.write(word, new IntWritable(map.get(k)));
    }
  }
}