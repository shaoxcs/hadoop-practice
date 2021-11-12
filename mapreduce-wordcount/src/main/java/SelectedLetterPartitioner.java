import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SelectedLetterPartitioner extends Partitioner<Text, IntWritable> {

  private static final Map<Character, Integer> map = generateMap();

  private static Map<Character, Integer> generateMap() {
    Map<Character, Integer> map = new HashMap<>();
    map.put('m', 0);
    map.put('n', 1);
    map.put('o', 2);
    map.put('p', 3);
    map.put('q', 4);
    return map;
  }

  @Override
  public int getPartition(final Text key, final IntWritable value, final int i) {
    return map.getOrDefault(key.toString().toLowerCase().charAt(0), 0);
  }
}
