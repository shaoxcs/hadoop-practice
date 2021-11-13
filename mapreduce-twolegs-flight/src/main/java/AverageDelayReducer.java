import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageDelayReducer extends Reducer<Text, Text, Text, Text> {

  // [key="midCity,date", value="anotherCity,time,delayTime"]
  private static final int ANOTHER_CITY_INDEX = 0;
  private static final int TIME_INDEX = 1;
  private static final int DELAY_INDEX = 2;

  private List<Flight> firstLegs;
  private List<Flight> secondLegs;
  private double sum;
  private int countPair;

  public void reduce(Text key, Iterable<Text> values, Context context) {
    this.firstLegs = new ArrayList<>();
    this.secondLegs = new ArrayList<>();
    values.forEach(val -> {
      final String[] data = val.toString().split(",");
      try {
        final String anotherCity = data[ANOTHER_CITY_INDEX];
        final String time = data[TIME_INDEX];
        final double delay = Double.parseDouble(data[DELAY_INDEX]);

        if (anotherCity.equals("ORD")) {
          firstLegs.add(new Flight(time, delay));
        } else {
          secondLegs.add(new Flight(time, delay));
        }

      } catch (NumberFormatException | ArrayIndexOutOfBoundsException ignored) {

      }
    });

    firstLegs.sort(Comparator.comparing(a -> a.time));
    secondLegs.sort(Comparator.comparing(a -> a.time));

    for (Flight second : secondLegs) {
      final String leaveTime = second.time;
      for (Flight first : firstLegs) {
        final String arrTime = first.time;
        if (arrTime.compareTo(leaveTime) >= 0) {
          break;
        }
        sum += second.delayTime + first.delayTime;
        countPair += 1;
      }
    }
  }

  @Override
  protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
      throws IOException, InterruptedException {
    final String key = countPair + " pairs";
    context.write(new Text(key), new Text("" + sum / countPair));
    super.cleanup(context);
  }

  static class Flight {
    String time;
    double delayTime;

    public Flight(String time, double delayTime) {
      this.time = time;
      this.delayTime = delayTime;
    }
  }
}
