import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.util.Strings;

public class FlightDataMapper extends Mapper<Object, TextArrayWritable, Text, Text> {

  private static final int YEAR_INDEX = 0;
  private static final int FLIGHT_DATE_INDEX = 5;
  private static final int ORIGIN_CITY_INDEX = 11;
  private static final int DES_CITY_INDEX = 17;
  private static final int DEP_TIME_INDEX = 24;
  private static final int ARR_TIME_INDEX = 35;
  private static final int ARR_DELAY_MIN_INDEX = 37;
  private static final int CANCELLED_INDEX = 41;
  private static final int DIVERTED_INDEX = 44;

  @Override
  protected void map(Object key, TextArrayWritable value, Mapper<Object, TextArrayWritable, Text, Text>.Context context)
      throws IOException, InterruptedException {

    final Writable[] data = value.get();

    if (!Character.isDigit(data[YEAR_INDEX].toString().charAt(0))) {
      return;
    }

    final String flightDate = data[FLIGHT_DATE_INDEX].toString();
    if (flightDate.compareTo("2007-06-01") < 0 || flightDate.compareTo("2008-06-01") >= 0) {
      return;
    }

    final String originCity = data[ORIGIN_CITY_INDEX].toString();
    final String desCity = data[DES_CITY_INDEX].toString();

    if (!originCity.equals("ORD") && !desCity.equals("JFK")) {
      return;
    }

    final String flightTime = originCity.equals("ORD")
        ? data[ARR_TIME_INDEX].toString()
        : data[DEP_TIME_INDEX].toString();

    final String anotherCity = originCity.equals("ORD") ? originCity : desCity;
    final String midCity = originCity.equals("ORD") ? desCity : originCity;
    final String outputMapKey = midCity + "," + flightDate;

    final String join = Strings.join(Arrays.asList(
        anotherCity,
        flightTime,
        data[ARR_DELAY_MIN_INDEX].toString()), ',');
    context.write(new Text(outputMapKey), new Text(join));
  }
}
