import au.com.bytecode.opencsv.CSVParser;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

/**
 * An {@link org.apache.hadoop.mapreduce.InputFormat} for CSV
 * plain text files.  Keys are byte offsets in
 * the file, and values are {@link org.apache.hadoop.io.ArrayWritable}'s with tokenized
 * values.
 */
public class CSVInputFormat extends FileInputFormat<LongWritable, TextArrayWritable> {

  public static String CSV_TOKEN_SEPARATOR_CONFIG = "csvinputformat.token.delimiter";

  @Override
  public RecordReader<LongWritable, TextArrayWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    String csvDelimiter = taskAttemptContext.getConfiguration().get(CSV_TOKEN_SEPARATOR_CONFIG);

    Character separator = null;
    if(csvDelimiter != null && csvDelimiter.length() == 1) {
      separator = csvDelimiter.charAt(0);
    }

    return new CSVRecordReader(separator);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    CompressionCodec codec =
        new CompressionCodecFactory(context.getConfiguration())
            .getCodec(filename);
    return codec == null;
  }

  public static class CSVRecordReader extends RecordReader<LongWritable, TextArrayWritable> {

    private LineRecordReader reader;
    private TextArrayWritable value;
    private final CSVParser parser;

    public CSVRecordReader(Character character) {
      reader = new LineRecordReader();

      if (character == null) {
        parser = new CSVParser();
      } else {
        parser = new CSVParser(character);
      }
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
      reader.initialize(inputSplit, taskAttemptContext);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (reader.nextKeyValue()) {
        this.value = getValue();
        return true;
      } else {
        value = null;
        return false;
      }
    }

    private TextArrayWritable getValue() throws IOException {
      String line = reader.getCurrentValue().toString();
      String[] tokens = parser.parseLine(line);

      return new TextArrayWritable(convert(tokens));
    }

    private Text[] convert(String[] s) {
      Text t[] = new Text[s.length];
      for(int i=0; i < t.length; i++) {
        t[i] = new Text(s[i]);
      }
      return t;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return reader.getCurrentKey();
    }

    @Override
    public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return reader.getProgress();
    }
  }
}