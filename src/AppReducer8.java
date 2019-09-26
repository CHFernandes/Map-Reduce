import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer8 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key,
                       Iterable<IntWritable> values,
                       Context con) throws IOException, InterruptedException {

        int nGlobal = 0;

        for (IntWritable v : values){
            nGlobal += v.get();
        }

        con.write(key, new IntWritable(nGlobal));

    }
}