import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer6 extends Reducer<Text, AppWritable6, Text, DoubleWritable> {

    public void reduce(Text key,
                       Iterable<AppWritable6> values,
                       Context con) throws IOException, InterruptedException {
        int nGlobal = 0;
        double valueGlobal = 0;
        double mediaGlobal = 0;

        for (AppWritable6 v : values){
            nGlobal += v.getN();
            valueGlobal += v.getValue();
        }

        mediaGlobal = valueGlobal/nGlobal;

        con.write(key, new DoubleWritable(mediaGlobal));

    }
}