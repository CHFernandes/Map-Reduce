import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer1 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key,
                       Iterable<DoubleWritable> values,
                       Context con) throws IOException, InterruptedException {
        int transacaoGlobal = 0;

        for (DoubleWritable v : values){
            transacaoGlobal += v.get();
        }

        con.write(key, new DoubleWritable(transacaoGlobal));

    }
}
