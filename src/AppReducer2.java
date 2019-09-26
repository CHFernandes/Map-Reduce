import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    public void reduce(IntWritable key,
                       Iterable<IntWritable> values,
                       Context con) throws IOException, InterruptedException {
        int transacaoGlobal = 0;

        for (IntWritable v : values){
            transacaoGlobal += v.get();
        }

        con.write(key, new IntWritable(transacaoGlobal));

    }
}

