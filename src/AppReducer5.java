import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer5 extends Reducer<Text, AppWritable5, Text, DoubleWritable> {

    public void reduce(Text key,
                       Iterable<AppWritable5> values,
                       Context con) throws IOException, InterruptedException {

        float pesoGlobal = 0;
        float nGlobal = 0;


        for (AppWritable5 v : values) {
            nGlobal += v.getN();
            pesoGlobal += v.getPeso();
        }

        float media = pesoGlobal / nGlobal;

        DoubleWritable saida = new DoubleWritable(media);

        con.write(key, saida);
    }
}