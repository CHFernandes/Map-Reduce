import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer4 extends Reducer<Text, AppWritable4, Text, DoubleWritable> {

    public void reduce(Text key,
                       Iterable<AppWritable4> values,
                       Context con) throws IOException, InterruptedException {

        double pesoGlobal = 0;
        int nGlobal = 0;


        for (AppWritable4 v : values) {
            nGlobal += v.getN();
            pesoGlobal += v.getPeso();
        }

        double media = pesoGlobal / nGlobal;

        DoubleWritable saida = new DoubleWritable(media);

        con.write(key, saida);
    }
}