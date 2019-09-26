import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AppReducer7 extends Reducer<Text, AppWritable7, Text, AppWritable7> {

    public void reduce(Text key,
                       Iterable<AppWritable7> values,
                       Context con) throws IOException, InterruptedException {

        String commodityGlobal = "Nada";
        double mediaGlobal = 0;

        for (AppWritable7 v : values){
            if (v.getMedia() > mediaGlobal && v.getMedia() != Double.POSITIVE_INFINITY){
                mediaGlobal = v.getMedia();
                commodityGlobal = v.getCommodity();
            }
        }

        con.write(key, new AppWritable7(commodityGlobal, mediaGlobal));

    }
}