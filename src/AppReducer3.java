import jdk.nashorn.internal.objects.Global;
import org.apache.commons.math3.geometry.partitioning.utilities.OrderedTuple;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.hash.Hash;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class AppReducer3 extends Reducer<Text, AppWritable3, Text, AppWritable3> {

    public void reduce(Text key,
                       Iterable<AppWritable3> values,
                       Context con) throws IOException, InterruptedException {

        String commodityGlobal = "Nada";

        HashMap<String, Integer> commodities = new HashMap<String, Integer>();

        // para cada valor
        for (AppWritable3 v : values) {
            if(commodities.containsKey(v.getCommodity())){
                int c = commodities.get(v.getCommodity()) + 1;
                commodities.put(v.getCommodity(), c);
            }else {
                commodities.put(v.getCommodity(), 1);
            }
        }

        int contador;
        int contadorGlobal = 0;

        for (String i : commodities.keySet()) {
            contador = commodities.get(i);
            if(contador > contadorGlobal){
                contadorGlobal = contador;
                commodityGlobal = i;
            }
        }

        AppWritable3 saida = new AppWritable3(contadorGlobal, commodityGlobal);

        con.write(key, saida);
    }
}