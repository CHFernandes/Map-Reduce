import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper3 extends Mapper<Object, Text, Text, AppWritable3> {

    public void map(Object key, Text value, Context con)
            throws IOException, InterruptedException {

        String content = value.toString();

        String[] palavras = content.split(";");


        // palavras[3] é a mercadoria
        Text chave = new Text(palavras[3]);

        // só se o pais for Brasil
        if(palavras[0].equals("Brazil") && palavras[1].equals("2016") && !palavras[3].equals("ALL COMMODITIES")) {
            String mercadoria = palavras[3];

            AppWritable3 saida = new AppWritable3(1, mercadoria);

            //passar para o reduce
            con.write(new Text("Mercadoria com a maior quantidade de transações: "), saida);

        }
    }
}