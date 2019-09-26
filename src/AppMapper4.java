import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper4 extends Mapper<Object, Text, Text, AppWritable4> {

    public void map(Object key, Text value, Context con)
            throws IOException, InterruptedException {

        String content = value.toString();

        String[] palavras = content.split(";");


        // verificando se esta vazio e com a escrita certa
        if(!palavras[6].equals("") && !palavras[6].equals("weight_kg")) {
            // para ficar em ordem mercadoria, depois ano
            Text chave = new Text("Ano: " + palavras[1] +" Mercadoria: "+ palavras[3]);

            // O valor é o peso + n
            AppWritable4 valor = new AppWritable4(1, Double.parseDouble(palavras[6]));

            // Passando isso pro reduce
            con.write(chave, valor);
        }
    }
}