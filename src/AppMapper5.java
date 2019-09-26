import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper5 extends Mapper<Object, Text, Text, AppWritable5> {

    public void map(Object key, Text value, Context con)
            throws IOException, InterruptedException {

        String content = value.toString();

        String[] palavras = content.split(";");


        // verificando se esta vazio e com a escrita certa
        if(!palavras[6].equals("") && !palavras[6].equals("weight_kg") && palavras[0].equals("Brazil")) {
            // para ficar em ordem mercadoria, depois ano
            Text chave = new Text("Mercadoria: " + palavras[3] +"   Ano: "+ palavras[1] + "   Peso: ");

            // O valor é o peso + n
            AppWritable5 valor = new AppWritable5(1, Double.parseDouble(palavras[6]));


            // Passando isso pro reduce e verificando se é só o Brasil

            con.write(chave, valor);
        }



    }
}