import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper7 extends Mapper<Object, Text, Text, AppWritable7> {
    public void map(Object key, Text value, Context con) throws IOException,
            InterruptedException {
        // conversão do conteudo em string
        String conteudo = value.toString();

        //split por virgula
        String[] palavras = conteudo.split(";");

        if (!palavras[3].equals("") && !palavras[3].equals("commodity") && !palavras[6].equals("")
                && !palavras[5].equals("")){
            String commodity = palavras[3];
            double peso = Double.parseDouble(palavras[6]);
            double valor = Double.parseDouble(palavras[5]);
            double media = valor/peso;

            // emitir (chave, valor)

            // mandando para o reduce

            con.write(new Text("Maior preço por peso: "), new AppWritable7(commodity, media));
        }
    }
}