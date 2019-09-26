import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AppMapper8 extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context con) throws IOException,
            InterruptedException {
        // conversão do conteudo em string
        String conteudo = value.toString();

        //split por virgula
        String[] palavras = conteudo.split(";");

        if (!palavras[1].equals("") && !palavras[1].equals("year") && !palavras[4].equals("")
                && !palavras[4].equals("flow")){
            String ano = palavras[1];
            String fluxo = palavras[4];

            // emitir (chave, valor)

            // mandando para o reduce

            con.write(new Text("Ano: " + ano +" Fluxo: " + fluxo), new IntWritable(1));
        }
    }
}