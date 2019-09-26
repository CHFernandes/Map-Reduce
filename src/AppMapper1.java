import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AppMapper1 extends Mapper<Object, Text, Text, DoubleWritable> {

    public void map(Object key, Text value, Context con) throws IOException,
            InterruptedException {
        // conversão do conteudo em string
        String conteudo = value.toString();

        //split por virgula
        String[] palavras = conteudo.split(";");

        String pais = palavras[0];

        if (pais.equals("Brazil")){
            if (!palavras[8].equals("")){
                double transacao = Double.parseDouble(palavras[8]);
                String mercadoria = palavras[3];

                // emitir (chave, valor)

                // mandando para o reduce

                con.write(new Text(mercadoria), new DoubleWritable(transacao));
            }
        }
    }
}
