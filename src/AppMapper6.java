import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AppMapper6 extends Mapper<Object, Text, Text, AppWritable6> {

    public void map(Object key, Text value, Context con) throws IOException,
            InterruptedException {
        // conversão do conteudo em string
        String conteudo = value.toString();

        //split por virgula
        String[] palavras = conteudo.split(";");

        String pais = palavras[0];

        if (pais.equals("Brazil")){
            if (!palavras[6].equals("") && !palavras[6].equals("weight_kg")){
                double peso = Double.parseDouble(palavras[6]);
                String ano = palavras[1];
                double valor = Double.parseDouble(palavras[5]);

                // emitir (chave, valor)

                // mandando para o reduce

                con.write(new Text("Peso: "+ peso + " Ano: " + ano), new AppWritable6(1, valor));
            }
        }
    }
}
