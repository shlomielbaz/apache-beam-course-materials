import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementsExample {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        PCollection<String> pCustList= p.apply(TextIO.read().from("C:\\Users\\shlomi_e\\apache-beam-course-materials\\project\\data\\section3\\customer.csv"));

        //Using TypeDescriptors

        PCollection<String> pOutput=pCustList.apply(MapElements.into(TypeDescriptors.strings()).via((String obj) -> obj.toUpperCase()));

        pOutput.apply(TextIO.write().to("C:\\Users\\shlomi_e\\apache-beam-course-materials\\project\\data\\section3\\cust_output.csv").withNumShards(1).withSuffix(".csv"));

        p.run();

    }
}
