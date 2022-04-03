
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InmemoryExample {

    public static void main(String[] args) {
        String basePath = "data";

        Pipeline pipeline = Pipeline.create();

        PCollection<CustomerEntity> list = pipeline.apply(Create.of(getCustomer()));

        PCollection pStrList = list.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity customer) -> customer.getName()));

        pStrList.apply(TextIO.write().to(String.format("%s\\section2\\inmemory.csv", basePath)).withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }

    static List<CustomerEntity> getCustomer() {

        CustomerEntity c1 = new CustomerEntity("1001", "John");
        CustomerEntity c2 = new CustomerEntity("1002", "Adam");

        List<CustomerEntity> list = new ArrayList<CustomerEntity>();
        list.add(c1);
        list.add(c2);

        return list;
    }
}
