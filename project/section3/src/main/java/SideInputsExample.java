
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Map;


public class SideInputsExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        PCollection<KV<String, String>> pReturn =   p.apply(TextIO.read().from("C:\\Users\\shlomi_e\\apache-beam-course-materials\\project\\data\\section3\\return.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void process(ProcessContext c){
                        String arr[] = c.element().split(",");
                        c.output(KV.of(arr[0], arr[1]));
                    }
                }));

        PCollectionView<Map<String, String>> pMap = pReturn
                .apply(View.asMap());

        PCollection<String> pCustList = p.apply(TextIO.read().from("C:\\Users\\shlomi_e\\apache-beam-course-materials\\project\\data\\section3\\cust_order.csv"));

        pCustList.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c){
                Map<String, String> pSideInputView = c.sideInput(pMap);
                String arr[] = c.element().split(",");
                String custName = pSideInputView.get(arr[0]);

                if(custName == null) {
                    System.out.println(c.element());
                }
            }
        }).withSideInputs(pMap));

        p.run();
    }
}
