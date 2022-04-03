import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.joda.time.Duration;

import java.sql.PreparedStatement;


public class RealTimeStreamingETLPrintOut {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        p
            .apply(KafkaIO.<Long,IotEvent>read()
                    .withBootstrapServers("localhost:9092")
                    .withTopic("beamtopic")
                    .withKeyDeserializer(LongDeserializer.class)
                    .withValueDeserializer(IotDeserializer.class)
                    .withoutMetadata()
            )
            .apply(Values.<IotEvent>create())
            .apply(Window.<IotEvent>into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(ParDo.of(new DoFn<IotEvent, String>() {

                @ProcessElement
                public void processElement(ProcessContext c) {

                    if(c.element().getTemperature()>80.0) {
                        c.output(c.element().getDeviceId());
                    }
                }
            }))
            .apply(Count.perElement())
            .apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                        System.out.println(c.element());
                }
            }));

        p.run();
    }
}
