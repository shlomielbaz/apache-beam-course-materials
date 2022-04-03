

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {
    public static void main(String[] args) {
        Myoptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Myoptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> output = pipeline.apply(TextIO.read().from(options.getInputFile()));

        output.apply(TextIO.write().to(options.getOutputFile()).withNumShards(1).withSuffix(options.getExtn()));

        pipeline.run();
    }
}

// String basePath = "data\\section2\\outpu.csv";
// String.format("%s\\section2\\input.csv", basePath)
// String.format("%s\\section2\\input.csv", basePath)
// ".csv"