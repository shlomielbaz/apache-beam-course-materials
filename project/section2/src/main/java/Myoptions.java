
import org.apache.beam.sdk.options.PipelineOptions;

public interface Myoptions extends PipelineOptions {

    String getInputFile();

    void setInputFile(String file);

    String getOutputFile();

    void setOutputFile(String file);

    String getExtn();

    void setExtn(String file);
}

