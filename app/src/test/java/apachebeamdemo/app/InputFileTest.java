package apachebeamdemo.app;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class InputFileTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testReadInputFromFile() {
        PCollection<String> input =
                pipeline.apply(TextIO.read().from("./src/main/resources/words.txt"));

        PCollection<String> output = input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split(" ")))
        );

        PAssert.that(output)
                .containsInAnyOrder("An", "advanced", "unified", "programming", "model");

        pipeline.run();
    }
}
