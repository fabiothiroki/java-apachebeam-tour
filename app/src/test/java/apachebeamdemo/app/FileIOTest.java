package apachebeamdemo.app;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

public class FileIOTest {

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

    @Test
    public void testWriteOutputToFile() {
        PCollection<String> input =
                pipeline.apply(TextIO.read().from("./src/main/resources/words.txt"));

        PCollection<KV<String, Long>> output = input
                .apply(
                    FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split(" ")))
                )
                .apply(Count.<String>perElement());;

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("An", 1L),
                        KV.of("advanced", 1L),
                        KV.of("unified", 1L),
                        KV.of("programming", 1L),
                        KV.of("model", 1L)
                );

        output
                .apply(
                        MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> kv) -> kv.getKey() + " " + kv.getValue()))
                .apply(TextIO.write().to("./src/main/resources/wordscount"));

        pipeline.run();
    }
}
