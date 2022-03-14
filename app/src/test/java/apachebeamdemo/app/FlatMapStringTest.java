package apachebeamdemo.app;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FlatMapStringTest {

    static final String[] WORDS_ARRAY = new String[] {
            "hi bob", "hello alice", "hi sue"};

    static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testReturnsSingleArrayOfLowerCaseStrings() {
        PCollection<String> input = pipeline.apply(Create.of(WORDS));

        PCollection<String> output = input.apply(
                FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split(" ")))
        );

        PAssert.that(output)
                .containsInAnyOrder("hi", "bob", "hello", "alice", "hi", "sue");

        pipeline.run();
    }

    @Test
    public void testCountEachWord() {
        PCollection<String> input = pipeline.apply(Create.of(WORDS));

        PCollection<KV<String, Long>> output = input
                .apply(
                    FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split(" ")))
                )
                .apply(Count.<String>perElement());

        PAssert.that(output)
                .containsInAnyOrder(
                        KV.of("hi", 2L),
                        KV.of("hello", 1L),
                        KV.of("alice", 1L),
                        KV.of("sue", 1L),
                        KV.of("bob", 1L));

        pipeline.run();
    }
}
