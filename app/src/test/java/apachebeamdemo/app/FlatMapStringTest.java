package apachebeamdemo.app;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class FlatMapStringTest {

    static final String[] WORDS_ARRAY = new String[] {
            "hi bob", "hello alice"};

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
                .containsInAnyOrder("hi", "bob", "hello", "alice");

        pipeline.run();
    }
}
