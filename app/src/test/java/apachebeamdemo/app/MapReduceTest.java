package apachebeamdemo.app;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class MapReduceTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testMultiplyByTwoAndSum() {
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        PCollection<Integer> output = numbers
                .apply(
                    MapElements.into(TypeDescriptors.integers())
                        .via((Integer number) -> number * 2))
                .apply(Sum.integersGlobally());

        PAssert.that(output)
                .containsInAnyOrder(30);

        pipeline.run();
    }
}
