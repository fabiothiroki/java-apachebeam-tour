package apachebeamdemo.app;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

public class MultiplyByTwoTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testMultiplyByTwo() {
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        PCollection<Integer> output = numbers.apply(
                MapElements.into(TypeDescriptors.integers())
                        .via((Integer number) -> number * 2)
        );
        
        PAssert.that(output)
                .containsInAnyOrder(2, 4, 6, 8, 10);

        pipeline.run();
    }
}
