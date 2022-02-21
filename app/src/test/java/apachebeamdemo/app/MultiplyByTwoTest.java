package apachebeamdemo.app;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class MultiplyByTwoTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testMultiplyByTwo() {
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        PCollection<Integer> output = applyTransform(numbers);

        PAssert.that(output)
                .containsInAnyOrder(2, 4, 6, 8, 10);

        pipeline.run().waitUntilFinish();
    }

    static PCollection<Integer> applyTransform(PCollection<Integer> input) {
        return input.apply(ParDo.of(new DoFn<Integer, Integer>() {

            @ProcessElement
            public void processElement(@Element Integer number, DoFn.OutputReceiver<Integer> out) {
                out.output(number * 2);
            }

        }));
    }
}
