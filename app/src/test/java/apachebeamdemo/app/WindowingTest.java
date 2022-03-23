package apachebeamdemo.app;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

public class WindowingTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testReadInputFromFile() {
        PCollection<Integer> transactions =
                pipeline.apply(
                        Create.timestamped(
                                TimestampedValue.of(10, Instant.parse("2019-06-01T00:00:00+00:00")),
                                TimestampedValue.of(20, Instant.parse("2019-06-01T00:00:00+00:00")),
                                TimestampedValue.of(30, Instant.parse("2019-06-05T00:00:00+00:00")),
                                TimestampedValue.of(40, Instant.parse("2019-06-05T00:00:00+00:00")),
                                TimestampedValue.of(50, Instant.parse("2019-06-05T00:00:00+00:00"))
                        )
                );

        PCollection<Integer> output =
                transactions
                .apply(Window.into(FixedWindows.of(Duration.standardDays(1))))
                .apply(Combine.globally(Sum.ofIntegers()).withoutDefaults())
 ;

        PAssert.that(output)
                .inWindow(new IntervalWindow(
                        Instant.parse("2019-06-01T00:00:00+00:00"),
                        Instant.parse("2019-06-02T00:00:00+00:00")))
                .containsInAnyOrder(30);

        PAssert.that(output)
                .inWindow(new IntervalWindow(
                        Instant.parse("2019-06-05T00:00:00+00:00"),
                        Instant.parse("2019-06-06T00:00:00+00:00")))
                .containsInAnyOrder(120);

        pipeline.run();
    }
}
