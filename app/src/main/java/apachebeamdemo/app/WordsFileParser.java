package apachebeamdemo.app;

import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

public class WordsFileParser extends PTransform<PCollection<String>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return input
                .apply(FlatMapElements.into(TypeDescriptors.strings())
                    .via((String line) -> Arrays.asList(line.split(" ")))
                );
    }
}
