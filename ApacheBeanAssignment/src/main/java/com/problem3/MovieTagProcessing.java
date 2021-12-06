package com.problem3;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MovieTagProcessing {


    private static final String CSV_HEADER = "userId,movieId,tag";

    public static void main(String[] args) {

        final MovieTagProcessingOptions averagePriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MovieTagProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(averagePriceProcessingOptions);

        pipeline.apply("Read-Lines", TextIO.read()
                        .from(averagePriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(Integer.parseInt(tokens[0]),tokens[2]);
                        }))
                //.apply("AverageAggregation", Mean.perKey())
                .apply("Count",Count.perElement())
                .apply("Format-result1", MapElements
                        .into(TypeDescriptors.strings())
                        .via(movieCount -> movieCount.getKey()+ "," + movieCount.getValue()))
                .apply("Format-result2", MapElements
                        .into(TypeDescriptors.strings())
                        .via(movieCount -> movieCount))
                .apply("WriteResult", TextIO.write()
                        .to(averagePriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("userId,movie_tag,tag_count"));

        pipeline.run();
        System.out.println("pipeline executed successfully");
    }

    public interface MovieTagProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/movie_tags.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/Output/result")
        String getOutputFile();

        void setOutputFile(String value);
    }
}