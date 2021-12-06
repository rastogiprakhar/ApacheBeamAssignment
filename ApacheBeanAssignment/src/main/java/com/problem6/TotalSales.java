package com.problem6;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

class TotalSales {


    private static final String CSV_HEADER = "Transaction_date,Product,Price,Payment_Type,Name,City,State,Country,Account_Created,Last_Login,Latitude,Longitude,US Zip";

    public static void main(String[] args) {


        final PriceProcessingOptions PriceProcessingOptions = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PriceProcessingOptions.class);

        Pipeline pipeline = Pipeline.create(PriceProcessingOptions);

        pipeline.apply("Read-Lines", TextIO.read()
                        .from(PriceProcessingOptions.getInputFile()))
                .apply("Filter-Header", Filter.by((String line) ->
                        !line.isEmpty() && !line.contains(CSV_HEADER)))
                .apply("Map", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                        .via((String line) -> {
                            String[] tokens = line.split(",");
                            return KV.of(line.split("/")[1], Integer.parseInt(tokens[2]));
                        }))
                .apply("Combine", Sum.integersPerKey())
                .apply("Format-result", MapElements
                        .into(TypeDescriptors.strings())
                        .via(carCount -> carCount.getKey() + "," + carCount.getValue()))
                .apply("WriteResult", TextIO.write()
                        .to(PriceProcessingOptions.getOutputFile())
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader("date,price"));

        pipeline.run();
        System.out.println("pipeline executed successfully");
    }

    public interface PriceProcessingOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("src/main/resources/SalesJan2009.csv")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write")
        @Default.String("src/main/resources/Output/TotalSales")
        String getOutputFile();

        void setOutputFile(String value);
    }
}