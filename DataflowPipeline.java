import com.google.api.client.util.Charsets;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.io.ByteSource;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.*;

public class DataflowPipeline {
    private static final TupleTag<KV<Destination, KV<String, String>>> CSV_LINES = new TupleTag<KV<Destination, KV<String, String>>> () {
    };

    private static final TupleTag<KV<Destination, Iterable<KV<String, String>>>> VALIDATION_OUTPUT = new TupleTag<KV<Destination, Iterable<KV<String, String>>>>() {
    };

    private static final TupleTag<DataflowError> READ_CSV_ERROR = new TupleTag<DataflowError>() {
    };

    private static final TupleTag<DataflowError> VALIDATION_ERROR = new TupleTag<DataflowError>() {
    };


    public static void main(String[] args) {
        PipelineOptionsFactory.register(DynamicPipelineOptions.class);
        DynamicPipelineOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(DynamicPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> filenames = pipeline.apply(Create.ofProvider(options.getInputFiles(), StringUtf8Coder.of())).apply(ParDo.of(new GenerateFilenames()));
        List<PCollection<DataflowError>> errorCollections = new ArrayList<>();
        PCollection<KV<Destination, String>> readFiles = filenames
                .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply(FileIO.readMatches())
                .apply(MapElements.via(new InferableFunction<FileIO.ReadableFile, KV<Destination, String>>() {
                    @Override
                    public KV<Destination, String> apply(FileIO.ReadableFile input) throws Exception {
                        InputStream inputStream = Channels.newInputStream(input.open());
                        ByteSource byteSource = new ByteSource() {
                            @Override
                            public InputStream openStream() throws IOException {
                                return inputStream;
                            }
                        };
                        String text = byteSource.asCharSource(Charsets.ISO_8859_1).read();
                        return KV.of(new Destination(input.getMetadata().resourceId().toString()), text);
                    }
                }).exceptionsInto(TypeDescriptor.of(DataflowError.class))
                        .exceptionsVia(input -> new DataflowError(input.exception(),
                                input.element().getMetadata().resourceId().getFilename(),
                                Destination.rawDestination(input.element().getMetadata().resourceId().getFilename()))))
                .failuresTo(errorCollections);
        PCollectionTuple readCSVPCollectionTuple = readFiles.apply("Read CSV", ParDo.of(new ReadCSV(CSV_LINES, READ_CSV_ERROR, options.getDelimiter()))
                .withOutputTags(CSV_LINES, TupleTagList.of(READ_CSV_ERROR)));
        PCollectionTuple validationPCollectionTuple = readCSVPCollectionTuple.get(CSV_LINES)
                .apply("Validate", new ValidateData(VALIDATION_OUTPUT, VALIDATION_ERROR, options.getConfigPath()));
        Collections.addAll(errorCollections, readCSVPCollectionTuple.get(READ_CSV_ERROR), validationPCollectionTuple.get(VALIDATION_ERROR));
        Set<BigQueryIO.Write.SchemaUpdateOption> schemaUpdateOptions = new HashSet<>();
        schemaUpdateOptions.add(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_ADDITION);
//        schemaUpdateOptions.add(BigQueryIO.Write.SchemaUpdateOption.ALLOW_FIELD_RELAXATION);
        WriteResult writeResult = validationPCollectionTuple.get(VALIDATION_OUTPUT).apply(BigQueryIO.<KV<Destination, Iterable<KV<String, String>>>>write()
                .withFormatFunction(input -> {
                    TableRow tableRow = new TableRow();
                    input.getValue().forEach(stringStringKV -> {
                                tableRow.set(stringStringKV.getKey(), stringStringKV.getValue());
                            }
                    );
                    return tableRow;
                })
                .withSchemaUpdateOptions(schemaUpdateOptions)
                .skipInvalidRows()
                .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                .withExtendedErrorInfo()
                .optimizedWrites()
                .withCustomGcsTempLocation(options.getTempGCSFileLocation())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .to(input -> new TableDestination(input.getValue().getKey().getBigQueryTableReference().toTableReference(), null)));
        PCollection<DataflowError> bigQueryInsertErrors = writeResult.getFailedInsertsWithErr().apply(MapElements.into(TypeDescriptor.of(DataflowError.class))
                .via((BigQueryInsertError input) ->
                        new DataflowError(input.getRow(), input.getError(), input.getTable())
                ));
        Collections.addAll(errorCollections, bigQueryInsertErrors, readCSVPCollectionTuple.get(READ_CSV_ERROR), validationPCollectionTuple.get(VALIDATION_ERROR));
        PCollectionList.of(errorCollections).apply("Flatten Errors", Flatten.pCollections())
                .apply(BigQueryIO.<DataflowError>write()
                        .to(options.getErrorTable())
                        .withFormatFunction(DataflowError::convertToTableRow)
                        .withCustomGcsTempLocation(options.getTempGCSFileLocation())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withSchema(new TableSchema().setFields(Arrays.asList(
                                new TableFieldSchema().setName("Timestamp").setType("TIMESTAMP").setMode("REQUIRED"),
                                new TableFieldSchema().setName("Data").setType("STRING"),
                                new TableFieldSchema().setName("Message").setType("STRING"),
                                new TableFieldSchema().setName("StackTrace").setType("STRING"),
                                new TableFieldSchema().setName("ExceptionType").setType("STRING"),
                                new TableFieldSchema().setName("File").setType("STRING")))));
        pipeline.run();
    }

    private static class GenerateFilenames extends DoFn<String, String> implements Serializable {

        @ProcessElement
        public void processElement(ProcessContext context) throws RuntimeException {
            Arrays.asList(context.element().split(",")).forEach(context::output);
        }
    }

    public interface DynamicPipelineOptions extends PipelineOptions, GcpOptions, DataflowPipelineOptions {
        @Description("Temporary Location for Dataflow Files")
        ValueProvider<String> getTempGCSFileLocation();

        void setTempGCSFileLocation(ValueProvider<String> fileLocation);


        @Description("Delimiter to use for csv parsing")
        @Default.String(",")
        ValueProvider<String> getDelimiter();

        void setDelimiter(ValueProvider<String> delimiter);


        @Description("BigQuery table to write errors to")
        @Validation.Required
        ValueProvider<String> getErrorTable();

        void setErrorTable(ValueProvider<String> errorTable);


        @Description("Path to config file containing details of the csv files to parse")
        @Validation.Required
        ValueProvider<String> getConfigPath();

        void setConfigPath(ValueProvider<String> configPath);

        @Description("List of input file locations")
        @Validation.Required
        ValueProvider<String> getInputFiles();

        void setInputFiles(ValueProvider<String> inputFiles);
    }

}
