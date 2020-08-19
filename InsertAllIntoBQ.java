import com.google.api.client.util.Lists;
import com.google.cloud.bigquery.storage.v1beta1.AvroProto;
import com.google.cloud.bigquery.storage.v1beta1.BigQueryStorageClient;
import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.yaml.snakeyaml.Yaml;
import org.apache.commons.lang3.ArrayUtils;

import javax.annotation.Nullable;
import javax.sql.DataSource;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InsertAllIntoBQ extends PTransform<PBegin, PCollection<Void>> {

    public InsertAllIntoBQ(@Nullable String name) {
        super(name);
    }

    @Override
    public PCollection<Void> expand(PBegin input) {
        PCollection<KV<String, String>> filenames = input.apply(Create.of((Void) null))
                .apply(ParDo.of(new GenerateFilenames()));
        PCollection<KV<String, Iterable<Row>>> collatedRows = filenames
                .apply(ParDo.of(new GenerateBQStreams()))
                .apply(ParDo.of(new ReadFromBQ()))
                .apply(GroupByKey.create());
        return collatedRows.apply(ParDo.of(new InsertRowsIntoBQ()));
    }

    private static class GenerateFilenames extends DoFn<Void, KV<String, String>> implements Serializable {

        private JsonArray mappingConfig;

        private GenerateFilenames() {
        }

        @StartBundle
        public void setUp(StartBundleContext context) throws IOException {
            BQtoSQLPipeline.BQtOSQLOptions pipelineOptions = (BQtoSQLPipeline.BQtOSQLOptions) context.getPipelineOptions();
            this.mappingConfig = JsonParser.parseString(Utils.readFileFromFilesystem(pipelineOptions.getConfigMappingLocation().get()))
                    .getAsJsonArray();
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws RuntimeException, IOException {
            this.mappingConfig.forEach(jsonElement -> {
                String key = jsonElement.getAsJsonObject().get("bigqueryTable").getAsString();
                String value = jsonElement.getAsJsonObject().get("SQLTable").getAsString();
                context.output(KV.of(key, value));
            });
        }
    }

    private static class GenerateBQStreams extends DoFn<KV<String, String>, KV<String, BQReadInstantiationOutput>> implements Serializable {

        private BigQueryStorageClient bigQueryStorageClient;

        public GenerateBQStreams() {
        }

        @Setup
        public void setUp() throws IOException {
            this.bigQueryStorageClient = BigQueryStorageClient.create();
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws RuntimeException {
            KV<String, String> element = context.element();
            String bigQueryTable = element.getKey();
            String cloudSQLTable = element.getValue();
            TableReferenceProto.TableReference tableReference = TableReferenceProto.TableReference.newBuilder()
                    .setProjectId(bigQueryTable.split(":")[0])
                    .setDatasetId(bigQueryTable.split(":")[1].split(".")[0])
                    .setTableId(bigQueryTable.split(":")[1].split(".")[1])
                    .build();
            String parent = bigQueryTable.split(":")[0];
            Storage.CreateReadSessionRequest createReadSessionRequest = Storage.CreateReadSessionRequest.newBuilder()
                    .setTableReference(tableReference)
                    .setParent(parent)
                    .build();
            Storage.ReadSession response = bigQueryStorageClient.createReadSession(createReadSessionRequest);
            AvroProto.AvroSchema avroSchema = response.getAvroSchema();
            for (Storage.Stream stream : response.getStreamsList()) {
                Storage.StreamPosition readPosition = Storage.StreamPosition
                        .newBuilder()
                        .setStream(stream)
                        .build();
                context.output(KV.of(cloudSQLTable, new BQReadInstantiationOutput(readPosition, avroSchema)));
            }
        }
    }

    private static class BQReadInstantiationOutput implements Serializable {
        private Storage.StreamPosition streamPosition;
        private AvroProto.AvroSchema schema;

        public BQReadInstantiationOutput(Storage.StreamPosition streamPosition, AvroProto.AvroSchema schema) {
            this.streamPosition = streamPosition;
            this.schema = schema;
        }

        public Storage.StreamPosition getStreamPosition() {
            return streamPosition;
        }

        public void setStreamPosition(Storage.StreamPosition streamPosition) {
            this.streamPosition = streamPosition;
        }

        public AvroProto.AvroSchema getSchema() {
            return schema;
        }

        public void setSchema(AvroProto.AvroSchema schema) {
            this.schema = schema;
        }
    }

    private static class ReadFromBQ extends DoFn<KV<String, BQReadInstantiationOutput>, KV<String, Row>> implements Serializable {

        private BigQueryStorageClient bigQueryStorageClient;

        public ReadFromBQ() {
        }

        @Setup
        public void setUp() throws IOException {
            this.bigQueryStorageClient = BigQueryStorageClient.create();
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws RuntimeException, IOException {
            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(context.element().getValue().getSchema().getSchema());
            SimpleRowReader rowReader = new SimpleRowReader(schema);
            Storage.ReadRowsRequest request = Storage.ReadRowsRequest.newBuilder()
                    .setReadPosition(context.element().getValue().getStreamPosition())
                    .build();
            for (Storage.ReadRowsResponse readRowsResponse : this.bigQueryStorageClient.readRowsCallable().call(request)) {
                rowReader.processRows(readRowsResponse.getAvroRows()).forEach(genericRecord -> context
                        .output(KV.of(context.element().getKey(), genericRecord)));
            }
        }
    }

    /*
     * SimpleRowReader handles deserialization of the Avro-encoded row blocks transmitted
     * from the storage API using a generic datum decoder.
     */
    private static class SimpleRowReader {

        private final DatumReader<GenericRecord> datumReader;
        private BinaryDecoder decoder = null;
        private GenericRecord row = null;
        private Schema schema;

        public SimpleRowReader(Schema schema) {
            this.schema = schema;
            datumReader = new GenericDatumReader<>(schema);
        }

        /**
         * Sample method for processing AVRO rows which only validates decoding.
         *
         * @param avroRows object returned from the ReadRowsResponse.
         */
        public List<Row> processRows(AvroProto.AvroRows avroRows) throws IOException {
            List<Row> records = new ArrayList<>();
            decoder =
                    DecoderFactory.get()
                            .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);

            while (!decoder.isEnd()) {
                row = datumReader.read(row, decoder);
                Row beamRow = AvroUtils.toBeamRowStrict(this.row, AvroUtils.toBeamSchema(schema));
                records.add(beamRow);
            }
            return records;
        }
    }

    private static class InsertRowsIntoBQ extends DoFn<KV<String, Iterable<Row>>, Void> {

        private DataSource pool;

        @StartBundle
        public void setUp(StartBundleContext context) throws IOException {
            BQtoSQLPipeline.BQtOSQLOptions BQtOSQLOptions = (BQtoSQLPipeline.BQtOSQLOptions) context.getPipelineOptions();
            Map<String, String> configMap = readDatabaseCredentials(BQtOSQLOptions.getCredentialFileLocation().get(), BQtOSQLOptions.getProject(),
                    BQtOSQLOptions.getKMSKeyLocation().get(), BQtOSQLOptions.getKMSKeyring().get(),
                    BQtOSQLOptions.getKMSKeyName().get());
            HikariConfig config = new HikariConfig();
            config.setJdbcUrl(String.format("jdbc:postgresql:///%s", configMap.get("DB_NAME")));
            config.setUsername(configMap.get("DB_USERNAME"));
            config.setPassword(configMap.get("DB_PASSWORD"));
            if (!(BQtOSQLOptions.getCLOUD_SQL_CONNECTION_NAME().get().equals("null"))) {
                config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
                config.addDataSourceProperty("cloudSqlInstance", BQtOSQLOptions.getCLOUD_SQL_CONNECTION_NAME().get());
            }
            this.pool = new HikariDataSource(config);
        }

        @ProcessElement
        public void processElement(ProcessContext context) throws RuntimeException, SQLException {
            String preparedStatementString = generatePreparedStatement(context.element().getKey(), context.element().getValue());
            try (Connection connection = pool.getConnection()) {
                PreparedStatement preparedStatement = connection.prepareStatement(preparedStatementString);
                preparedStatement.executeUpdate();
            }

        }

        @Teardown
        public void teardown() throws Exception {
            this.pool.getConnection().close();
        }
    }

    private static Map<String, String> readDatabaseCredentials(
            String fileLocation, String projectId, String locationId, String keyRingId, String cryptoKeyId)
            throws IOException {
        MatchResult matchResult = FileSystems.match(fileLocation);
        List<ResourceId> resourceIds = matchResult.metadata().stream().map(MatchResult.Metadata::resourceId).collect(Collectors.toList());
        ReadableByteChannel byteChannel = FileSystems.open(resourceIds.get(0));
        ByteBuffer bbuf = ByteBuffer.allocate(2048);
        ArrayList<Byte> bytes = new ArrayList<>();
        while (byteChannel.read(bbuf) != -1) {
            bbuf.flip();
            Byte[] byteObject = ArrayUtils.toObject(bbuf.array());
            Collections.addAll(bytes, byteObject);
            bbuf.clear();
        }
        byte[] bytesArray = ArrayUtils.toPrimitive(bytes.toArray(new Byte[bytes.size()]));
        byte[] decryptedArray = Utils.decrypt(projectId, locationId, keyRingId, cryptoKeyId, bytesArray);
        String config = new String(decryptedArray, StandardCharsets.UTF_8);
        return new Yaml().load(config);
    }

    private static String generatePreparedStatement(String table, Iterable<Row> rows) {
        ArrayList<Row> rowArrayList = Lists.newArrayList(rows);
        StringBuilder stringBuilder = new StringBuilder("INSERT INTO ");
        stringBuilder.append(table).append(" (");
        for (int i = 0; i < rowArrayList.get(0).getSchema().getFields().size(); i++) {
            org.apache.beam.sdk.schemas.Schema.Field field = rowArrayList.get(0).getSchema().getFields().get(i);
            if (i == (rowArrayList.get(0).getSchema().getFields().size() - 1)) {
                stringBuilder.append(" ) VALUES \n\r");
            } else {
                stringBuilder.append(field.getName()).append(", ");
            }
        }
        for (int i = 0; i < rowArrayList.size(); i++) {
            StringBuilder values = new StringBuilder();
            values.append("(");
            Row row = rowArrayList.get(i);
            for (int j = 0; j < row.getFieldCount(); j++) {
                if (!(row.getValue(j) == null)) {
                    String stringValue = row.getValue(j).toString();
                    values.append(stringValue);
                }
                if (!(j == (row.getFieldCount() - 1))) {
                    values.append(", ");
                }
            }
            if (i == (rowArrayList.size() - 1)) {
                values.append(");");
            } else {
                values.append("), \n\r");
            }
            stringBuilder.append(values.toString());
        }
        return stringBuilder.toString();
    }
}
