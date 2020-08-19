import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.*;

public class BQtoSQLPipeline {


    public interface BQtOSQLOptions extends PipelineOptions, GcpOptions {

        @Description("Config mapping file location")
        @Validation.Required
        ValueProvider<String> getConfigMappingLocation();

        void setConfigMappingLocation(ValueProvider<String> configMappingLocation);

        @Description("Name of the Database")
        @Validation.Required
        ValueProvider<String> getDB_NAME();

        void setDB_NAME(ValueProvider<String> DB_NAME);

        @Description("CLOUDSQL Connection Name")
        @Default.String("null")
        ValueProvider<String> getCLOUD_SQL_CONNECTION_NAME();

        void setCLOUD_SQL_CONNECTION_NAME(ValueProvider<String> CLOUD_SQL_CONNECTION_NAME);

        @Description("Config file location with database credentials")
        @Validation.Required
        ValueProvider<String> getCredentialFileLocation();

        void setCredentialFileLocation(ValueProvider<String> CredentialFileLocation);

        @Description("Location of KMS key")
        @Validation.Required
        ValueProvider<String> getKMSKeyLocation();

        void setKMSKeyLocation(ValueProvider<String> KMSKeyLocation);

        @Description("Keyring of KMS key")
        @Validation.Required
        ValueProvider<String> getKMSKeyring();

        void setKMSKeyring(ValueProvider<String> KMSKeyring);

        @Description("Name of KMS key")
        @Validation.Required
        ValueProvider<String> getKMSKeyName();

        void setKMSKeyName(ValueProvider<String> KMSKey);
    }

    public static void main(String[] args) {
        PipelineOptionsFactory.register(BQtOSQLOptions.class);
        BQtOSQLOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(BQtOSQLOptions.class);
        Pipeline p = Pipeline.create(options);
        p.apply("Read into BQ from SQL", new InsertAllIntoBQ("Ingest tables to CloudSQL"));
    }
}