import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.common.io.CharStreams;
import com.google.gson.*;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

//TODO - Should rewrite these as a coder extension for TableSchema - AVRO format??

/**
 * General utilities class
 */
public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static TableSchema convertFromString(String jsonSchema) {
        TableSchema tableSchema = new TableSchema();
        JsonElement jsonElement = JsonParser.parseString(jsonSchema);
        if (jsonElement.isJsonObject()) {
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            JsonArray elements = jsonObject.get("fields").getAsJsonArray();
            return tableSchema.setFields(getTableFieldsFromJsonArray(elements));
        } else {
            LOG.error("Json Schema must follow BigQuery format: { fields [...table rows] }");
            throw new JsonParseException("Json Schema must follow BigQuery format");
        }
    }

    private static List<TableFieldSchema> getTableFieldsFromJsonArray(JsonArray elements) {
        return StreamSupport.stream(elements.spliterator(), false)
                .map(JsonElement::getAsJsonObject)
                .map(element -> new TableFieldSchema().setName(element.get("name").getAsString())
                        .setType(element.get("type").getAsString())
                        .setMode(element.get("mode").getAsString()))
                .collect(Collectors.toList());
    }

    public static List<String> convertCSVLineToList(String record, String delimiter) {
        char charDelimiter = delimiter.charAt(0);
        List<String> toReturn = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean previous = false;
        for (char c : record.toCharArray()) {
            if (c != charDelimiter) {
                previous = false;
                current.append(c);
            } else {
                if (previous) {
                    toReturn.add("");
                } else {
                    toReturn.add(current.toString());
                    current = new StringBuilder();
                }
                previous = true;
            }
        }
        if (current.toString().length() != 0) {
            toReturn.add(current.toString());
        }
        return toReturn;
    }

    /**
     * Reads a file from a filesystem into a UTF-8 String. Backed by {@link FileSystems} such that it may read from S3, GCS and local
     *
     * @param pathToFile the path to the file
     * @return file contents as a String
     * @throws IOException if the file is cannot be found
     */
    public static String readFileFromFilesystem(String pathToFile) throws IOException {
        Reader reader = Channels.newReader(readFileFromFilesystemToByteChannel(pathToFile), StandardCharsets.UTF_8.name());
        String toReturn = CharStreams.toString(reader);
        reader.close();
        return toReturn;
    }

    /**
     * Reads a file from a filesystem. Backed by {@link FileSystems} such that it may read from S3, GCS and local
     *
     * @param pathToFile the path to the file
     * @return
     * @throws IOException if the file is cannot be found
     */
    public static ReadableByteChannel readFileFromFilesystemToByteChannel(String pathToFile) throws IOException {
        // Hacky solution to ensure that if we are reading a local rather than a GCS file we append the relative path
        LOG.info("Reading " + pathToFile);
        if (!(pathToFile.startsWith("gs:"))) {
            pathToFile = Paths.get(Paths.get("").toAbsolutePath().toString(), pathToFile).toString();
        }
        MatchResult matchResult = FileSystems.match(pathToFile);
        if (matchResult.status() == MatchResult.Status.NOT_FOUND) {
            LOG.error(pathToFile + " not found");
            throw new IOException(pathToFile + " not found");
        } else if (matchResult.status() == MatchResult.Status.ERROR) {
            LOG.error(pathToFile + " read encountered an I/O error");
            throw new IOException(pathToFile + " I/O error");
        }
        List<ResourceId> resourceIds = matchResult.metadata().stream().map(MatchResult.Metadata::resourceId).collect(Collectors.toList());
        return FileSystems.open(resourceIds.get(0));
    }

    /**
     * Parses a filename to find the dataset associated with the file
     *
     * @param fileName the filename to parse
     * @return the associated dataset
     * @throws IllegalArgumentException if no dataset is found
     */
    public static String getDatasetFromFilename(String fileName) throws IllegalArgumentException {
        String dataset = "";
        if (fileName.contains("DS_E1_HS_NT")) {
            dataset = "DS_E1_HS_NT";
        } else if (fileName.contains("CEL_DATA")) {
            dataset = "CEL_DATA";
        } else if (fileName.contains("CELONIS_SETUP")) {
            dataset = "CELONIS_SETUP";
        } else if (fileName.contains("E1_HS_NT")) {
            dataset = "E1_HS_NT";
        } else if (fileName.contains("S1_HS_NT")) {
            dataset = "S1_HS_NT";
        } else {
            throw new IllegalArgumentException("No known dataset in the provided file");
        }
        if (fileName.contains("_raw")) {
            dataset += "_raw";
        }
        return dataset;
    }


    /**
     * Checks the differenc between 2 lists
     *
     * @param list          list to look at
     * @param listToCompare list to compare to
     * @return a list of differences that is: the set difference list\listToCompare
     */
    public static List<String> checkFieldDifferences(List<String> list, List<String> listToCompare) {
        List<String> toReturn = new ArrayList<>(list);
        toReturn.removeAll(listToCompare);
        return toReturn;
    }

    /**
     * Converts a JsonArray of JsonObjects into a list using a specified key
     *
     * @param array the JsonArray of {@link JsonObject}s
     * @param key   the key of JsonObjects to pull into the returning list
     * @return a list of Strings
     */
    public static List<String> convertJsonObjectArrayToListOnKey(JsonArray array, String key) {
        List<String> toReturn = new ArrayList<>();
        array.forEach(jsonElement -> toReturn.add(jsonElement.getAsJsonObject().get(key).getAsString()));
        return toReturn;
    }

    /**
     * Decrypts the provided ciphertext with the specified crypto key.
     */
    public static byte[] decrypt(
            String projectId, String locationId, String keyRingId, String cryptoKeyId, byte[] ciphertext)
            throws IOException {
        try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
            String resourceName = CryptoKeyName.format(projectId, locationId, keyRingId, cryptoKeyId);
            DecryptResponse response = client.decrypt(resourceName, ByteString.copyFrom(ciphertext));
            return response.getPlaintext().toByteArray();
        }
    }
}
