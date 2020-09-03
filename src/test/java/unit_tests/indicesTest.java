package unit_tests;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.pilato.elasticsearch.tools.SettingsFinder;
import fr.pilato.elasticsearch.tools.SettingsReader;
import jdk.nashorn.internal.runtime.ECMAException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import static fr.pilato.elasticsearch.tools.AbstractBeyonderTest.restClient;
import static fr.pilato.elasticsearch.tools.index.IndexElasticsearchUpdater.isIndexExist;

public class indicesTest {
    private static RestClient client;
    private static final Logger logger = LoggerFactory.getLogger(SettingsReader.class);


    @BeforeClass
    public static void startElasticsearchRestClient() throws IOException {
        client = restClient();
    }

    @AfterClass
    public static void stopElasticsearchRestClient() throws IOException {
        if (client != null) {
            client.close();
        }
    }


    /**
     * Add set force as false to create a missing index
     */
    @Test
    public void testMissingIndex() throws Exception {
        String path = "src/test/external_directory/random_directory";
        String missingIndex = "missing_index";
        // check if missing_index already exists
        Assert.assertFalse("The index {" + missingIndex + "} exists already", isIndexExist(client, missingIndex));
        // create new index
        modifiedStart(client, path, missingIndex, false);
        // check if the index is now available
        Assert.assertTrue("The index {" + missingIndex + "} was not created", isIndexExist(client, missingIndex));
        // delete it
        removeIndexInElasticsearch(client, missingIndex);
        // check if the index was deleted
        Assert.assertFalse("The index {" + missingIndex + "} was not deleted", isIndexExist(client, missingIndex));

    }

    /**
     * For force_create, the force parameter need be true
     */
    @Test
    public void testForceCreate() throws Exception {
        String path = "src/test/external_directory/random_directory";
        String forceIndex = "force_index";
        Date firstCreationDate, secondCreationDate;


        //check if exists
        Assert.assertFalse("The index {" + forceIndex + "} exists already", isIndexExist(client, forceIndex));
        // create new index
        modifiedStart(client, path, forceIndex, false);
        // check (Assert) it's existence
        Assert.assertTrue("The index {" + forceIndex + "} was not created", isIndexExist(client, forceIndex));
        // get the metadata from the index (timestamp for example)
        firstCreationDate = getCreationDate(client, forceIndex);
        // (force-) create the same index and check compare the metadata
        modifiedStart(client, path, forceIndex, true);
        // get the creation date after forcing creation of an existing index
        secondCreationDate = getCreationDate(client, forceIndex);
        // check, if there was created a new index
        Assert.assertTrue(secondCreationDate.after(firstCreationDate));
        // delete it
        removeIndexInElasticsearch(client, forceIndex);
        // check if the index was deleted
        Assert.assertFalse("The index {" + forceIndex + "} was not deleted", isIndexExist(client, forceIndex));
    }


    private Date getCreationDate(RestClient client, String index) throws Exception {
        Response response = client.performRequest(new Request("GET", "/_cat/indices/" + index + "?h=creation.date"));
        String creationDate = EntityUtils.toString(response.getEntity()).trim();
        return new Date(Long.parseLong(creationDate));
    }


    @Test
    public void testReplaceIfChanged() throws Exception {
        String index = "index_replace_test";
        String path1 = "src/test/external_directory/index_1";
        String path2 = "src/test/external_directory/index_2";
        Date firstCreationDate, secondCreationDate;


        //check if exists
        Assert.assertFalse("The index {" + index + "} exists already", isIndexExist(client, index));
        // create index
        modifiedStart(client, path1, index, false);
        // check existence
        Assert.assertTrue("The index {" + index + "} was not created", isIndexExist(client, index));
        // get timestamp
        firstCreationDate = getCreationDate(client, index);
        // create new index with different mapping (same name)
        modifiedStart(client, path2, index, false);
        //get second timestamp
        secondCreationDate = getCreationDate(client, index);
        // check, if the mappings was replaced
        Assert.assertTrue(secondCreationDate.after(firstCreationDate));
        // delete it
        removeIndexInElasticsearch(client, index);
        // check if the index was deleted
        Assert.assertFalse("The index {" + index + "} was not deleted", isIndexExist(client, index));
    }


    /**
     * overwritten version of start(). Instead of getting the settings from the resource directory,
     * the user can choose any directory where _settings.json files are located.
     *
     * @param client elasticsearch client
     * @param path   directory of _settings.json files
     * @param index  arbitrary index name
     * @param force  Remove index if exists (Warning: remove all data)
     * @throws Exception when beyonder can not start
     */
    private void modifiedStart(RestClient client, String path, String index, boolean force) throws Exception {
        List<String> settingFiles = getSettingFiles(path);
        //String settingFile1 = settingFiles.get(0);
        for (String settingFile : settingFiles) {
            String settings = null;
            // Create index
            try (InputStream asStream = new FileInputStream(settingFile)) {
                if (asStream == null) {
                    logger.trace("Can not find [{}] in class loader.", settingFile);
                }
                settings = IOUtils.toString(asStream, "UTF-8");
            } catch (IOException e) {
                logger.warn("Can not read [{}].", settingFile);
            }

            Objects.requireNonNull(settings);
            settings = StringSubstitutor.replace(settings, System.getenv());
            createIndexWithSettingsModified(client, index, settings, force);
        }
    }

    /**
     * @return List of absolut paths of "_settings.json" files.
     * @throws NullPointerException if path is invalid
     */
    private List<String> getSettingFiles(String path) throws NullPointerException {
        String[] pathNames;
        String absolutePath;
        ArrayList<String> validPaths = new ArrayList<>();

        File f = new File(path);
        absolutePath = f.getAbsolutePath();
        pathNames = f.list();
        for (String pathname : Objects.requireNonNull(pathNames)) {
            //look if _settings.json file exists
            if (pathname.endsWith(SettingsFinder.Defaults.IndexSettingsFileName)) {
                validPaths.add(absolutePath + "/" + pathname);
            }
        }
        return validPaths;
    }

    /**
     * create index with few cases.
     * 1. Create if the index doesn't exists
     * 2. Force creation, if force set to true
     * 3. Replace the existing index, if the mappings are different
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no specific settings
     * @param force    Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    private void createIndexWithSettingsModified(RestClient client, String index, String settings, boolean force) throws Exception {
        if (force && isIndexExist(client, index)) {
            logger.debug("Index [{}] already exists but force set to true. Removing all data!", index);
            removeIndexInElasticsearch(client, index);
        }
        if (force || !isIndexExist(client, index)) {
            // exists and force_create
            logger.debug("Index [{}] doesn't exist. Creating it.", index);
            createIndexWithSettingsInElasticsearch(client, index, settings);
        } else {
            JsonNode newMappingsNode = new ObjectMapper().readTree(settings);
            JsonNode existingMappingNode = getMappingNodeFromExitingIndex(index);
            // get the existing mapping and compare with the new mapping -> the function return boolean
            // if exists and difference -> change
            if (!newMappingsNode.equals(existingMappingNode)) {
                logger.debug("Index [{}] exist, but the mapping is different. Changing it.", index);
                removeIndexInElasticsearch(client, index);
                createIndexWithSettingsInElasticsearch(client, index, settings);
            } else {
                logger.debug("Index [{}] already exists and no difference between the old and the new mappings.", index);
            }
        }
    }

    /**
     * @return mapping node from an existing index
     * @throws Exception if the index doesn't exists or it has an invalid valid format
     */
    private JsonNode getMappingNodeFromExitingIndex(String index) throws Exception {
        Response response = client.performRequest(new Request("GET", "/" + index + "/_mapping/"));
        String responseString = EntityUtils.toString(response.getEntity()).trim();
        return new ObjectMapper().readTree(responseString).get(index);
    }

    // Does not changed from the original function. Copied here for local use
    private void removeIndexInElasticsearch(RestClient client, String index) throws Exception {
        logger.trace("removeIndex([{}])", index);

        assert client != null;
        assert index != null;

        Response response = client.performRequest(new Request("DELETE", "/" + index));
        if (response.getStatusLine().getStatusCode() != 200) {
            logger.warn("Could not delete index [{}]", index);
            throw new Exception("Could not delete index [" + index + "].");
        }

        logger.trace("/removeIndex([{}])", index);
    }

    // Does not changed from the original function. Copied here for local use
    private void createIndexWithSettingsInElasticsearch(RestClient client, String index, String settings) throws Exception {
        logger.trace("createIndex([{}])", index);

        assert client != null;
        assert index != null;

        Request request = new Request("PUT", "/" + index);

        // If there are settings for this index, we use it. If not, using Elasticsearch defaults.
        if (settings != null) {
            logger.trace("Found settings for index [{}]: [{}]", index, settings);
            request.setJsonEntity(settings);
        }

        Response response = client.performRequest(request);
        if (response.getStatusLine().getStatusCode() != 200) {
            logger.warn("Could not create index [{}]", index);
            throw new Exception("Could not create index [" + index + "].");
        }

        logger.trace("/createIndex([{}])", index);
    }


}
