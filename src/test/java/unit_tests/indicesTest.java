package unit_tests;

import fr.pilato.elasticsearch.tools.SettingsReader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;

import static fr.pilato.elasticsearch.tools.AbstractBeyonderTest.restClient;
import static fr.pilato.elasticsearch.tools.ElasticsearchBeyonder.modifiedStart;
import static fr.pilato.elasticsearch.tools.index.IndexElasticsearchUpdater.isIndexExist;

public class indicesTest {
    private static RestClient client;
    private static final Logger logger = LoggerFactory.getLogger(SettingsReader.class);
    /**
     * Includes the settings that must be validated
     */
    private static final ArrayList<String> RELEVANT_SETTINGS = new ArrayList<>(Collections.singletonList("analysis"));

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
        modifiedStart(client, path, missingIndex, false, RELEVANT_SETTINGS);
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
        modifiedStart(client, path, forceIndex, false, RELEVANT_SETTINGS);
        // check (Assert) it's existence
        Assert.assertTrue("The index {" + forceIndex + "} was not created", isIndexExist(client, forceIndex));
        // get the metadata from the index (timestamp for example)
        firstCreationDate = getCreationDate(client, forceIndex);
        // (force-) create the same index and check compare the metadata
        modifiedStart(client, path, forceIndex, true, RELEVANT_SETTINGS);
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
        Date firstCreationDate, secondCreationDate, thirdCreationDate;


        //check if exists
        Assert.assertFalse("The index {" + index + "} exists already", isIndexExist(client, index));
        // create index
        modifiedStart(client, path1, index, false, RELEVANT_SETTINGS);
        // check existence
        Assert.assertTrue("The index {" + index + "} was not created", isIndexExist(client, index));
        // get timestamp
        firstCreationDate = getCreationDate(client, index);
        // create new index with different mapping (same name)
        modifiedStart(client, path2, index, false, RELEVANT_SETTINGS);
        //get second timestamp
        secondCreationDate = getCreationDate(client, index);
        // check, if the mappings was replaced
        Assert.assertTrue(secondCreationDate.after(firstCreationDate));
        // create the same index with same mapping
        modifiedStart(client, path2, index, false, RELEVANT_SETTINGS);
        //get third timestamp
        thirdCreationDate = getCreationDate(client, index);
        // check if the timestamps are equal
        Assert.assertEquals(secondCreationDate, thirdCreationDate);
        // delete it
        removeIndexInElasticsearch(client, index);
        // check if the index was deleted
        Assert.assertFalse("The index {" + index + "} was not deleted", isIndexExist(client, index));


    }


    @Test
    public void testUpdateIndexWithRelevantIndex() throws Exception {
        String index = "relevant_index";
        String path1 = "/home/micahel/Work/elasticsearch-beyonder/src/test/external_directory/relevant_index/index";
        String path2 = "/home/micahel/Work/elasticsearch-beyonder/src/test/external_directory/relevant_index/modified_index";
        Date firstCreationDate, secondCreationDate, thirdCreationDate;

        //check if index exists
        Assert.assertFalse("The index {" + index + "} exists already", isIndexExist(client, index));
        // create index
        modifiedStart(client, path1, index, false, RELEVANT_SETTINGS);
        // check existence
        Assert.assertTrue("The index {" + index + "} was not created", isIndexExist(client, index));
        // get timestamp of the first creation
        firstCreationDate = getCreationDate(client, index);
        // create new index with same mapping (same name)
        modifiedStart(client, path1, index, false, RELEVANT_SETTINGS);
        //get second timestamp
        secondCreationDate = getCreationDate(client, index);
        // check, if the mappings are equal
        Assert.assertEquals(firstCreationDate, secondCreationDate);
        // update the index with different settings
        modifiedStart(client, path2, index, false, RELEVANT_SETTINGS);
        //get second timestamp
        thirdCreationDate = getCreationDate(client, index);
        // check, if the mappings was replaced
        Assert.assertTrue(thirdCreationDate.after(firstCreationDate));
        // delete it
        removeIndexInElasticsearch(client, index);
        // check if the index was deleted
        Assert.assertFalse("The index {" + index + "} was not deleted", isIndexExist(client, index));

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


}
