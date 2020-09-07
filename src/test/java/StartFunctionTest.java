import fr.pilato.elasticsearch.tools.AbstractBeyonderTest;
import fr.pilato.elasticsearch.tools.ElasticsearchBeyonder;
import fr.pilato.elasticsearch.tools.SettingsFinder;
import fr.pilato.elasticsearch.tools.SettingsReader;
import fr.pilato.elasticsearch.tools.index.IndexElasticsearchUpdater;
import fr.pilato.elasticsearch.tools.index.IndexFinder;
import fr.pilato.elasticsearch.tools.index.IndexSettingsReader;
import fr.pilato.elasticsearch.tools.template.TemplateFinder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
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
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static fr.pilato.elasticsearch.tools.AbstractBeyonderTest.restClient;
import static fr.pilato.elasticsearch.tools.ElasticsearchBeyonder.modifiedStart;
import static fr.pilato.elasticsearch.tools.index.IndexElasticsearchUpdater.*;
import static fr.pilato.elasticsearch.tools.template.TemplateElasticsearchUpdater.createTemplate;
import static org.junit.Assert.*;

public class StartFunctionTest {
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


    @Test
    public void testStartFunction() throws Exception {
        String path = "src/test/external_directory/dir_test_index/";
        String index = "start_function_test_index";
        // check if index exists
        Assert.assertFalse("The index {" + index + "} exists already", isIndexExist(client, index));
        // create new index
        modifiedStart(client, path, index);
        // check if index exists
        Assert.assertTrue("The index {" + index + "} was not created", isIndexExist(client, index));
        // delete index
        removeIndexInElasticsearch(client, index);
        // check if index exists
        Assert.assertFalse("The index {" + index + "} was not deleted", isIndexExist(client, index));

    }

    // copy from class indexElasticsearchUpdater
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