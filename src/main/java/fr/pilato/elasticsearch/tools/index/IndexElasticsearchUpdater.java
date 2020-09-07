/*
 * Licensed to David Pilato (the "Author") under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Author licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package fr.pilato.elasticsearch.tools.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Manage elasticsearch index settings
 *
 * @author David Pilato
 */
public class IndexElasticsearchUpdater {

    private static final Logger logger = LoggerFactory.getLogger(IndexElasticsearchUpdater.class);

    /**
     * Create a new index in Elasticsearch. Read also _settings.json if exists.
     *
     * @param client Elasticsearch client
     * @param root   dir within the classpath
     * @param index  Index name
     * @param force  Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    public static void createIndex(Client client, String root, String index, boolean force) throws Exception {
        String settings = IndexSettingsReader.readSettings(root, index);
        createIndexWithSettings(client, index, settings, force);
    }

    /**
     * Create a new index in Elasticsearch. Read also _settings.json if exists in default classpath dir.
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @param force  Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    public static void createIndex(Client client, String index, boolean force) throws Exception {
        String settings = IndexSettingsReader.readSettings(index);
        createIndexWithSettings(client, index, settings, force);
    }

    /**
     * Create a new index in Elasticsearch
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no specific settings
     * @param force    Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    public static void createIndexWithSettings(Client client, String index, String settings, boolean force) throws Exception {
        if (force && isIndexExist(client, index)) {
            logger.debug("Index [{}] already exists but force set to true. Removing all data!", index);
            removeIndexInElasticsearch(client, index);
        }
        if (force || !isIndexExist(client, index)) {
            logger.debug("Index [{}] doesn't exist. Creating it.", index);
            createIndexWithSettingsInElasticsearch(client, index, settings);
        } else {
            logger.debug("Index [{}] already exists.", index);
        }
    }

    /**
     * Remove a new index in Elasticsearch
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    private static void removeIndexInElasticsearch(Client client, String index) throws Exception {
        logger.trace("removeIndex([{}])", index);

        assert client != null;
        assert index != null;

        AcknowledgedResponse response = client.admin().indices().prepareDelete(index).get();
        if (!response.isAcknowledged()) {
            logger.warn("Could not delete index [{}]", index);
            throw new Exception("Could not delete index [" + index + "].");
        }

        logger.trace("/removeIndex([{}])", index);
    }

    /**
     * Create a new index in Elasticsearch
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no specific settings
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    private static void createIndexWithSettingsInElasticsearch(Client client, String index, String settings) throws Exception {
        logger.trace("createIndex([{}])", index);

        assert client != null;
        assert index != null;

        CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(index);

        // If there are settings for this index, we use it. If not, using Elasticsearch defaults.
        if (settings != null) {
            logger.trace("Found settings for index [{}]: [{}]", index, settings);
            cirb.setSource(settings, XContentType.JSON);
        }

        CreateIndexResponse createIndexResponse = cirb.execute().actionGet();
        if (!createIndexResponse.isAcknowledged()) {
            logger.warn("Could not create index [{}]", index);
            throw new Exception("Could not create index [" + index + "].");
        }

        logger.trace("/createIndex([{}])", index);
    }

    /**
     * Update settings in Elasticsearch
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no update settings
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    private static void updateIndexWithSettingsInElasticsearch(Client client, String index, String settings) throws Exception {
        logger.trace("updateIndex([{}])", index);

        assert client != null;
        assert index != null;

        if (settings != null) {
            logger.trace("Found update settings for index [{}]: [{}]", index, settings);
            logger.debug("updating settings for index [{}]", index);
            client.admin().indices().prepareUpdateSettings(index).setSettings(settings, XContentType.JSON).get();
        }

        logger.trace("/updateIndex([{}])", index);
    }

    /**
     * Check if an index already exists
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @return true if index already exists
     */
    @Deprecated
    public static boolean isIndexExist(Client client, String index) {
        return client.admin().indices().prepareExists(index).get().isExists();
    }

    /**
     * Update index settings in Elasticsearch. Read also _update_settings.json if exists.
     *
     * @param client Elasticsearch client
     * @param root   dir within the classpath
     * @param index  Index name
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    public static void updateSettings(Client client, String root, String index) throws Exception {
        String settings = IndexSettingsReader.readUpdateSettings(root, index);
        updateIndexWithSettingsInElasticsearch(client, index, settings);
    }

    /**
     * Update index settings in Elasticsearch. Read also _update_settings.json if exists in default classpath dir.
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @throws Exception if the elasticsearch API call is failing
     */
    @Deprecated
    public static void updateSettings(Client client, String index) throws Exception {
        String settings = IndexSettingsReader.readUpdateSettings(index);
        updateIndexWithSettingsInElasticsearch(client, index, settings);
    }

    /**
     * Create a new index in Elasticsearch. Read also _settings.json if exists.
     *
     * @param client Elasticsearch client
     * @param root   dir within the classpath
     * @param index  Index name
     * @param force  Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    public static void createIndex(RestClient client, String root, String index, boolean force) throws Exception {
        String settings = IndexSettingsReader.readSettings(root, index);
        createIndexWithSettings(client, index, settings, force);
    }

    /**
     * Create a new index in Elasticsearch. Read also _settings.json if exists in default classpath dir.
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @param force  Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    public static void createIndex(RestClient client, String index, boolean force) throws Exception {
        String settings = IndexSettingsReader.readSettings(index);
        createIndexWithSettings(client, index, settings, force);
    }

    /**
     * Create a new index in Elasticsearch
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no specific settings
     * @param force    Remove index if exists (Warning: remove all data)
     * @throws Exception if the elasticsearch API call is failing
     */
    public static void createIndexWithSettings(RestClient client, String index, String settings, boolean force) throws Exception {
        if (force && isIndexExist(client, index)) {
            logger.debug("Index [{}] already exists but force set to true. Removing all data!", index);
            removeIndexInElasticsearch(client, index);
        }
        if (force || !isIndexExist(client, index)) {
            logger.debug("Index [{}] doesn't exist. Creating it.", index);
            createIndexWithSettingsInElasticsearch(client, index, settings);
        } else {
            logger.debug("Index [{}] already exists.", index);
        }
    }

    /**
     * Remove a new index in Elasticsearch
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @throws Exception if the elasticsearch API call is failing
     */
    private static void removeIndexInElasticsearch(RestClient client, String index) throws Exception {
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

    /**
     * Create a new index in Elasticsearch
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no specific settings
     * @throws Exception if the elasticsearch API call is failing
     */
    private static void createIndexWithSettingsInElasticsearch(RestClient client, String index, String settings) throws Exception {
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

    /**
     * Update settings in Elasticsearch
     *
     * @param client   Elasticsearch client
     * @param index    Index name
     * @param settings Settings if any, null if no update settings
     * @throws Exception if the elasticsearch API call is failing
     */
    private static void updateIndexWithSettingsInElasticsearch(RestClient client, String index, String settings) throws Exception {
        logger.trace("updateIndex([{}])", index);

        assert client != null;
        assert index != null;


        if (settings != null) {
            logger.trace("Found update settings for index [{}]: [{}]", index, settings);
            logger.debug("updating settings for index [{}]", index);
            Request request = new Request("PUT", "/" + index + "/_settings");
            request.setJsonEntity(settings);
            client.performRequest(request);
        }

        logger.trace("/updateIndex([{}])", index);
    }

    /**
     * Check if an index already exists
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @return true if index already exists
     * @throws Exception if the elasticsearch API call is failing
     */
    public static boolean isIndexExist(RestClient client, String index) throws Exception {
        Response response = client.performRequest(new Request("HEAD", "/" + index));
        return response.getStatusLine().getStatusCode() == 200;
    }

    /**
     * Update index settings in Elasticsearch. Read also _update_settings.json if exists.
     *
     * @param client Elasticsearch client
     * @param root   dir within the classpath
     * @param index  Index name
     * @throws Exception if the elasticsearch API call is failing
     */
    public static void updateSettings(RestClient client, String root, String index) throws Exception {
        String settings = IndexSettingsReader.readUpdateSettings(root, index);
        updateIndexWithSettingsInElasticsearch(client, index, settings);
    }

    /**
     * Update index settings in Elasticsearch. Read also _update_settings.json if exists in default classpath dir.
     *
     * @param client Elasticsearch client
     * @param index  Index name
     * @throws Exception if the elasticsearch API call is failing
     */
    public static void updateSettings(RestClient client, String index) throws Exception {
        String settings = IndexSettingsReader.readUpdateSettings(index);
        updateIndexWithSettingsInElasticsearch(client, index, settings);
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
    public static void createIndexWithSettingsModified(RestClient client,
                                                       String index,
                                                       String settings,
                                                       boolean force,
                                                       List<String> relevantSettings) throws Exception {
        if (force && isIndexExist(client, index)) {
            logger.debug("Index [{}] already exists but force set to true. Removing all data!", index);
            removeIndexInElasticsearch(client, index);
        }
        if (force || !isIndexExist(client, index)) {
            // exists and force_create
            logger.debug("Index [{}] doesn't exist. Creating it.", index);
            createIndexWithSettingsInElasticsearch(client, index, settings);
        } else {
            JsonNode nodeFromFile = new ObjectMapper().readTree(settings);
            JsonNode modifiedMappingFormFile = normalizeMappingNode(nodeFromFile.get("mappings"));

            JsonNode settingsFromES = getSettingsNodeFromExistingIndex(client, index);
            JsonNode mappingsFromES = getMappingNodeFromExistingIndex(client, index);
            // get the existing mapping and compare with the new mapping -> the function return boolean
            // if exists and difference -> change
            if (!nodeFromFile.path("mappings").equals(mappingsFromES) || !compareSettings(relevantSettings, nodeFromFile.path("settings"), settingsFromES)) {
                logger.debug("Index [{}] exist, but the mapping is different. Changing it.", index);
                removeIndexInElasticsearch(client, index);
                createIndexWithSettingsInElasticsearch(client, index, settings);
            } else {
                logger.debug("Index [{}] already exists and no difference between the old and the new mappings.", index);
            }
        }
    }

    /**
     * Because the mappings are modified by ES, the original need to be modified too just for comparison.
     *
     * @return JsonNode without the fields like "index:true"
     */
    private static JsonNode normalizeMappingNode(JsonNode mappingsNode) {
        for (JsonNode o : mappingsNode.get("properties")) {
            JsonNode indexNode = o.path("index");

            if (indexNode.isBoolean() && indexNode.asBoolean()) {
                ((ObjectNode) o).remove("index");
            }
        }
        return mappingsNode;
    }

    /**
     * @return mapping node from an existing index
     * @throws Exception if the index doesn't exists or it has an invalid format
     */
    private static JsonNode getMappingNodeFromExistingIndex(RestClient client, String index) throws Exception {
        Response response = client.performRequest(new Request("GET", "/" + index + "/_mapping/"));
        String responseString = EntityUtils.toString(response.getEntity()).trim();
        return new ObjectMapper().readTree(responseString).path(index).path("mappings");
    }

    /**
     * @return settings node from an existing index
     * @throws Exception if the index doesn't exists or it has an invalid format
     */
    private static JsonNode getSettingsNodeFromExistingIndex(RestClient client, String index) throws Exception {
        Response response = client.performRequest(new Request("GET", "/" + index + "/_settings/index.analysis*"));
        String responseString = EntityUtils.toString(response.getEntity()).trim();
        return new ObjectMapper().readTree(responseString).path(index).path("settings").path("index");
    }

    public static boolean compareSettings(List<String> relevantSettings, JsonNode settingsFile, JsonNode settingsES) {

        for (String setting : relevantSettings) {
            if (!settingsFile.path(setting).equals(settingsES.path(setting))) {
                logger.warn("The setting [{}] is different. Settings are not equal.", setting);
                return false;
            }
        }
        logger.trace("The settings are equal.");
        return true;
    }


}
