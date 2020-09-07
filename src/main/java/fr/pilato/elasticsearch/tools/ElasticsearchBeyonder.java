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

package fr.pilato.elasticsearch.tools;

import fr.pilato.elasticsearch.tools.SettingsFinder.Defaults;
import fr.pilato.elasticsearch.tools.index.IndexFinder;
import fr.pilato.elasticsearch.tools.template.TemplateFinder;
import org.apache.commons.io.IOUtils;
import org.apache.commons.text.StringSubstitutor;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static fr.pilato.elasticsearch.tools.index.IndexElasticsearchUpdater.*;
import static fr.pilato.elasticsearch.tools.template.TemplateElasticsearchUpdater.createTemplate;

/**
 * By default, indexes are created with their default Elasticsearch settings. You can specify
 * your own settings for your index by putting a /es/indexname/_settings.json in your classpath.
 * <br>
 * So if you create a file named /es/twitter/_settings.json in your src/main/resources folder (for maven lovers),
 * it will be used by the factory to create the twitter index.
 * <pre>
 * {
 *   "index" : {
 *     "number_of_shards" : 3,
 *     "number_of_replicas" : 2
 *   }
 * }
 * </pre>
 * By default, types are not created and wait for the first document you send to Elasticsearch (auto mapping).
 * But, if you define a file named /es/indexname/type.json in your classpath, the type will be created at startup using
 * the type definition you give.
 * <br>
 * So if you create a file named /es/twitter/tweet.json in your src/main/resources folder (for maven lovers),
 * it will be used by the factory to create the tweet type in twitter index.
 * <pre>
 * {
 *   "tweet" : {
 *     "properties" : {
 *       "message" : {"type" : "string", "store" : "yes"}
 *     }
 *   }
 * }
 * </pre>
 * <p>
 * By convention, the factory will create all settings and mappings found under the /es classpath.<br>
 * You can disable convention and use configuration by setting autoscan to false.
 *
 * @author David Pilato
 */
public class ElasticsearchBeyonder {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchBeyonder.class);

    /**
     * Automatically scan classpath and creates indices, types, templates... in default dir.
     *
     * @param client elasticsearch client
     * @throws Exception when beyonder can not start
     */
    public static void start(RestClient client) throws Exception {
        start(client, Defaults.ConfigDir);
    }

    /**
     * Automatically scan classpath and creates indices, types, templates...
     *
     * @param client elasticsearch client
     * @param root   dir within the classpath
     * @throws Exception when beyonder can not start
     */
    public static void start(RestClient client, String root) throws Exception {
        start(client, root, Defaults.MergeMappings, Defaults.ForceCreation);
    }

    /**
     * Automatically scan classpath and create indices, mappings, templates, and other settings.
     *
     * @param client elasticsearch client
     * @param root   dir within the classpath
     * @param merge  whether or not to merge mappings
     * @param force  whether or not to force creation of indices and templates
     * @throws Exception when beyonder can not start
     * @since 6.1
     */
    public static void start(RestClient client, String root, boolean merge, boolean force) throws Exception {
        logger.info("starting automatic settings/mappings discovery");

        // create templates
        List<String> templateNames = TemplateFinder.findTemplates(root);
        for (String templateName : templateNames) {
            createTemplate(client, root, templateName, force);
        }

        // create indices
        Collection<String> indexNames = IndexFinder.findIndexNames(root);
        for (String indexName : indexNames) {
            createIndex(client, root, indexName, force);
            updateSettings(client, root, indexName);
        }
        logger.info("start done. Rock & roll!");
    }

    /**
     * Automatically scan classpath and creates indices, types, templates... in default dir.
     *
     * @param client elasticsearch client
     * @throws Exception when beyonder can not start
     * @see #start(RestClient) for the RestClient implementation
     * @deprecated You should use now the RestClient implementation
     */
    @Deprecated
    public static void start(Client client) throws Exception {
        start(client, Defaults.ConfigDir);
    }

    /**
     * Automatically scan classpath and creates indices, types, templates...
     *
     * @param client elasticsearch client
     * @param root   dir within the classpath
     * @throws Exception when beyonder can not start
     * @see #start(RestClient, String) for the RestClient implementation
     * @deprecated You should use now the RestClient implementation
     */
    @Deprecated
    public static void start(Client client, String root) throws Exception {
        logger.info("starting automatic settings/mappings discovery");

        // TODO make it a parameter
        boolean merge = Defaults.MergeMappings;
        boolean force = Defaults.ForceCreation;

        // create templates
        List<String> templateNames = TemplateFinder.findTemplates(root);
        for (String templateName : templateNames) {
            createTemplate(client, root, templateName, force);
        }

        // create indices
        Collection<String> indexNames = IndexFinder.findIndexNames(root);
        for (String indexName : indexNames) {
            createIndex(client, root, indexName, force);
            updateSettings(client, root, indexName);
        }
        logger.info("start done. Rock & roll!");
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
    public static void modifiedStart(RestClient client, String path, String index, boolean force, List<String> relevantSettings) throws Exception {
        List<String> settingFiles = getSettingFiles(path);
        //String settingFile1 = settingFiles.get(0);
        for (String settingFile : settingFiles) {
            String settings = null;
            // Create index
            try (InputStream asStream = new FileInputStream(settingFile)) {
                settings = IOUtils.toString(asStream, StandardCharsets.UTF_8);
            } catch (IOException e) {
                logger.warn("Can not read [{}].", settingFile);
            }

            Objects.requireNonNull(settings);
            settings = StringSubstitutor.replace(settings, System.getenv());
            createIndexWithSettingsModified(client, index, settings, force, relevantSettings);
        }
    }

    /**
     * @return List of absolut paths of "_settings.json" files.
     * @throws NullPointerException if path is invalid
     */
    private static List<String> getSettingFiles(String path) throws NullPointerException {
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


}
