/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Lists;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.helix.ControllerTest;
import org.apache.pinot.plugin.ingestion.batch.common.BaseSegmentPushJobRunner;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.ingestion.BatchIngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PushJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.TableSpec;
import org.apache.pinot.spi.utils.builder.ControllerRequestURLBuilder;
import org.testng.Assert;


public abstract class BaseSegmentUploadConsistentPushIntegrationTest extends SegmentUploadIntegrationTest {

    public abstract BaseSegmentPushJobRunner getMetadataPushJobRunner();

    public void testUploadAndQuery()
        throws Exception {
        // Create and upload the schema and table config
        Schema schema = createSchema();
        addSchema(schema);
        TableConfig offlineTableConfig = createOfflineTableConfig();
        offlineTableConfig.setIngestionConfig(offlineTableConfig.getIngestionConfig());
        addTableConfig(offlineTableConfig);
        File tableConfigFile = new File(_tempDir, "tableConfig");
        FileUtils.write(tableConfigFile, offlineTableConfig.toJsonString(), StandardCharsets.UTF_8);

        List<File> avroFiles = getAllAvroFiles();

        // Create 1 segment, for METADATA push WITH move to final location
        ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(0), offlineTableConfig, schema, "_with_move",
            _segmentDir, _tarDir);

        BaseSegmentPushJobRunner runner = getMetadataPushJobRunner();
        SegmentGenerationJobSpec jobSpec = new SegmentGenerationJobSpec();
        PushJobSpec pushJobSpec = new PushJobSpec();
        // set moveToDeepStoreForMetadataPush to true
        pushJobSpec.setCopyToDeepStoreForMetadataPush(true);
        jobSpec.setPushJobSpec(pushJobSpec);
        PinotFSSpec fsSpec = new PinotFSSpec();
        fsSpec.setScheme("file");
        fsSpec.setClassName("org.apache.pinot.spi.filesystem.LocalPinotFS");
        jobSpec.setPinotFSSpecs(Lists.newArrayList(fsSpec));
        jobSpec.setOutputDirURI(_tarDir.getAbsolutePath());
        TableSpec tableSpec = new TableSpec();
        tableSpec.setTableName(DEFAULT_TABLE_NAME);
        tableSpec.setTableConfigURI(tableConfigFile.toURI().toString());
        jobSpec.setTableSpec(tableSpec);
        PinotClusterSpec clusterSpec = new PinotClusterSpec();
        clusterSpec.setControllerURI(_controllerBaseApiUrl);
        jobSpec.setPinotClusterSpecs(new PinotClusterSpec[]{clusterSpec});

        File dataDir = new File(_controllerConfig.getDataDir());
        File dataDirSegments = new File(dataDir, DEFAULT_TABLE_NAME);

        // Not present in dataDir, only present in sourceDir
        Assert.assertFalse(dataDirSegments.exists());
        Assert.assertEquals(_tarDir.listFiles().length, 1);

        runner.init(jobSpec);
        runner.run();

        // Segment should be seen in dataDir
        Assert.assertTrue(dataDirSegments.exists());
        Assert.assertEquals(dataDirSegments.listFiles().length, 1);
        Assert.assertEquals(_tarDir.listFiles().length, 1);

        // test segment loaded
        JsonNode segmentsList = getSegmentsList();
        Assert.assertEquals(segmentsList.size(), 1);
        String segmentNameWithMove = segmentsList.get(0).asText();
        Assert.assertTrue(segmentNameWithMove.endsWith("_with_move"));
        long numDocs = getNumDocs(segmentNameWithMove);
        testCountStar(numDocs);

        // Fetch segment lineage entry after running segment metadata push with consistent push enabled.
        String segmentLineageResponse = ControllerTest.sendGetRequest(
            ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl)
                .forListAllSegmentLineages(DEFAULT_TABLE_NAME, TableType.OFFLINE.toString()));
        // Segment lineage should be in completed state.
        Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
        // SegmentsFrom should be empty as we started with a blank table.
        Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[]"));
        // SegmentsTo should contain uploaded segment.
        Assert.assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"" + segmentNameWithMove + "\"]"));

        // Clear segment and tar dir
        for (File segment : _segmentDir.listFiles()) {
            FileUtils.deleteQuietly(segment);
        }
        for (File tar : _tarDir.listFiles()) {
            FileUtils.deleteQuietly(tar);
        }

        // Create 1 segment, for METADATA push WITHOUT move to final location
        ClusterIntegrationTestUtils.buildSegmentFromAvro(avroFiles.get(1), offlineTableConfig, schema, "_without_move",
            _segmentDir, _tarDir);
        jobSpec.setPushJobSpec(new PushJobSpec());
        runner = getMetadataPushJobRunner();

        Assert.assertEquals(dataDirSegments.listFiles().length, 1);
        Assert.assertEquals(_tarDir.listFiles().length, 1);

        runner.init(jobSpec);
        runner.run();

        // should not see new segments in dataDir
        Assert.assertEquals(dataDirSegments.listFiles().length, 1);
        Assert.assertEquals(_tarDir.listFiles().length, 1);

        // test segment loaded
        segmentsList = getSegmentsList();
        Assert.assertEquals(segmentsList.size(), 2);
        String segmentNameWithoutMove = null;
        for (JsonNode segment : segmentsList) {
            if (segment.asText().endsWith("_without_move")) {
                segmentNameWithoutMove = segment.asText();
            }
        }
        Assert.assertNotNull(segmentNameWithoutMove);
        numDocs = getNumDocs(segmentNameWithoutMove);
        testCountStar(numDocs);

        // Fetch segment lineage entry after running segment metadata push with consistent push enabled.
        segmentLineageResponse = ControllerTest.sendGetRequest(
            ControllerRequestURLBuilder.baseUrl(_controllerBaseApiUrl)
                .forListAllSegmentLineages(DEFAULT_TABLE_NAME, TableType.OFFLINE.toString()));
        // Segment lineage should be in completed state.
        Assert.assertTrue(segmentLineageResponse.contains("\"state\":\"COMPLETED\""));
        // SegmentsFrom should contain the previous segment
        Assert.assertTrue(segmentLineageResponse.contains("\"segmentsFrom\":[\"" + segmentNameWithMove + "\"]"));
        // SegmentsTo should contain uploaded segment.
        Assert.assertTrue(segmentLineageResponse.contains("\"segmentsTo\":[\"" + segmentNameWithoutMove + "\"]"));
    }

    @Override
    protected IngestionConfig getIngestionConfig() {
        IngestionConfig ingestionConfig = new IngestionConfig();
        ingestionConfig.setBatchIngestionConfig(new BatchIngestionConfig(null, "REFRESH", "DAILY", true));
        return ingestionConfig;
    }
}
