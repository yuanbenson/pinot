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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsistentDataPushUtils {
  private ConsistentDataPushUtils() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentPushUtils.class);
  private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();
  public static final String SEGMENT_NAME_POSTFIX = "segment.name.postfix";

  public static void pushSegmentsTar(SegmentGenerationJobSpec spec, PinotFS outputDirFS, List<String> segmentsToPush) {
    Map<URI, String> uriToLineageEntryIdMap = new HashMap<>();
    try {
      uriToLineageEntryIdMap =
          ConsistentDataPushUtils.preUpload(spec, ConsistentDataPushUtils.getTarSegmentsTo(segmentsToPush));
      SegmentPushUtils.pushSegments(spec, outputDirFS, segmentsToPush);
      ConsistentDataPushUtils.postUpload(spec, uriToLineageEntryIdMap);
    } catch (Exception e) {
      ConsistentDataPushUtils.handleUploadException(spec, uriToLineageEntryIdMap, e);
      throw new RuntimeException(e);
    }
  }

  public static void pushSegmentsUris(SegmentGenerationJobSpec spec, List<String> segmentUris) {
    Map<URI, String> uriToLineageEntryIdMap = new HashMap<>();
    try {
      uriToLineageEntryIdMap =
          ConsistentDataPushUtils.preUpload(spec, ConsistentDataPushUtils.getTarSegmentsTo(segmentUris));
      SegmentPushUtils.sendSegmentUris(spec, segmentUris);
      ConsistentDataPushUtils.postUpload(spec, uriToLineageEntryIdMap);
    } catch (Exception e) {
      ConsistentDataPushUtils.handleUploadException(spec, uriToLineageEntryIdMap, e);
      throw new RuntimeException(e);
    }
  }

  public static void pushSegmentsMetadata(SegmentGenerationJobSpec spec, PinotFS outputDirFS,
      Map<String, String> segmentUriToTarPathMap) {
    Map<URI, String> uriToLineageEntryIdMap = new HashMap<>();
    try {
      uriToLineageEntryIdMap = ConsistentDataPushUtils.preUpload(spec,
          ConsistentDataPushUtils.getMetadataSegmentsTo(segmentUriToTarPathMap));
      SegmentPushUtils.sendSegmentUriAndMetadata(spec, outputDirFS, segmentUriToTarPathMap);
      ConsistentDataPushUtils.postUpload(spec, uriToLineageEntryIdMap);
    } catch (Exception e) {
      ConsistentDataPushUtils.handleUploadException(spec, uriToLineageEntryIdMap, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks for enablement of consistent data push. If enabled, start consistent data push protocol and
   * returns a map of controller URI to lineage entry IDs.
   * If not, returns an empty hashmap.
   */
  public static Map<URI, String> preUpload(SegmentGenerationJobSpec spec, List<String> segmentsTo)
      throws Exception {
    String rawTableName = spec.getTableSpec().getTableName();
    boolean consistentDataPushEnabled = consistentDataPushEnabled(spec);
    LOGGER.info("{} consistent push", consistentDataPushEnabled ? "Enabled" : "Disabled");
    Map<URI, String> uriToLineageEntryIdMap = new HashMap<>();
    if (consistentDataPushEnabled) {
      // Check whether unique segment name should be disabled only when consistent push is enabled.
      boolean disableUniqueSegmentName = uniqueSegmentNameDisabled(spec);
      LOGGER.info("{} unique segment name", disableUniqueSegmentName ? "Disabled" : "Enabled");
      LOGGER.info("Start consistent push for table: " + rawTableName);
      Map<URI, List<String>> uriToExistingOfflineSegments = getSelectedOfflineSegments(spec, rawTableName);
      LOGGER.info("Existing segments for table {}: " + uriToExistingOfflineSegments, rawTableName);
      LOGGER.info("New segments for table: {}: " + segmentsTo, rawTableName);
      uriToLineageEntryIdMap = startReplaceSegments(spec, uriToExistingOfflineSegments, segmentsTo);
    }
    return uriToLineageEntryIdMap;
  }

  /**
   * uriToLineageEntryIdMap is non-empty if and only if consistent data push is enabled.
   * If uriToLineageEntryIdMap is non-empty, end the consistent data push protocol for each controller.
   */
  public static void postUpload(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap) {
    String rawTableName = spec.getTableSpec().getTableName();
    if (!uriToLineageEntryIdMap.isEmpty()) {
      LOGGER.info("End consistent push for table: " + rawTableName);
      endReplaceSegments(spec, uriToLineageEntryIdMap);
    }
  }

  public static List<String> getMetadataSegmentsTo(Map<String, String> segmentUriToTarPathMap) {
    List<String> segmentsTo = new ArrayList<>();
    for (String segmentUriPath : segmentUriToTarPathMap.keySet()) {
      String tarFilePath = segmentUriToTarPathMap.get(segmentUriPath);
      String fileName = new File(tarFilePath).getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
      segmentsTo.add(segmentName);
    }
    return segmentsTo;
  }

  public static Map<URI, URI> getStartReplaceSegmentUris(SegmentGenerationJobSpec spec, String rawTableName) {
    Map<URI, URI> baseUriToStartReplaceSegmentUriMap = new HashMap<>();
    for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
      URI controllerURI;
      try {
        controllerURI = new URI(pinotClusterSpec.getControllerURI());
        baseUriToStartReplaceSegmentUriMap.put(controllerURI,
            FileUploadDownloadClient.getStartReplaceSegmentsURI(controllerURI, rawTableName,
                TableType.OFFLINE.toString(), true));
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
      }
    }
    return baseUriToStartReplaceSegmentUriMap;
  }

  public static Map<URI, String> startReplaceSegments(SegmentGenerationJobSpec spec,
      Map<URI, List<String>> uriToSegmentsFrom, List<String> segmentsTo)
      throws Exception {
    Map<URI, String> uriToLineageEntryIdMap = new HashMap<>();
    String rawTableName = spec.getTableSpec().getTableName();
    Map<URI, URI> segmentsUris = getStartReplaceSegmentUris(spec, rawTableName);
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    LOGGER.info("Start replace segment URIs: " + segmentsUris);

    int attempts = 1;
    long retryWaitMs = 1000L;

    for (Map.Entry<URI, URI> entry : segmentsUris.entrySet()) {
      URI controllerUri = entry.getKey();
      URI startSegmentUri = entry.getValue();
      List<String> segmentsFrom = uriToSegmentsFrom.get(controllerUri);

      if (!Collections.disjoint(segmentsFrom, segmentsTo)) {
        // This scenario could happen only when "disable.unique.segment.name" is enabled,
        // i.e. newly generated segment name is the same as the existing one in the table. If so, we should
        //    1) either clean up all the consistent push custom config from table config,
        //    2) or remove "disable.unique.segment.name" to continue enabling consistent push.
        String errorMsg =
            String.format("Found same segment names when attempting to enable consistent push for table: %s",
                rawTableName);
        LOGGER.error(errorMsg);
        throw new RuntimeException(errorMsg);
      }

      StartReplaceSegmentsRequest startReplaceSegmentsRequest =
          new StartReplaceSegmentsRequest(segmentsFrom, segmentsTo);
      RetryPolicies.exponentialBackoffRetryPolicy(attempts, retryWaitMs, 5).attempt(() -> {
        try {
          SimpleHttpResponse response =
              FILE_UPLOAD_DOWNLOAD_CLIENT.startReplaceSegments(startSegmentUri, startReplaceSegmentsRequest,
                  authProvider);

          String responseString = response.getResponse();
          LOGGER.info(
              "Got response {}: {} while sending start replace segment request for table: {}, uploadURI: {}, request:"
                  + " {}", response.getStatusCode(), responseString, rawTableName, startSegmentUri,
              startReplaceSegmentsRequest);
          String segmentLineageEntryId =
              JsonUtils.stringToJsonNode(responseString).get("segmentLineageEntryId").asText();
          uriToLineageEntryIdMap.put(controllerUri, segmentLineageEntryId);
          return true;
        } catch (SocketTimeoutException se) {
          // In case of the timeout, we should re-try.
          return false;
        } catch (HttpErrorStatusException e) {
          if (e.getStatusCode() >= 500) {
            return false;
          } else {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
              LOGGER.error("Table: {} not found when sending request: {}", rawTableName, startSegmentUri);
            }
            throw e;
          }
        }
      });
    }
    return uriToLineageEntryIdMap;
  }

  public static void endReplaceSegments(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap) {
    AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());
    String rawTableName = spec.getTableSpec().getTableName();
    for (URI uri : uriToLineageEntryIdMap.keySet()) {
      String segmentLineageEntryId = uriToLineageEntryIdMap.get(uri);
      try {
        FILE_UPLOAD_DOWNLOAD_CLIENT.endReplaceSegments(
            FileUploadDownloadClient.getEndReplaceSegmentsURI(uri, rawTableName, TableType.OFFLINE.toString(),
                segmentLineageEntryId), HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, authProvider);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + uri + "'");
      } catch (HttpErrorStatusException | IOException e) {
        e.printStackTrace();
      }
    }
  }

  public static void handleUploadException(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap,
      Exception exception) {
    LOGGER.error("Exception when pushing segments. Marking segment lineage entry to 'REVERTED'.", exception);
    String rawTableName = spec.getTableSpec().getTableName();
    for (Map.Entry<URI, String> entry : uriToLineageEntryIdMap.entrySet()) {
      String segmentLineageEntryId = entry.getValue();
      try {
        URI uri =
            FileUploadDownloadClient.getRevertReplaceSegmentsURI(entry.getKey(), rawTableName, TableType.OFFLINE.name(),
                segmentLineageEntryId, true);
        SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT.revertReplaceSegments(uri);
        LOGGER.info("Got response {}: {} while sending revert replace segment request for table: {}, uploadURI: {}",
            response.getStatusCode(), response.getResponse(), rawTableName, entry.getKey());
      } catch (URISyntaxException | HttpErrorStatusException | IOException e) {
        LOGGER.error("Exception when sending revert replace segment request to controller: {} for table: {}",
            entry.getKey(), rawTableName, e);
      }
    }
  }

  /**
   * Ensures that all files in tarFilePaths have the expected tar file extension and strip the extension to obtain
   * segment names.
   */
  public static List<String> getTarSegmentsTo(List<String> tarFilePaths) {
    List<String> segmentNames = new ArrayList<>();
    for (String tarFilePath : tarFilePaths) {
      File tarFile = new File(tarFilePath);
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
      segmentNames.add(segmentName);
    }
    return segmentNames;
  }

  public static TableConfig getTableConfig(SegmentGenerationJobSpec spec) {
    String rawTableName = spec.getTableSpec().getTableName();
    TableConfig tableConfig =
        SegmentGenerationUtils.getTableConfig(spec.getTableSpec().getTableConfigURI(), spec.getAuthToken());
    Preconditions.checkState(tableConfig != null, "Failed to find table config for table: %s", rawTableName);
    return tableConfig;
  }

  public static boolean consistentDataPushEnabled(SegmentGenerationJobSpec spec) {
    TableConfig tableConfig = getTableConfig(spec);
    // Enable consistent data push only if "consistentDataPush" is set to true in batch ingestion config and the
    // table is REFRESH use case.
    return "REFRESH".equalsIgnoreCase(IngestionConfigUtils.getBatchSegmentIngestionType(tableConfig))
        && IngestionConfigUtils.getBatchSegmentIngestionConsistentDataPushEnabled(tableConfig);
  }

  public static boolean uniqueSegmentNameDisabled(SegmentGenerationJobSpec spec) {
    TableConfig tableConfig = getTableConfig(spec);
    return IngestionConfigUtils.getBatchSegmentIngestionDisableUniqueSegmentNames(tableConfig);
  }

  /**
   * Returns a map of controller URI to a list of existing OFFLINE segments.
   */
  public static Map<URI, List<String>> getSelectedOfflineSegments(SegmentGenerationJobSpec spec, String rawTableName) {
    Map<URI, List<String>> uriToOfflineSegments = new HashMap<>();
    for (PinotClusterSpec pinotClusterSpec : spec.getPinotClusterSpecs()) {
      URI controllerURI;
      List<String> offlineSegments;
      try {
        controllerURI = new URI(pinotClusterSpec.getControllerURI());
        Map<String, List<String>> segments = FILE_UPLOAD_DOWNLOAD_CLIENT.getSegments(controllerURI, rawTableName, TableType.OFFLINE.toString(), true);
        offlineSegments = segments.get(TableType.OFFLINE.toString());
        uriToOfflineSegments.put(controllerURI, offlineSegments);
      } catch (URISyntaxException e) {
        throw new RuntimeException("Got invalid controller uri - '" + pinotClusterSpec.getControllerURI() + "'");
      } catch (HttpErrorStatusException | IOException e) {
        e.printStackTrace();
      }
    }
    return uriToOfflineSegments;
  }

  public static void configureSegmentPostfix(SegmentGenerationJobSpec spec) {
    SegmentNameGeneratorSpec segmentNameGeneratorSpec = spec.getSegmentNameGeneratorSpec();
    if (consistentDataPushEnabled(spec) && !uniqueSegmentNameDisabled(spec)) {
      // Append current timestamp to existing configured segment name postfix, if configured, to make segment name
      // unique.
      if (segmentNameGeneratorSpec == null) {
        segmentNameGeneratorSpec = new SegmentNameGeneratorSpec();
      }
      String existingPostfix = segmentNameGeneratorSpec.getConfigs().getOrDefault(SEGMENT_NAME_POSTFIX, "");
      segmentNameGeneratorSpec.addConfig(SEGMENT_NAME_POSTFIX,
          String.join("_", existingPostfix, Long.toString(System.currentTimeMillis())));
      spec.setSegmentNameGeneratorSpec(segmentNameGeneratorSpec);
    }
  }
}
