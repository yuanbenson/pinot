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
package org.apache.pinot.plugin.ingestion.batch.hadoop;

import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.runner.SegmentPushJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;

public class HadoopSegmentUriPushJobRunner extends SegmentPushJobRunner implements Serializable {

  public HadoopSegmentUriPushJobRunner() {
  }

  public HadoopSegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  public void pushSegments(Triple<String[], PinotFS, URI> fileSysParams) {
    String[] files = fileSysParams.getLeft();
    URI outputDirURI = fileSysParams.getRight();

    List<String> segmentUris = new ArrayList<>();
    for (String file : files) {
      URI uri = URI.create(file);
      if (uri.getPath().endsWith(Constants.TAR_GZ_FILE_EXT)) {
        URI updatedURI = SegmentPushUtils
            .generateSegmentTarURI(outputDirURI, uri, _spec.getPushJobSpec().getSegmentUriPrefix(),
                _spec.getPushJobSpec().getSegmentUriSuffix());
        segmentUris.add(updatedURI.toString());
      }
    }
    ConsistentDataPushUtils.pushSegmentsUris(_spec, segmentUris);
  }
}
