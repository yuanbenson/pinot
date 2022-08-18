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
package org.apache.pinot.plugin.ingestion.batch.spark;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.pinot.plugin.ingestion.batch.common.BaseSegmentPushJobRunner;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;


public class SparkSegmentMetadataPushJobRunner extends BaseSegmentPushJobRunner
    implements Serializable {

  public SparkSegmentMetadataPushJobRunner() {
  }

  public SparkSegmentMetadataPushJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  public void getSegmentsToPush() {
    for (String file : _files) {
      if (file.endsWith(Constants.TAR_GZ_FILE_EXT)) {
        _segmentsToPush.add(file);
      }
    }
  }

  public void pushSegments() {
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();

    int pushParallelism = _spec.getPushJobSpec().getPushParallelism();
    if (pushParallelism < 1) {
      pushParallelism = _segmentsToPush.size();
    }
    if (pushParallelism == 1) {
      // Push from driver
      try {
        SegmentPushUtils.pushSegments(_spec, _outputDirFS, _segmentsToPush);
      } catch (RetriableOperationException | AttemptsExceededException e) {
        throw new RuntimeException(e);
      }
    } else {
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      JavaRDD<String> pathRDD = sparkContext.parallelize(_segmentsToPush, pushParallelism);
      URI finalOutputDirURI = _outputDirURI;
      // Prevent using lambda expression in Spark to avoid potential serialization exceptions, use inner function
      // instead.
      pathRDD.foreach(new VoidFunction<String>() {
        @Override
        public void call(String segmentTarPath)
            throws Exception {
          PluginManager.get().init();
          for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
            PinotFSFactory
                .register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
          }
          try {
            Map<String, String> segmentUriToTarPathMap = SegmentPushUtils
                .getSegmentUriToTarPathMap(finalOutputDirURI, _spec.getPushJobSpec(), new String[]{segmentTarPath});
            SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(finalOutputDirURI.getScheme()),
                segmentUriToTarPathMap);
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
  }
}
