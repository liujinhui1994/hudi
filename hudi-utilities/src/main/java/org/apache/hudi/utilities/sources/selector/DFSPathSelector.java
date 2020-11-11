/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.selector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class DFSPathSelector extends AbstractDFSPathSelector {

  protected static volatile Logger LOG = LogManager.getLogger(DFSPathSelector.class);
  protected static final List<String> IGNORE_FILEPREFIX_LIST = Arrays.asList(".", "_");
  protected final transient FileSystem fs;

  public DFSPathSelector(TypedProperties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    this.fs = FSUtils.getFs(inputPath, hadoopConf);
  }

  /**
   * Get the list of files changed since last checkpoint.
   *
   * @param lastCheckpointStr the last checkpoint time string, empty if first run
   * @param sourceLimit       max bytes to read each time
   * @return the list of files concatenated and their latest modified time
   */
  @Override
  public Pair<Option<String>, String> getNextFilePathsAndMaxModificationTime(Option<String> lastCheckpointStr,
      long sourceLimit) {

    try {
      // obtain all eligible files under root folder.
      LOG.info("Root path => " + inputPath + " source limit => " + sourceLimit);
      long lastCheckpointTime = lastCheckpointStr.map(Long::parseLong).orElse(Long.MIN_VALUE);
      List<FileStatus> eligibleFiles = listEligibleFiles(fs, new Path(inputPath), lastCheckpointTime);
      // sort them by modification time.
      eligibleFiles.sort(Comparator.comparingLong(FileStatus::getModificationTime));
      // Filter based on checkpoint & input size, if needed
      long currentBytes = 0;
      long maxModificationTime = Long.MIN_VALUE;
      List<FileStatus> filteredFiles = new ArrayList<>();
      for (FileStatus f : eligibleFiles) {
        if (currentBytes + f.getLen() >= sourceLimit) {
          // we have enough data, we are done
          break;
        }

        maxModificationTime = f.getModificationTime();
        currentBytes += f.getLen();
        filteredFiles.add(f);
      }

      // no data to read
      if (filteredFiles.isEmpty()) {
        return new ImmutablePair<>(Option.empty(), lastCheckpointStr.orElseGet(() -> String.valueOf(Long.MIN_VALUE)));
      }

      // read the files out.
      String pathStr = filteredFiles.stream().map(f -> f.getPath().toString()).collect(Collectors.joining(","));

      return new ImmutablePair<>(Option.ofNullable(pathStr), String.valueOf(maxModificationTime));
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to read from source from checkpoint: " + lastCheckpointStr, ioe);
    }
  }

  /**
   * List files recursively, filter out illegible files/directories while doing so.
   */
  private List<FileStatus> listEligibleFiles(FileSystem fs, Path path, long lastCheckpointTime) throws IOException {
    // skip files/dirs whose names start with (_, ., etc)
    FileStatus[] statuses = fs.listStatus(path, file ->
      IGNORE_FILEPREFIX_LIST.stream().noneMatch(pfx -> file.getName().startsWith(pfx)));
    List<FileStatus> res = new ArrayList<>();
    for (FileStatus status: statuses) {
      if (status.isDirectory()) {
        // avoid infinite loop
        if (!status.isSymlink()) {
          res.addAll(listEligibleFiles(fs, status.getPath(), lastCheckpointTime));
        }
      } else if (status.getModificationTime() > lastCheckpointTime && status.getLen() > 0) {
        res.add(status);
      }
    }
    return res;
  }
}