/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.tools.mlog;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.logfile.MLogTxtWriter;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.CreateTemplatePlan;
import org.apache.iotdb.db.qp.physical.crud.SetDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.AutoCreateDeviceMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateAlignedTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.DropContinuousQueryPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTTLPlan;
import org.apache.iotdb.db.qp.physical.sys.SetUsingDeviceTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** parse the binary mlog or snapshot to text */
public class MLogParser {

  private static final Logger logger = LoggerFactory.getLogger(MLogParser.class);
  private static final String MLOG_CLI_PREFIX = "MlogParser";

  private static final String FILE_ARGS = "f";
  private static final String FILE_NAME = "mlog file";

  private static final String OUT_ARGS = "o";
  private static final String OUT_NAME = "output txt file";

  private static final String HELP_ARGS = "help";

  private static String inputFile;
  private static String outputFile;

  /**
   * create the commandline options.
   *
   * @return object Options
   */
  public static Options createOptions() {
    Options options = new Options();

    Option opFile =
        Option.builder(FILE_ARGS)
            .required()
            .argName(FILE_NAME)
            .hasArg()
            .desc("Need to specify a binary mlog file to parse (required)")
            .build();
    options.addOption(opFile);

    Option opOut =
        Option.builder(OUT_ARGS)
            .required(false)
            .argName(OUT_NAME)
            .hasArg()
            .desc("Could specify the output file after parse (optional)")
            .build();
    options.addOption(opOut);

    Option opHelp =
        Option.builder(HELP_ARGS)
            .longOpt(HELP_ARGS)
            .hasArg(false)
            .desc("Display help information")
            .build();
    options.addOption(opHelp);

    return options;
  }

  public static void main(String[] args) {
    Options options = createOptions();
    HelpFormatter hf = new HelpFormatter();
    hf.setOptionComparator(null);
    CommandLine commandLine;
    CommandLineParser parser = new DefaultParser();

    if (args == null || args.length == 0) {
      logger.warn("Too few params input, please check the following hint.");
      hf.printHelp(MLOG_CLI_PREFIX, options, true);
      return;
    }
    try {
      commandLine = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error("Parse error: {}", e.getMessage());
      hf.printHelp(MLOG_CLI_PREFIX, options, true);
      return;
    }
    if (commandLine.hasOption(HELP_ARGS)) {
      hf.printHelp(MLOG_CLI_PREFIX, options, true);
      return;
    }

    try {
      parseBasicParams(commandLine);
      parseFromFile(inputFile, outputFile);
    } catch (Exception e) {
      logger.error("Encounter an error, because: {} ", e.getMessage());
    }
  }

  public static void parseBasicParams(CommandLine commandLine) throws ParseException {
    inputFile = checkRequiredArg(FILE_ARGS, FILE_NAME, commandLine);
    outputFile = commandLine.getOptionValue(OUT_ARGS);

    if (outputFile == null) {
      outputFile = "tmp.txt";
    }
  }

  public static String checkRequiredArg(String arg, String name, CommandLine commandLine)
      throws ParseException {
    String str = commandLine.getOptionValue(arg);
    if (str == null) {
      String msg = String.format("Required values for option '%s' not provided", name);
      logger.info(msg);
      logger.info("Use -help for more information");
      throw new ParseException(msg);
    }
    return str;
  }

  public static void parseFromFile(String inputFile, String outputFile) throws IOException {
    try (MLogReader mLogReader = new MLogReader(inputFile);
        MLogTxtWriter mLogTxtWriter = new MLogTxtWriter(outputFile)) {

      while (mLogReader.hasNext()) {
        PhysicalPlan plan = mLogReader.next();
        switch (plan.getOperatorType()) {
          case CREATE_TIMESERIES:
            mLogTxtWriter.createTimeseries(
                (CreateTimeSeriesPlan) plan, ((CreateTimeSeriesPlan) plan).getTagOffset());
            break;
          case CREATE_ALIGNED_TIMESERIES:
            mLogTxtWriter.createAlignedTimeseries((CreateAlignedTimeSeriesPlan) plan);
            break;
          case DELETE_TIMESERIES:
            for (PartialPath partialPath : plan.getPaths()) {
              mLogTxtWriter.deleteTimeseries(partialPath.getFullPath());
            }
            break;
          case SET_STORAGE_GROUP:
            mLogTxtWriter.setStorageGroup(((SetStorageGroupPlan) plan).getPath().getFullPath());
            break;
          case DELETE_STORAGE_GROUP:
            for (PartialPath partialPath : plan.getPaths()) {
              mLogTxtWriter.deleteStorageGroup(partialPath.getFullPath());
            }
            break;
          case TTL:
            mLogTxtWriter.setTTL(
                ((SetTTLPlan) plan).getStorageGroup().getFullPath(),
                ((SetTTLPlan) plan).getDataTTL());
            break;
          case CHANGE_ALIAS:
            mLogTxtWriter.changeAlias(
                ((ChangeAliasPlan) plan).getPath().getFullPath(),
                ((ChangeAliasPlan) plan).getAlias());
            break;
          case CHANGE_TAG_OFFSET:
            mLogTxtWriter.changeOffset(
                ((ChangeTagOffsetPlan) plan).getPath().getFullPath(),
                ((ChangeTagOffsetPlan) plan).getOffset());
            break;
          case MEASUREMENT_MNODE:
            mLogTxtWriter.serializeMeasurementMNode((MeasurementMNodePlan) plan);
            break;
          case STORAGE_GROUP_MNODE:
            mLogTxtWriter.serializeStorageGroupMNode((StorageGroupMNodePlan) plan);
            break;
          case MNODE:
            mLogTxtWriter.serializeMNode((MNodePlan) plan);
            break;
          case CREATE_CONTINUOUS_QUERY:
            mLogTxtWriter.createContinuousQuery((CreateContinuousQueryPlan) plan);
            break;
          case DROP_CONTINUOUS_QUERY:
            mLogTxtWriter.dropContinuousQuery((DropContinuousQueryPlan) plan);
            break;
          case CREATE_TEMPLATE:
            mLogTxtWriter.createTemplate((CreateTemplatePlan) plan);
            break;
          case SET_DEVICE_TEMPLATE:
            mLogTxtWriter.setTemplate((SetDeviceTemplatePlan) plan);
            break;
          case SET_USING_DEVICE_TEMPLATE:
            mLogTxtWriter.setUsingTemplate((SetUsingDeviceTemplatePlan) plan);
            break;
          case AUTO_CREATE_DEVICE_MNODE:
            mLogTxtWriter.autoCreateDeviceNode(
                ((AutoCreateDeviceMNodePlan) plan).getPath().getFullPath());
            break;
          default:
            logger.warn("unknown plan {}", plan);
        }
      }
    }
  }
}
