package org.apache.iotdb.db.tools.logvisual;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.iotdb.db.tools.logvisual.exceptions.NoLogFileLoadedException;
import org.apache.iotdb.db.tools.logvisual.exceptions.UnmatchedContentException;
import org.apache.iotdb.db.tools.logvisual.exceptions.VisualizationException;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.time.Millisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * LogVisualizer is the backend of this tool, which parses, groups and plots the logs according
 * to a given visualization plan.
 * The general procedure of one visualization would be:
 *  1. call setLogFile() and setPropertyFile() to set those files;
 *  2. call loadLogParser() to create a logParser;
 *  3. call executePlan() to execute a visualization plan.
 *  4. call getCharts() and getStatisticMap() to get the results.
 */
public class LogVisualizer {

  private static final Logger logger = LoggerFactory.getLogger(LogVisualizer.class);

  private LogParser logParser;
  /**
   * logCache stores the logs that satisfies the filter of the plan being executed.
   */
  private List<LogEntry> logCache = new ArrayList<>();
  /**
   * logGroups stores the logs by their their tags (specified by the plan).
   */
  private Map<List<String>, List<LogEntry>> logGroups = new HashMap<>();

  /**
   * the plans currently loaded into memory.
   */
  private Map<String, VisualizationPlan> plans = new HashMap<>();

  /**
   * the timeseries plots generated for each group of logs.
   * measurement name -> the chart of the measurement
   */
  private Map<String, JFreeChart> charts;
  /**
   * the statistics (count, mean, min, max) of each group of logs.
   * measurement name -> the statistics of the measurement of each group
   */
  private Map<String, List<TimeSeriesStatistics>> statisticsMap;

  /**
   * the file that defines how to parse the logs in the log file.
   */
  private File parserPropertyFile;
  /**
   * the file that contains the logs to be visualized.
   */
  private File logFile;

  // params: <LogFilePath> <ParserPropertyFilePath> <PlanFilePath> <OutputDirectory>
  public static void main(String[] args) throws IOException, VisualizationException {
    if (args.length != 4) {
      System.out.println("Wrong number of parameters: "
          + " params: <LogFilePath> <ParserPropertyFilePath> <PlanFilePath> <OutputDirectory>");
      return;
    }
    File logFile = new File(args[0]);
    if (!logFile.exists() || !logFile.isFile()) {
      System.out.println("Log file does not exist");
      return;
    }
    File parserPropertyFile = new File(args[1]);
    if (!parserPropertyFile.exists() || !parserPropertyFile.isFile()) {
      System.out.println("Parser property file does not exist");
      return;
    }
    File planFile = new File(args[2]);
    if (!planFile.exists()) {
      System.out.println("Plan file does not exist");
      return;
    }

    File destDir = new File(args[3]);
    if (destDir.exists() && !destDir.isDirectory()) {
      System.out.println("The output path is not a directory");
      return;
    }
    destDir.mkdirs();

    LogVisualizer visualizer = new LogVisualizer();
    visualizer.setLogFile(logFile);
    visualizer.setParserPropertyFile(parserPropertyFile);
    visualizer.loadLogParser();

    visualizer.loadPlan(planFile);
    Collection<VisualizationPlan> plans = visualizer.listPlans();

    for (VisualizationPlan plan : plans) {
      visualizer.executePlan(plan);
      visualizer.saveResults(destDir.getPath() + File.separator + plan.getName());
      System.out.println("Executed and saved results of plan "+ plan.getName());
    }

    System.out.println("Visualization completed");
  }

  private void clearLogGroups() {
    // log cache is cleared after logGroups are generated, so we do not clear it here
    logGroups.clear();
  }

  public void loadLogParser() throws IOException {
    if (parserPropertyFile == null) {
      throw new IOException("Parser property file unset!");
    }
    if (logFile == null) {
      throw new IOException("Log file unset!");
    }
    // close the previous
    close();
    String propertyFilePath = parserPropertyFile.getPath();
    String logFilePath = logFile.getPath();
    Properties properties = new Properties();
    try (FileInputStream inputStream = new FileInputStream(propertyFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream)) {
      properties.load(bufferedInputStream);
    }
    logParser = new PatternLogParser(properties, logFilePath);
  }

  public void close() throws IOException {
    if (logParser != null) {
      logParser.close();
      logParser = null;
    }
  }

  public void loadPlans(File[] planFiles) throws IOException {
    for (File file : planFiles) {
      loadPlan(file);
    }
  }

  private void loadPlan(File file) throws IOException {
    if (!file.exists()) {
      return;
    }
    if (file.isDirectory()) {
      loadPlans(file.listFiles());
    } else {
      loadPlan(file.getPath());
    }
  }

  private void loadPlan(String planFilePath) throws IOException {
    VisualizationPlan plan = new VisualizationPlan(planFilePath);
    plans.put(plan.getName(), plan);
  }

  public Collection<VisualizationPlan> listPlans() {
    return plans.values();
  }

  public void executePlan(VisualizationPlan plan) throws VisualizationException {
    if (logParser == null) {
      throw new NoLogFileLoadedException();
    }
    try {
      // read the logs fom the beginning
      logParser.reset();
    } catch (IOException e) {
      throw new VisualizationException(e);
    }
    collectLogs(plan);
    groupLogs();
    // <measurementName, <tag, timeseries>>
    Map<String,Map<String, TimeSeries>> taggedTimeSeries = createTimeSeries(plan);
    charts = drawCharts(taggedTimeSeries, plan);
    statisticsMap = genStatisticMap(taggedTimeSeries);
    clearLogGroups();
  }

  /**
   * Read all logs from the logParser and filter unwanted or malformed ones.
   * @param plan
   * @throws VisualizationException
   */
  private void collectLogs(VisualizationPlan plan) throws VisualizationException {
    LogFilter logFilter = plan.getLogFilter();
    try {
      LogEntry logEntry;
      readLogs:
      while ((logEntry = logParser.next()) != null) {
        try {
          plan.parseContents(logEntry);
        } catch (UnmatchedContentException e) {
          continue;
        }

        switch (logFilter.filter(logEntry)) {
          // all logs must be time-ordered, so on meeting a log that is too new we can end the loop
          case BEYOND_END_TIME:
            break readLogs;
          case REJECT:
            break;
          case OK:
            logCache.add(logEntry);
        }
      }
    } catch (IOException e) {
      throw new VisualizationException(e);
    }
    logger.info("Collected {} logs from {}", logCache.size(), logFile.getPath());
  }

  private void groupLogs() {
    for (LogEntry logEntry : logCache) {
      logGroups.computeIfAbsent(logEntry.getTags(), tag -> new ArrayList<>()).add(logEntry);
    }
    logCache.clear();
    logger.info("Found {} different tags", logGroups.size());
  }

  // <measurementName, <tag, timeseries>>
  private Map<String, Map<String, TimeSeries>> createTimeSeries(VisualizationPlan plan) {
    Map<String, Map<String, TimeSeries>> ret = new HashMap<>();
    for (Entry<List<String>, List<LogEntry>> entry : logGroups.entrySet()) {
      List<String> tags = entry.getKey();
      List<LogEntry> logs = entry.getValue();
      // create a tag string for the log group as the key of the timeseries
      String concatenatedTag;
      if (tags.isEmpty()) {
        concatenatedTag = plan.getName() + " ";
      } else {
        StringBuilder builder = new StringBuilder(plan.getName() + "-" + tags.get(0));
        for (int i = 1; i < tags.size(); i++) {
          builder.append(",").append(tags.get(i));
        }
        builder.append(" ");
        concatenatedTag = builder.toString();
      }

      if (plan.getMeasurementPositions() != null) {
        // the measurements are given, create a timeseries for each measurement
        String[] legends = plan.getLegends();
        // use the values in each log to build the timeseries
        for (LogEntry logEntry : logs) {
          List<Double> values = logEntry.getMeasurements();
          for (int i = 0; i < values.size(); i++) {
            String legend = legends[i];
            TimeSeries timeSeries = ret.computeIfAbsent(legend, leg -> new HashMap<>())
                .computeIfAbsent(concatenatedTag, tag -> new TimeSeries(tag + legend));
            timeSeries.addOrUpdate(new Millisecond(logEntry.getDate()), values.get(i));
          }
        }
      } else {
        // the measurement are not given, just record the time when each log happened
        String legend = "TimeOfOccurrence";
        TimeSeries happenedInstance = ret.computeIfAbsent(legend, tag -> new HashMap<>())
            .computeIfAbsent(concatenatedTag, tag -> new TimeSeries( tag + legend));
        for (LogEntry logEntry : logs) {
          happenedInstance.addOrUpdate(new Millisecond(logEntry.getDate()), 1.0);
        }
      }
    }
    return ret;
  }

  private Map<String, JFreeChart> drawCharts(Map<String, Map<String, TimeSeries>> taggedTimeSeries,
      VisualizationPlan plan) {
    Map<String, JFreeChart> charts = new HashMap<>();
    for (Entry<String, Map<String, TimeSeries>> entry : taggedTimeSeries.entrySet()) {
      String measurementName = entry.getKey();
      TimeSeriesCollection timeSeriesList = new TimeSeriesCollection();
      for (TimeSeries timeSeries : entry.getValue().values()) {
        timeSeriesList.addSeries(timeSeries);
      }
      // contain the start time of the timeseries in the x-axis name
      Date startDate = new Date((long) timeSeriesList.getDomainBounds(true).getLowerBound());
      JFreeChart chart = ChartFactory.createTimeSeriesChart(measurementName, "time-"+ startDate, measurementName,
          timeSeriesList);
      XYPlot xyPlot = chart.getXYPlot();
      XYLineAndShapeRenderer xyLineAndShapeRenderer = ((XYLineAndShapeRenderer) xyPlot
          .getRenderer());
      // show the origin data points in the plot
      xyLineAndShapeRenderer.setDefaultShapesVisible(true);
      xyLineAndShapeRenderer.setDefaultShapesFilled(true);
      if (plan.getMeasurementPositions() == null) {
        // do not draw lines if we only record the time instances of the logs
        xyLineAndShapeRenderer.setDefaultLinesVisible(false);
      }
      charts.put(measurementName, chart);
    }
    return charts;
  }

  public Map<String, JFreeChart> getCharts() {
    return charts;
  }

  public void setParserPropertyFile(File parserPropertyFile) {
    this.parserPropertyFile = parserPropertyFile;
  }

  public void setLogFile(File logFile) {
    this.logFile = logFile;
  }

  private Map<String, List<TimeSeriesStatistics>> genStatisticMap(Map<String, Map<String, TimeSeries>>
      taggedTimeSeries) {
    Map<String, List<TimeSeriesStatistics>> ret = new HashMap<>();
    for (Entry<String, Map<String, TimeSeries>> timeSeriesEntry : taggedTimeSeries.entrySet()) {
      // calculate the statistics of the logs in each group
      String measurementName = timeSeriesEntry.getKey();
      List<TimeSeriesStatistics> seriesStatistics = new ArrayList<>();
      Map<String, TimeSeries> seriesMap = timeSeriesEntry.getValue();
      for (TimeSeries timeSeries : seriesMap.values()) {
        seriesStatistics.add(new TimeSeriesStatistics(timeSeries));
      }
      ret.put(measurementName, seriesStatistics);
    }
    return ret;
  }

  public void saveResults(String destDirPath) throws VisualizationException {
    if (charts == null || statisticsMap == null) {
      throw new VisualizationException("No results to be saved");
    }

    File destDir = new File(destDirPath);
    if (destDir.exists() && !destDir.isDirectory()) {
      throw new VisualizationException(String.format("%s exists and is not a directory", destDirPath));
    }
    if (!destDir.exists() && !destDir.mkdirs()) {
      throw new VisualizationException(String.format("Cannot create directory %s", destDirPath));
    }
    destDir.mkdirs();

    // save charts
    for (Entry<String, JFreeChart> chartEntry : charts.entrySet()) {
      File chartFile = new File(destDir, chartEntry.getKey() + ".png");
      JFreeChart chart = chartEntry.getValue();
      try (FileOutputStream fileOutputStream = new FileOutputStream(chartFile);
          BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(fileOutputStream)) {
        ChartUtils.writeChartAsPNG(bufferedOutputStream, chart, 800, 600);
      } catch (IOException e) {
        throw new VisualizationException(String.format("Cannot save chart of %s", chartEntry.getKey()), e);
      }
    }

    // save statistics
    File statisticFile = new File(destDir, "statistic.csv");
    try (FileWriter fileWriter = new FileWriter(statisticFile);
        BufferedWriter writer = new BufferedWriter(fileWriter)) {
      TimeSeriesStatistics.serializeHeader(writer);
      for (Entry<String, List<TimeSeriesStatistics>> statisticEntry : statisticsMap.entrySet()) {
        for (TimeSeriesStatistics timeSeriesStatistics : statisticEntry.getValue()) {
          timeSeriesStatistics.serialize(writer);
        }
      }
    } catch (IOException e) {
      throw new VisualizationException(String.format("Cannot save statistics to %s", destDirPath), e);
    }
  }

  public Map<String, List<TimeSeriesStatistics>> getStatisticsMap() {
    return statisticsMap;
  }
}