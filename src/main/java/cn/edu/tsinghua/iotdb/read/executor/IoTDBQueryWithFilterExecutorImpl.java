package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.IoTDBQueryDataSetForQueryWithQueryFilterImpl;
import cn.edu.tsinghua.iotdb.read.IoTDBQueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBQueryByTimestampsReader;
import cn.edu.tsinghua.iotdb.read.timegenerator.IoTDBTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.BinaryQueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor with filter
 * */
public class IoTDBQueryWithFilterExecutorImpl{

    public IoTDBQueryWithFilterExecutorImpl() {}

    public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        TimestampGenerator  timestampGenerator = new IoTDBTimeGenerator(queryExpression.getQueryFilter());

        LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), queryExpression.getQueryFilter());
        return new IoTDBQueryDataSetForQueryWithQueryFilterImpl(timestampGenerator, readersOfSelectedSeries);
    }

    private static void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReaderByTimeStamp> readersOfSelectedSeries,
                                             List<Path> selectedSeries, QueryFilter queryFilter) throws IOException, FileNodeManagerException {

        LinkedHashMap<Path, SeriesFilter> path2SeriesFilter = parseSeriesFilter(queryFilter);

        for (Path path : selectedSeries) {
             QueryDataSource queryDataSource = null;
             if(path2SeriesFilter.containsKey(path)){
                 queryDataSource = IoTDBQueryDataSourceExecutor.getQueryDataSource(path2SeriesFilter.get(path));
             }
             else {
                 queryDataSource = IoTDBQueryDataSourceExecutor.getQueryDataSource(path);
             }
             SeriesReaderByTimeStamp seriesReader = new IoTDBQueryByTimestampsReader(queryDataSource);
             readersOfSelectedSeries.put(path, seriesReader);
        }
    }

    private static LinkedHashMap parseSeriesFilter(QueryFilter queryFilter){
        LinkedHashMap<Path, SeriesFilter> path2SeriesFilter = new LinkedHashMap<Path, SeriesFilter>();
        traverse(queryFilter, path2SeriesFilter);
        return path2SeriesFilter;
    }

    private static void traverse(QueryFilter queryFilter, LinkedHashMap path2SeriesFilter){
        if(queryFilter.getType() == QueryFilterType.SERIES){
            SeriesFilter seriesFilter = (SeriesFilter) queryFilter;
            path2SeriesFilter.put(seriesFilter.getSeriesPath(), seriesFilter);
        }
        else{
            BinaryQueryFilter binaryQueryFilter = (BinaryQueryFilter) queryFilter;
            traverse(binaryQueryFilter.getLeft(), path2SeriesFilter);
            traverse(binaryQueryFilter.getRight(), path2SeriesFilter);
        }
    }
}
