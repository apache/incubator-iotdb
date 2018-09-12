package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.executor.IoTDBQueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.IoTDBQueryWithFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.IoTDBQueryWithoutFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IoTDBQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBQueryEngine.class);

    public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
        if (queryExpression.hasQueryFilter()) {
            try {
                QueryFilter queryFilter = queryExpression.getQueryFilter();
                QueryFilter regularQueryFilter = QueryFilterOptimizer.getInstance().convertGlobalTimeFilter(queryFilter, queryExpression.getSelectedSeries());
                queryExpression.setQueryFilter(regularQueryFilter);
                if (regularQueryFilter.getType() == QueryFilterType.GLOBAL_TIME) {
                    return IoTDBQueryWithGlobalTimeFilterExecutorImpl.execute(queryExpression);
                } else {
                    return IoTDBQueryWithFilterExecutorImpl.execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return IoTDBQueryWithoutFilterExecutorImpl.execute(queryExpression);
        }
    }
}
