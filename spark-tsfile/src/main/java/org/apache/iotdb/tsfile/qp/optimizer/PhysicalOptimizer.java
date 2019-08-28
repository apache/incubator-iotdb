package org.apache.iotdb.tsfile.qp.optimizer;

import org.apache.iotdb.tsfile.qp.common.*;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.util.*;

public class PhysicalOptimizer {

    //determine whether to query all delta_objects from TSFile. true means do query.
    private boolean flag;
    private List<String> validDeltaObjects = new ArrayList<>();
    private List<String> columnNames;

    public PhysicalOptimizer(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<TSQueryPlan> optimize(SingleQuery singleQuery, List<String> paths,
                                      TsFileSequenceReader in, Long start, Long end) throws IOException {
        List<String> actualDeltaObjects = in.getDeviceNameInRange(start, end);
        List<MeasurementSchema> actualSeries = in.readFileMetadata().getMeasurementSchemaList();

        List<String> selectedSeries = new ArrayList<>();
        for (String path : paths) {
            if (!columnNames.contains(path) && !path.equals(SQLConstant.RESERVED_TIME)) {
                selectedSeries.add(path);
            }
        }
        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;

        if (singleQuery != null) {
            timeFilter = singleQuery.getTimeFilterOperator();
            valueFilter = singleQuery.getValueFilterOperator();
            if (valueFilter != null) {
                List<String> filterPaths = valueFilter.getAllPaths();
                List<String> actualPaths = new ArrayList<>();
                for (MeasurementSchema series : actualSeries) {
                    actualPaths.add(series.getMeasurementId());
                }
                //if filter paths doesn't in tsfile, don't query
                if (!actualPaths.containsAll(filterPaths))
                    return new ArrayList<>();
            }

            flag = true;
            Map<String, Set<String>> selectColumns = mergeColumns(singleQuery.getColumnFilterOperator());
            if (!flag) {
                //e.g. where column1 = 'd1' and column2 = 'd2', should not query
                return new ArrayList<>();
            }

            //if select deltaObject, then match with measurement
            if (!selectColumns.isEmpty()) {
                combination(actualDeltaObjects, selectColumns, selectColumns.keySet().toArray(), 0, new String[selectColumns.size()]);
            } else {
                validDeltaObjects.addAll(in.getDeviceNameInRange(start, end));
            }
        } else {
            validDeltaObjects.addAll(in.getDeviceNameInRange(start, end));
        }

        List<MeasurementSchema> fileSeries = in.readFileMetadata().getMeasurementSchemaList();
        Set<String> seriesSet = new HashSet<>();
        for (MeasurementSchema series : fileSeries) {
            seriesSet.add(series.getMeasurementId());
        }

        //query all measurements from TSFile
        if (selectedSeries.size() == 0) {
            for (MeasurementSchema series : actualSeries) {
                selectedSeries.add(series.getMeasurementId());
            }
        } else {
            //remove paths that doesn't exist in file
            selectedSeries.removeIf(path -> !seriesSet.contains(path));
        }

        List<TSQueryPlan> tsFileQueries = new ArrayList<>();
        for (String deltaObject : validDeltaObjects) {
            List<String> newPaths = new ArrayList<>();
            for (String path : selectedSeries) {
                String newPath = deltaObject + SQLConstant.PATH_SEPARATOR + path;
                newPaths.add(newPath);
            }
            if (valueFilter == null) {
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, null));
            } else {
                FilterOperator newValueFilter = valueFilter.clone();
                newValueFilter.addHeadDeltaObjectPath(deltaObject);
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, newValueFilter));
            }
        }
        return tsFileQueries;
    }


    /**
     * calculate combinations of selected columns and add valid deltaObjects to validDeltaObjects
     *
     * @param actualDeltaObjects deltaObjects from file
     * @param columnValues       e.g. (device:{d1,d2}) (board:{c1,c2}) or (delta_object:{d1,d2})
     * @param columns            e.g. device, board
     * @param beginIndex         current recursion list index
     * @param values             combination of column values
     */
    private void combination(List<String> actualDeltaObjects, Map<String, Set<String>> columnValues, Object[] columns, int beginIndex, String[] values) {
        // which should in column names -> now just device_name
        // use delta_object column
        if (columnValues.containsKey(SQLConstant.RESERVED_DELTA_OBJECT)) {
            Set<String> delta_objects = columnValues.get(SQLConstant.RESERVED_DELTA_OBJECT);
            for (String delta_object : delta_objects) {
                if (actualDeltaObjects.contains(delta_object))
                    validDeltaObjects.add(delta_object);
            }
            return;
        }

        if (beginIndex == columns.length) {
            for (String deltaObject : actualDeltaObjects) {
                boolean valid = true;
                //if deltaObject is root.column1_value.column2_value then
                //actualValues is [root, column1_value, column2_value]
                String[] actualValues = deltaObject.split(SQLConstant.REGEX_PATH_SEPARATOR);
                for (int i = 0; i < columns.length; i++) {
                    int columnIndex = columnNames.indexOf(columns[i].toString());
                    if (!actualValues[columnIndex].equals(values[i])) {
                        valid = false;
                    }
                }
                if (valid)
                    validDeltaObjects.add(deltaObject);
            }
            return;
        }

        for (String c : columnValues.get(columns[beginIndex].toString())) {
            values[beginIndex] = c;
            combination(actualDeltaObjects, columnValues, columns, beginIndex + 1, values);
        }
    }

    private Map<String, Set<String>> mergeColumns(List<FilterOperator> columnFilterOperators) {
        Map<String, Set<String>> column_values_map = new HashMap<>();
        for (FilterOperator filterOperator : columnFilterOperators) {
            Pair<String, Set<String>> column_values = mergeColumn(filterOperator);
            if (column_values != null && !column_values.right.isEmpty())
                column_values_map.put(column_values.left, column_values.right);
        }
        return column_values_map;
    }

    /**
     * merge one column filterOperator
     *
     * @param columnFilterOperator column filter
     * @return selected values of the column filter
     */
    private Pair<String, Set<String>> mergeColumn(FilterOperator columnFilterOperator) {
        if (columnFilterOperator == null) {
            return null;
        }
        if (columnFilterOperator.isLeaf()) {
            // special case : not equal
            if (columnFilterOperator.getTokenIntType() == SQLConstant.NOTEQUAL) {
                return null;
            }
            //
            Set<String> ret = new HashSet<>();
            ret.add(((BasicOperator) columnFilterOperator).getSeriesValue());
            return new Pair<>(columnFilterOperator.getSinglePath(), ret);
        }
        List<FilterOperator> children = columnFilterOperator.getChildren();
        if (children == null || children.isEmpty()) {
            return new Pair<>(null, new HashSet<>());
        }
        Pair<String, Set<String>> ret = mergeColumn(children.get(0));
        if (ret == null) {
            return null;
        }
        for (int i = 1; i < children.size(); i++) {
            Pair<String, Set<String>> temp = mergeColumn(children.get(i));
            if (temp == null) {
                return null;
            }
            switch (columnFilterOperator.getTokenIntType()) {
                case SQLConstant.KW_AND:
                    ret.right.retainAll(temp.right);
                    //example: "where device = d1 and device = d2" should not query data
                    if (ret.right.isEmpty()) {
                        flag = false;
                    }
                    break;
                case SQLConstant.KW_OR:
                    ret.right.addAll(temp.right);
                    break;
                default:
                    throw new UnsupportedOperationException("given error token type:" + columnFilterOperator.getTokenIntType());
            }
        }
        return ret;
    }
}
