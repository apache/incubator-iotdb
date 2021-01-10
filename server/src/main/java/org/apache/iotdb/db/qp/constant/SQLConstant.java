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
package org.apache.iotdb.db.qp.constant;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.sql.SqlBaseLexer;

/**
 * this class contains several constants used in SQL.
 */
@SuppressWarnings("unused") // some fields are for future features
public class SQLConstant {

  private SQLConstant() {
    // forbidding instantiation
  }

  private static final String[] SINGLE_ROOT_ARRAY = new String[1];
  private static final String[] SINGLE_TIME_ARRAY = new String[1];
  public static final PartialPath TIME_PATH = new PartialPath(SINGLE_TIME_ARRAY);
  public static final String ALIGNBY_DEVICE_COLUMN_NAME = "Device";
  public static final String RESERVED_TIME = "time";
  public static final String IS_AGGREGATION = "IS_AGGREGATION";
  public static final String NOW_FUNC = "now()";
  public static final String START_TIME_STR = "1970-1-01T00:00:00";

  public static final String LINE_FEED_SIGNAL = "\n";
  public static final String ROOT = "root";
  public static final String METADATA_PARAM_EQUAL = "=";
  public static final String QUOTE = "'";
  public static final String DQUOTE = "\"";
  public static final String BOOLEAN_TRUE = "true";
  public static final String BOOLEAN_FALSE = "false";
  public static final String BOOLEAN_TRUE_NUM = "1";
  public static final String BOOLEAN_FALSE_NUM = "0";

  // names of aggregations
  public static final String MIN_TIME = "min_time";
  public static final String MAX_TIME = "max_time";

  public static final String MAX_VALUE = "max_value";
  public static final String MIN_VALUE = "min_value";

  public static final String FIRST_VALUE = "first_value";
  public static final String LAST_VALUE = "last_value";

  public static final String LAST = "last";

  public static final String COUNT = "count";
  public static final String AVG = "avg";
  public static final String SUM = "sum";

  public static final String ALL = "all";

  private static final Set<String> NATIVE_FUNCTION_NAMES = new HashSet<>(Arrays.asList(
      MIN_TIME, MAX_TIME, MIN_VALUE, MAX_VALUE, FIRST_VALUE, LAST_VALUE, COUNT, SUM, AVG));

  public static final int KW_AND = 1;
  public static final int KW_OR = 2;
  public static final int KW_NOT = 3;

  public static final int EQUAL = SqlBaseLexer.OPERATOR_EQ;
  public static final int NOTEQUAL = SqlBaseLexer.OPERATOR_NEQ;
  public static final int LESSTHANOREQUALTO = SqlBaseLexer.OPERATOR_LTE;
  public static final int LESSTHAN = SqlBaseLexer.OPERATOR_LT;
  public static final int GREATERTHANOREQUALTO = SqlBaseLexer.OPERATOR_GTE;
  public static final int GREATERTHAN = SqlBaseLexer.OPERATOR_GT;
  public static final int IN = SqlBaseLexer.OPERATOR_IN;

  public static final int TOK_SELECT = 21;
  public static final int TOK_FROM = 22;
  public static final int TOK_WHERE = 23;
  public static final int TOK_INSERT = 24;
  public static final int TOK_DELETE = 25;
  public static final int TOK_UPDATE = 26;
  public static final int TOK_QUERY = 27;

  public static final int TOK_CREATE_INDEX = 31;
  public static final int TOK_DROP_INDEX = 32;
  public static final int TOK_QUERY_INDEX = 33;

  public static final int TOK_GRANT_WATERMARK_EMBEDDING = 34;
  public static final int TOK_REVOKE_WATERMARK_EMBEDDING = 35;

  public static final int TOK_AUTHOR_CREATE = 41;
  public static final int TOK_AUTHOR_DROP = 42;
  public static final int TOK_AUTHOR_GRANT = 43;
  public static final int TOK_AUTHOR_REVOKE = 44;
  public static final int TOK_AUTHOR_UPDATE_USER = 46;

  public static final int TOK_DATALOAD = 45;

  public static final int TOK_METADATA_CREATE = 51;
  public static final int TOK_METADATA_DELETE = 52;
  public static final int TOK_METADATA_SET_FILE_LEVEL = 53;
  public static final int TOK_PROPERTY_CREATE = 54;
  public static final int TOK_PROPERTY_ADD_LABEL = 55;
  public static final int TOK_PROPERTY_DELETE_LABEL = 56;
  public static final int TOK_PROPERTY_LINK = 57;
  public static final int TOK_PROPERTY_UNLINK = 58;
  public static final int TOK_LIST = 59;


  public static final int TOK_DURATION = 60;
  public static final int TOK_DATE_EXPR = 61;
  public static final int TOK_METADATA_DELETE_FILE_LEVEL = 62;

  public static final int TOK_SET = 63;
  public static final int TOK_UNSET = 64;
  public static final int TOK_SHOW = 65;
  public static final int TOK_LOAD_CONFIGURATION = 66;

  public static final int TOK_FLUSH_TASK_INFO = 67;
  public static final int TOK_LOAD_FILES = 69;
  public static final int TOK_REMOVE_FILE = 70;
  public static final int TOK_MOVE_FILE = 71;
  public static final int TOK_VERSION = 72;
  public static final int TOK_TIMESERIES = 73;
  public static final int TOK_STORAGE_GROUP = 74;
  public static final int TOK_CHILD_PATHS = 75;
  public static final int TOK_DEVICES = 76;
  public static final int TOK_COUNT_TIMESERIES = 77;
  public static final int TOK_COUNT_NODE_TIMESERIES = 78;
  public static final int TOK_COUNT_NODES = 79;

  public static final int TOK_METADATA_ALTER = 80;

  public static final int TOK_FLUSH = 81;
  public static final int TOK_MERGE = 82;
  public static final int TOK_FULL_MERGE = 83;

  public static final int TOK_CLEAR_CACHE = 84;

  public static final int TOK_LOAD_CONFIGURATION_GLOBAL = 85;
  public static final int TOK_LOAD_CONFIGURATION_LOCAL = 86;

  public static final int TOK_SHOW_MERGE_STATUS = 87;
  public static final int TOK_DELETE_PARTITION = 88;

  public static final int TOK_CREATE_SCHEMA_SNAPSHOT = 89;
  public static final int TOK_TRACING = 91;

  public static final int TOK_FUNCTION_CREATE = 92;
  public static final int TOK_FUNCTION_DROP = 93;
  public static final int TOK_SHOW_FUNCTIONS = 94;

  public static final int TOK_COUNT_DEVICES = 95;
  public static final int TOK_COUNT_STORAGE_GROUP = 96;
  public static final int TOK_COUNT_CLIENT = 97;
  public static final int TOK_CONFIG = 98;

  public static final Map<Integer, String> tokenSymbol = new HashMap<>();
  public static final Map<Integer, String> tokenNames = new HashMap<>();
  public static final Map<Integer, Integer> reverseWords = new HashMap<>();

  public static String[] getSingleRootArray() {
    return SINGLE_ROOT_ARRAY;
  }

  public static String[] getSingleTimeArray() {
    return SINGLE_TIME_ARRAY;
  }

  static {
    SINGLE_ROOT_ARRAY[0] = ROOT;
    SINGLE_TIME_ARRAY[0] = RESERVED_TIME;
    tokenSymbol.put(KW_AND, "&");
    tokenSymbol.put(KW_OR, "|");
    tokenSymbol.put(KW_NOT, "!");
    tokenSymbol.put(EQUAL, "=");
    tokenSymbol.put(NOTEQUAL, "<>");
    tokenSymbol.put(LESSTHANOREQUALTO, "<=");
    tokenSymbol.put(LESSTHAN, "<");
    tokenSymbol.put(GREATERTHANOREQUALTO, ">=");
    tokenSymbol.put(GREATERTHAN, ">");
  }

  static {
    tokenNames.put(KW_AND, "and");
    tokenNames.put(KW_OR, "or");
    tokenNames.put(KW_NOT, "not");
    tokenNames.put(EQUAL, "equal");
    tokenNames.put(NOTEQUAL, "not_equal");
    tokenNames.put(LESSTHANOREQUALTO, "lessthan_or_equalto");
    tokenNames.put(LESSTHAN, "lessthan");
    tokenNames.put(GREATERTHANOREQUALTO, "greaterthan_or_equalto");
    tokenNames.put(GREATERTHAN, "greaterthan");
    tokenNames.put(IN, "in");

    tokenNames.put(TOK_SELECT, "TOK_SELECT");
    tokenNames.put(TOK_FROM, "TOK_FROM");
    tokenNames.put(TOK_WHERE, "TOK_WHERE");
    tokenNames.put(TOK_INSERT, "TOK_INSERT");
    tokenNames.put(TOK_DELETE, "TOK_DELETE");
    tokenNames.put(TOK_UPDATE, "TOK_UPDATE");
    tokenNames.put(TOK_QUERY, "TOK_QUERY");

    tokenNames.put(TOK_AUTHOR_CREATE, "TOK_AUTHOR_CREATE");
    tokenNames.put(TOK_AUTHOR_DROP, "TOK_AUTHOR_DROP");
    tokenNames.put(TOK_AUTHOR_GRANT, "TOK_AUTHOR_GRANT");
    tokenNames.put(TOK_AUTHOR_REVOKE, "TOK_AUTHOR_REVOKE");
    tokenNames.put(TOK_AUTHOR_UPDATE_USER, "TOK_AUTHOR_UPDATE_USER");
    tokenNames.put(TOK_DATALOAD, "TOK_DATALOAD");

    tokenNames.put(TOK_METADATA_CREATE, "TOK_METADATA_CREATE");
    tokenNames.put(TOK_METADATA_DELETE, "TOK_METADATA_DELETE");
    tokenNames.put(TOK_METADATA_SET_FILE_LEVEL, "TOK_METADATA_SET_FILE_LEVEL");
    tokenNames.put(TOK_METADATA_DELETE_FILE_LEVEL, "TOK_METADATA_DELETE_FILE_LEVEL");
    tokenNames.put(TOK_PROPERTY_CREATE, "TOK_PROPERTY_CREATE");
    tokenNames.put(TOK_PROPERTY_ADD_LABEL, "TOK_PROPERTY_ADD_LABEL");
    tokenNames.put(TOK_PROPERTY_DELETE_LABEL, "TOK_PROPERTY_DELETE_LABEL");
    tokenNames.put(TOK_PROPERTY_LINK, "TOK_PROPERTY_LINK");
    tokenNames.put(TOK_PROPERTY_UNLINK, "TOK_PROPERTY_UNLINK");

    tokenNames.put(TOK_LIST, "TOK_LIST");
    tokenNames.put(TOK_SET, "TOK_SET");
    tokenNames.put(TOK_UNSET, "TOK_UNSET");
    tokenNames.put(TOK_SHOW, "TOK_SHOW");

    tokenNames.put(TOK_LOAD_CONFIGURATION, "TOK_LOAD_CONFIGURATION");
    tokenNames.put(TOK_FLUSH_TASK_INFO, "TOK_FLUSH_TASK_INFO");

    tokenNames.put(TOK_LOAD_FILES, "TOK_LOAD_FILES");
    tokenNames.put(TOK_REMOVE_FILE, "TOK_REMOVE_FILE");
    tokenNames.put(TOK_MOVE_FILE, "TOK_MOVE_FILE");

    tokenNames.put(TOK_SHOW_MERGE_STATUS, "TOK_SHOW_MERGE_STATUS");
    tokenNames.put(TOK_DELETE_PARTITION, "TOK_DELETE_PARTITION");

    tokenNames.put(TOK_TRACING, "TOK_TRACING");

    tokenNames.put(TOK_FUNCTION_CREATE, "TOK_FUNCTION_CREATE");
    tokenNames.put(TOK_FUNCTION_DROP, "TOK_FUNCTION_DROP");
    tokenNames.put(TOK_SHOW_FUNCTIONS, "TOK_SHOW_FUNCTIONS");

    tokenNames.put(TOK_CREATE_INDEX, "TOK_CREATE_INDEX");
    tokenNames.put(TOK_DROP_INDEX, "TOK_DROP_INDEX");
    tokenNames.put(TOK_QUERY_INDEX, "TOK_QUERY_INDEX");
  }

  static {
    reverseWords.put(KW_AND, KW_OR);
    reverseWords.put(KW_OR, KW_AND);
    reverseWords.put(EQUAL, NOTEQUAL);
    reverseWords.put(NOTEQUAL, EQUAL);
    reverseWords.put(LESSTHAN, GREATERTHANOREQUALTO);
    reverseWords.put(GREATERTHANOREQUALTO, LESSTHAN);
    reverseWords.put(LESSTHANOREQUALTO, GREATERTHAN);
    reverseWords.put(GREATERTHAN, LESSTHANOREQUALTO);
  }

  public static boolean isReservedPath(PartialPath pathStr) {
    return pathStr.equals(TIME_PATH);
  }

  public static Set<String> getNativeFunctionNames() {
    return NATIVE_FUNCTION_NAMES;
  }
}
