package org.apache.iotdb.rpc;


public class Config {
    public enum Constant {
        NUMBER("number"), BOOLEAN("bool");

        Constant(String type) {
            this.type = type;
        }
        private String type;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    private Config(){}

    public static Constant boolFormat = Constant.BOOLEAN;
    public static boolean rpcThriftCompressionEnable = false;
    public static int connectionTimeoutInMs = 0;
    public static final int RETRY_NUM = 3;
    public static final long RETRY_INTERVAL = 1000;
    public static int fetchSize = 10000;

    public static void setBoolFormat(Constant boolFormat) {
        Config.boolFormat = boolFormat;
    }

}
