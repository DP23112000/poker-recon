package com.divyanshu.pokerwalletrecon;

import org.apache.spark.SparkConf;

public class Config {

    public static final String STAGE = "stage";
    public static final String PROD = "prod";

    public static SparkConf getStageSparkConf() {
        SparkConf conf = new SparkConf();
        conf.setAppName("event-reports")
                .setIfMissing("spark.master", "local[*]")
                .set("spark.cassandra.connection.host","10.43.6.80");
        return conf;
    }

    public static SparkConf getProdSparkConf(){
        SparkConf conf = new SparkConf();
        conf.setAppName("event-reports")
                .setIfMissing("spark.master", "local[*]")
                .set("spark.cassandra.connection.host","10.0.56.93")
                .set("spark.cassandra.connection.localDC","ap-south");
        return conf;
    }

    public static String getEnv() {
        String env = System.getenv("ENV");
        return (env != null && env.equalsIgnoreCase(PROD)) ? PROD : STAGE;
    }
}