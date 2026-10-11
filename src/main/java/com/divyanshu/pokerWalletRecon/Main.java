package com.gamezy.pokerWalletRecon;

import org.apache.spark.SparkConf;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Main implements RequestHandler<Void,Void> {

    @Override
    public Void handleRequest(Void unused, Context context) {
        SparkConf conf;

        if(Config.getEnv().equals("stage")) {
            conf = Config.getStageSparkConf();
        } else {
            conf = Config.getProdSparkConf();
        }

        new Orchestrator(conf).run();
        return null;
    }
}
