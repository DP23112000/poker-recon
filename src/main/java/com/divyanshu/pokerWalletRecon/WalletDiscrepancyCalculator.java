package com.divyanshu.pokerWalletRecon;

import org.apache.spark.sql.Row;
import java.util.List;

public class WalletDiscrepancyCalculator {

    public double calculateDiscrepancy(List<Row> mismatchedRecords) {

        // Using BigDecimal for strict financial precision to prevent floating-point errors during recon
        double totalDiscrepancyAmount = 0.0;

        for (Row row : mismatchedRecords) {

            String partnerId = row.getAs("partner_id");

            if (partnerId.equals("1")) {

                double gamezyWallet = row.getAs("gamezy_wallet_balance");
                double pocket52Wallet = row.getAs("pocket52_wallet_balance");

                totalDiscrepancyAmount += (gamezyWallet - pocket52Wallet);
            }
        }

        return totalDiscrepancyAmount;
    }
}