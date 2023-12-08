package com.oltpbenchmark.benchmarks.job;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.types.TransactionStatus;

import java.sql.Connection;
import java.sql.SQLException;

public class JOBWorker extends Worker<JoinOrderBenchmark> {
    public JOBWorker(JoinOrderBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);

    }
    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws Procedure.UserAbortException, SQLException {
        return null;
    }
}
