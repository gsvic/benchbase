package com.oltpbenchmark.benchmarks.job;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;

import java.sql.SQLException;
import java.util.List;

public class JOBLoader extends Loader<JoinOrderBenchmark> {
    public JOBLoader(JoinOrderBenchmark benchmark) {
        super(benchmark);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() throws SQLException {
        return null;
    }
}
