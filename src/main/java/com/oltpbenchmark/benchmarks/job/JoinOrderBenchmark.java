package com.oltpbenchmark.benchmarks.job;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tpch.TPCHWorker;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaWorker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JoinOrderBenchmark extends BenchmarkModule {
    /**
     * Constructor!
     *
     * @param workConf
     */
    public JoinOrderBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

        int numTerminals = workConf.getTerminals();
        //LOG.info(String.format("Creating %d workers for TPC-H", numTerminals));
        for (int i = 0; i < numTerminals; i++) {
            workers.add(new JOBWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<? extends BenchmarkModule> makeLoaderImpl() {
        return null;
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return null;
    }
}
