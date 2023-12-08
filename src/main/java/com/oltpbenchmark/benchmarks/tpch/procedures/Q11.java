/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpch.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpch.TPCHConstants;
import com.oltpbenchmark.benchmarks.tpch.TPCHUtil;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q11 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               ps_partkey,
               SUM(ps_supplycost * ps_availqty) AS VALUE
            FROM
               partsupp,
               supplier,
               nation
            WHERE
               ps_suppkey = s_suppkey
               AND s_nationkey = n_nationkey
               AND n_name = ?
            GROUP BY
               ps_partkey
            HAVING
               SUM(ps_supplycost * ps_availqty) > (
               SELECT
                  SUM(ps_supplycost * ps_availqty) * ?
               FROM
                  partsupp, supplier, nation
               WHERE
                  ps_suppkey = s_suppkey
                  AND s_nationkey = n_nationkey
                  AND n_name = ? )
               ORDER BY
                  VALUE DESC
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q11 = """

            select
            	ps_partkey,
            	sum(ps_supplycost * ps_availqty) as value
            from
            	partsupp,
            	supplier,
            	nation
            where
            	ps_suppkey = s_suppkey
            	and s_nationkey = n_nationkey
            	and n_name = 'MOZAMBIQUE'
            group by
            	ps_partkey having
            		sum(ps_supplycost * ps_availqty) > (
            			select
            				sum(ps_supplycost * ps_availqty) * 0.0001000000
            			from
            				partsupp,
            				supplier,
            				nation
            			where
            				ps_suppkey = s_suppkey
            				and s_nationkey = n_nationkey
            				and n_name = 'MOZAMBIQUE'
            		)
            order by
            	value desc
            limit 1;
            """;
        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q11));

        return stmt;
    }
}
