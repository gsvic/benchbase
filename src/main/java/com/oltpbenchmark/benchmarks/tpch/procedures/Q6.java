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
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q6 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               SUM(l_extendedprice * l_discount) AS revenue
            FROM
               lineitem
            WHERE
               l_shipdate >= DATE ?
               AND l_shipdate < DATE ? + INTERVAL '1' YEAR
               AND l_discount BETWEEN ? - 0.01 AND ? + 0.01
               AND l_quantity < ?
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q6 = """
        select
            sum(l_extendedprice * l_discount) as revenue
        from
            lineitem
        where
            l_shipdate >= date '1996-01-01'
            and l_shipdate < date '1996-01-01' + interval '1' year
            and l_discount between 0.09 - 0.01 and 0.09 + 0.01
            and l_quantity < 25
        limit 1;
            """;
        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q6));
        return stmt;
    }
}
