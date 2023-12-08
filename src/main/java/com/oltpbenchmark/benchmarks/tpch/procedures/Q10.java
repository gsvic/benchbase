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

public class Q10 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               c_custkey,
               c_name,
               SUM(l_extendedprice * (1 - l_discount)) AS revenue,
               c_acctbal,
               n_name,
               c_address,
               c_phone,
               c_comment
            FROM
               customer,
               orders,
               lineitem,
               nation
            WHERE
               c_custkey = o_custkey
               AND l_orderkey = o_orderkey
               AND o_orderdate >= DATE ?
               AND o_orderdate < DATE ? + INTERVAL '3' MONTH
               AND l_returnflag = 'R'
               AND c_nationkey = n_nationkey
            GROUP BY
               c_custkey,
               c_name,
               c_acctbal,
               c_phone,
               n_name,
               c_address,
               c_comment
            ORDER BY
               revenue DESC LIMIT 20
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q10 = """
            select
            	c_custkey,
            	c_name,
            	sum(l_extendedprice * (1 - l_discount)) as revenue,
            	c_acctbal,
            	n_name,
            	c_address,
            	c_phone,
            	c_comment
            from
            	customer,
            	orders,
            	lineitem,
            	nation
            where
            	c_custkey = o_custkey
            	and l_orderkey = o_orderkey
            	and o_orderdate >= date '1994-01-01'
            	and o_orderdate < date '1994-01-01' + interval '3' month
            	and l_returnflag = 'R'
            	and c_nationkey = n_nationkey
            group by
            	c_custkey,
            	c_name,
            	c_acctbal,
            	c_phone,
            	n_name,
            	c_address,
            	c_comment
            order by
            	revenue desc
            limit 10;
            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q10));
        return stmt;
    }
}
