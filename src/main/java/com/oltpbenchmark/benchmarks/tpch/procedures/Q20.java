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
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q20 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               s_name,
               s_address
            FROM
               supplier,
               nation
            WHERE
               s_suppkey IN
               (
                  SELECT
                     ps_suppkey
                  FROM
                     partsupp
                  WHERE
                     ps_partkey IN
                     (
                        SELECT
                           p_partkey
                        FROM
                           part
                        WHERE
                           p_name LIKE ?
                     )
                     AND ps_availqty > (
                     SELECT
                        0.5 * SUM(l_quantity)
                     FROM
                        lineitem
                     WHERE
                        l_partkey = ps_partkey
                        AND l_suppkey = ps_suppkey
                        AND l_shipdate >= DATE ?
                        AND l_shipdate < DATE ? + INTERVAL '1' YEAR )
               )
               AND s_nationkey = n_nationkey
               AND n_name = ?
            ORDER BY
               s_name
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q20 = """

            select
            	s_name,
            	s_address
            from
            	supplier,
            	nation
            where
            	s_suppkey in (
            		select
            			ps_suppkey
            		from
            			partsupp
            		where
            			ps_partkey in (
            				select
            					p_partkey
            				from
            					part
            				where
            					p_name like 'red%'
            			)
            			and ps_availqty > (
            				select
            					0.5 * sum(l_quantity)
            				from
            					lineitem
            				where
            					l_partkey = ps_partkey
            					and l_suppkey = ps_suppkey
            					and l_shipdate >= date '1993-01-01'
            					and l_shipdate < date '1993-01-01' + interval '1' year
            			)
            	)
            	and s_nationkey = n_nationkey
            	and n_name = 'ALGERIA'
            order by
            	s_name
            limit 1;

            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q20));

        return stmt;
    }
}
