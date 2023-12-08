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

public class Q14 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               100.00 * SUM(
               CASE
                  WHEN
                     p_type LIKE 'PROMO%'
                  THEN
                     l_extendedprice * (1 - l_discount)
                  ELSE
                     0
               END
            ) / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
            FROM
               lineitem, part
            WHERE
               l_partkey = p_partkey
               AND l_shipdate >= DATE ?
               AND l_shipdate < DATE ? + INTERVAL '1' MONTH
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q14 = """

            select
            	100.00 * sum(case
            		when p_type like 'PROMO%'
            			then l_extendedprice * (1 - l_discount)
            		else 0
            	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
            from
            	lineitem,
            	part
            where
            	l_partkey = p_partkey
            	and l_shipdate >= date '1994-07-01'
            	and l_shipdate < date '1994-07-01' + interval '1' month
            limit 1;

            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q14));

        return stmt;
    }
}
