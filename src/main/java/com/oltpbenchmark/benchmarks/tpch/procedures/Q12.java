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

public class Q12 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
                l_shipmode,
                SUM(
                    CASE
                        WHEN
                            o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH'
                        THEN
                            1
                        ELSE
                            0
                    END
                ) AS high_line_count,
                SUM(
                    CASE
                        WHEN
                            o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH'
                        THEN
                            1
                        ELSE
                            0
                    END
                ) AS low_line_count
            FROM
                orders,
                lineitem
            WHERE
                o_orderkey = l_orderkey
                AND l_shipmode IN (?, ?)
                AND l_commitdate < l_receiptdate
                AND l_shipdate < l_commitdate
                AND l_receiptdate >= DATE ?
                AND l_receiptdate < DATE ? + INTERVAL '1' YEAR
            GROUP BY
                l_shipmode
            ORDER BY
                l_shipmode
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q12 = """

                select
                	l_shipmode,
                	sum(case
                		when o_orderpriority = '1-URGENT'
                			or o_orderpriority = '2-HIGH'
                			then 1
                		else 0
                	end) as high_line_count,
                	sum(case
                		when o_orderpriority <> '1-URGENT'
                			and o_orderpriority <> '2-HIGH'
                			then 1
                		else 0
                	end) as low_line_count
                from
                	orders,
                	lineitem
                where
                	o_orderkey = l_orderkey
                	and l_shipmode in ('RAIL', 'MAIL')
                	and l_commitdate < l_receiptdate
                	and l_shipdate < l_commitdate
                	and l_receiptdate >= date '1994-01-01'
                	and l_receiptdate < date '1994-01-01' + interval '1' year
                group by
                	l_shipmode
                order by
                	l_shipmode
                limit 1;

            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q12));
        return stmt;
    }
}
