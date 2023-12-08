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

public class Q9 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               nation,
               o_year,
               SUM(amount) AS sum_profit
            FROM
               (
                  SELECT
                     n_name AS nation,
                     EXTRACT(YEAR
                  FROM
                     o_orderdate) AS o_year,
                     l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
                  FROM
                     part,
                     supplier,
                     lineitem,
                     partsupp,
                     orders,
                     nation
                  WHERE
                     s_suppkey = l_suppkey
                     AND ps_suppkey = l_suppkey
                     AND ps_partkey = l_partkey
                     AND p_partkey = l_partkey
                     AND o_orderkey = l_orderkey
                     AND s_nationkey = n_nationkey
                     AND p_name LIKE ?
               )
               AS profit
            GROUP BY
               nation,
               o_year
            ORDER BY
               nation,
               o_year DESC
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q9 = """

            select
            	nation,
            	o_year,
            	sum(amount) as sum_profit
            from
            	(
            		select
            			n_name as nation,
            			extract(year from o_orderdate) as o_year,
            			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
            		from
            			part,
            			supplier,
            			lineitem,
            			partsupp,
            			orders,
            			nation
            		where
            			s_suppkey = l_suppkey
            			and ps_suppkey = l_suppkey
            			and ps_partkey = l_partkey
            			and p_partkey = l_partkey
            			and o_orderkey = l_orderkey
            			and s_nationkey = n_nationkey
            			and p_name like '%ghost%'
            	) as profit
            group by
            	nation,
            	o_year
            order by
            	nation,
            	o_year desc
            limit 1;

            """;
        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q9));
        return stmt;
    }
}
