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

public class Q8 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               o_year,
               SUM(
               CASE
                  WHEN
                     nation = ?
                  THEN
                     volume
                  ELSE
                     0
               END
            ) / SUM(volume) AS mkt_share
            FROM
               (
                  SELECT
                     EXTRACT(YEAR
                  FROM
                     o_orderdate) AS o_year,
                     l_extendedprice * (1 - l_discount) AS volume,
                     n2.n_name AS nation
                  FROM
                     part,
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2,
                     region
                  WHERE
                     p_partkey = l_partkey
                     AND s_suppkey = l_suppkey
                     AND l_orderkey = o_orderkey
                     AND o_custkey = c_custkey
                     AND c_nationkey = n1.n_nationkey
                     AND n1.n_regionkey = r_regionkey
                     AND r_name = ?
                     AND s_nationkey = n2.n_nationkey
                     AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
                     AND p_type = ?
               )
               AS all_nations
            GROUP BY
               o_year
            ORDER BY
               o_year
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q8 = """

            select
            	o_year,
            	sum(case
            		when nation = 'MOROCCO' then volume
            		else 0
            	end) / sum(volume) as mkt_share
            from
            	(
            		select
            			extract(year from o_orderdate) as o_year,
            			l_extendedprice * (1 - l_discount) as volume,
            			n2.n_name as nation
            		from
            			part,
            			supplier,
            			lineitem,
            			orders,
            			customer,
            			nation n1,
            			nation n2,
            			region
            		where
            			p_partkey = l_partkey
            			and s_suppkey = l_suppkey
            			and l_orderkey = o_orderkey
            			and o_custkey = c_custkey
            			and c_nationkey = n1.n_nationkey
            			and n1.n_regionkey = r_regionkey
            			and r_name = 'AFRICA'
            			and s_nationkey = n2.n_nationkey
            			and o_orderdate between date '1995-01-01' and date '1996-12-31'
            			and p_type = 'MEDIUM ANODIZED NICKEL'
            	) as all_nations
            group by
            	o_year
            order by
            	o_year
            limit 1;

            """;
        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q8));

        return stmt;
    }
}
