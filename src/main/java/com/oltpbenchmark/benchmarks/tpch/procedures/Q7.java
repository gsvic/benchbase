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

public class Q7 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               supp_nation,
               cust_nation,
               l_year,
               SUM(volume) AS revenue
            FROM
               (
                  SELECT
                     n1.n_name AS supp_nation,
                     n2.n_name AS cust_nation,
                     EXTRACT(YEAR
                  FROM
                     l_shipdate) AS l_year,
                     l_extendedprice * (1 - l_discount) AS volume
                  FROM
                     supplier,
                     lineitem,
                     orders,
                     customer,
                     nation n1,
                     nation n2
                  WHERE
                     s_suppkey = l_suppkey
                     AND o_orderkey = l_orderkey
                     AND c_custkey = o_custkey
                     AND s_nationkey = n1.n_nationkey
                     AND c_nationkey = n2.n_nationkey
                     AND
                     (
                        (n1.n_name = ? AND n2.n_name = ?)
                        OR
                        (n1.n_name = ? AND n2.n_name = ?)
                     )
                     AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
               )
               AS shipping
            GROUP BY
               supp_nation,
               cust_nation,
               l_year
            ORDER BY
               supp_nation,
               cust_nation,
               l_year
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q7 = """

                select
                	supp_nation,
                	cust_nation,
                	l_year,
                	sum(volume) as revenue
                from
                	(
                		select
                			n1.n_name as supp_nation,
                			n2.n_name as cust_nation,
                			extract(year from l_shipdate) as l_year,
                			l_extendedprice * (1 - l_discount) as volume
                		from
                			supplier,
                			lineitem,
                			orders,
                			customer,
                			nation n1,
                			nation n2
                		where
                			s_suppkey = l_suppkey
                			and o_orderkey = l_orderkey
                			and c_custkey = o_custkey
                			and s_nationkey = n1.n_nationkey
                			and c_nationkey = n2.n_nationkey
                			and (
                				(n1.n_name = 'UNITED STATES' and n2.n_name = 'MOROCCO')
                				or (n1.n_name = 'MOROCCO' and n2.n_name = 'UNITED STATES')
                			)
                			and l_shipdate between date '1995-01-01' and date '1996-12-31'
                	) as shipping
                group by
                	supp_nation,
                	cust_nation,
                	l_year
                order by
                	supp_nation,
                	cust_nation,
                	l_year
                limit 1;

            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q7));

        return stmt;
    }
}
