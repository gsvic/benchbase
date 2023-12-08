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
import com.oltpbenchmark.benchmarks.tpch.TPCHUtil;
import com.oltpbenchmark.util.RandomGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Q19 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               SUM(l_extendedprice* (1 - l_discount)) AS revenue
            FROM
               lineitem,
               part
            WHERE
               (
                  p_partkey = l_partkey
                  AND p_brand = ?
                  AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                  AND l_quantity >= ?
                  AND l_quantity <= ? + 10
                  AND p_size BETWEEN 1 AND 5
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON'
               )
               OR
               (
                  p_partkey = l_partkey
                  AND p_brand = ?
                  AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                  AND l_quantity >= ?
                  AND l_quantity <= ? + 10
                  AND p_size BETWEEN 1 AND 10
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON'
               )
               OR
               (
                  p_partkey = l_partkey
                  AND p_brand = ?
                  AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                  AND l_quantity >= ?
                  AND l_quantity <= ? + 10
                  AND p_size BETWEEN 1 AND 15
                  AND l_shipmode IN ('AIR', 'AIR REG')
                  AND l_shipinstruct = 'DELIVER IN PERSON'
               )
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q19 = """

            select
            	c_name,
            	c_custkey,
            	o_orderkey,
            	o_orderdate,
            	o_totalprice,
            	sum(l_quantity)
            from
            	customer,
            	orders,
            	lineitem
            where
            	o_orderkey in (
            		select
            			l_orderkey
            		from
            			lineitem
            		group by
            			l_orderkey having
            				sum(l_quantity) > 312
            	)
            	and c_custkey = o_custkey
            	and o_orderkey = l_orderkey
            group by
            	c_name,
            	c_custkey,
            	o_orderkey,
            	o_orderdate,
            	o_totalprice
            order by
            	o_totalprice desc,
            	o_orderdate
            limit 1;
            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q19));

        return stmt;
    }
}
