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
import java.util.HashSet;
import java.util.Set;

public class Q16 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               p_brand,
               p_type,
               p_size,
               COUNT(DISTINCT ps_suppkey) AS supplier_cnt
            FROM
               partsupp,
               part
            WHERE
               p_partkey = ps_partkey
               AND p_brand <> ?
               AND p_type NOT LIKE ?
               AND p_size IN (?, ?, ?, ?, ?, ?, ?, ?)
               AND ps_suppkey NOT IN
               (
                  SELECT
                     s_suppkey
                  FROM
                     supplier
                  WHERE
                     s_comment LIKE '%Customer%Complaints%'
               )
            GROUP BY
               p_brand,
               p_type,
               p_size
            ORDER BY
               supplier_cnt DESC,
               p_brand,
               p_type,
               p_size
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q16 = """
            select
            	p_brand,
            	p_type,
            	p_size,
            	count(distinct ps_suppkey) as supplier_cnt
            from
            	partsupp,
            	part
            where
            	p_partkey = ps_partkey
            	and p_brand <> 'Brand#32'
            	and p_type not like 'SMALL BURNISHED%'
            	and p_size in (3, 38, 9, 4, 12, 10, 42, 40)
            	and ps_suppkey not in (
            		select
            			s_suppkey
            		from
            			supplier
            		where
            			s_comment like '%Customer%Complaints%'
            	)
            group by
            	p_brand,
            	p_type,
            	p_size
            order by
            	supplier_cnt desc,
            	p_brand,
            	p_type,
            	p_size
            limit 1;

            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q16));

        return stmt;
    }
}
