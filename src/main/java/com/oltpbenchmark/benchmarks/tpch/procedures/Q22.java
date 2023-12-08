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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class Q22 extends GenericQuery {

    public final SQLStmt query_stmt = new SQLStmt("""
            SELECT
               cntrycode,
               COUNT(*) AS numcust,
               SUM(c_acctbal) AS totacctbal
            FROM
               (
                  SELECT
                     SUBSTRING(c_phone FROM 1 FOR 2) AS cntrycode,
                     c_acctbal
                  FROM
                     customer
                  WHERE
                     SUBSTRING(c_phone FROM 1 FOR 2) IN (?, ?, ?, ?, ?, ?, ?)
                     AND c_acctbal >
                     (
                         SELECT
                            AVG(c_acctbal)
                         FROM
                            customer
                         WHERE
                            c_acctbal > 0.00
                            AND SUBSTRING(c_phone FROM 1 FOR 2) IN (?, ?, ?, ?, ?, ?, ?)
                     )
                     AND NOT EXISTS
                     (
                         SELECT
                            *
                         FROM
                            orders
                         WHERE
                            o_custkey = c_custkey
                     )
               )
               AS custsale
            GROUP BY
               cntrycode
            ORDER BY
               cntrycode
            """
    );

    @Override
    protected PreparedStatement getStatement(Connection conn, RandomGenerator rand, double scaleFactor) throws SQLException {
        String q22 = """

            select
            	cntrycode,
            	count(*) as numcust,
            	sum(c_acctbal) as totacctbal
            from
            	(
            		select
            			substring(c_phone from 1 for 2) as cntrycode,
            			c_acctbal
            		from
            			customer
            		where
            			substring(c_phone from 1 for 2) in
            				('10', '14', '11', '30', '29', '21', '12')
            			and c_acctbal > (
            				select
            					avg(c_acctbal)
            				from
            					customer
            				where
            					c_acctbal > 0.00
            					and substring(c_phone from 1 for 2) in
            						('10', '14', '11', '30', '29', '21', '12')
            			)
            			and not exists (
            				select
            					*
            				from
            					orders
            				where
            					o_custkey = c_custkey
            			)
            	) as custsale
            group by
            	cntrycode
            order by
            	cntrycode
            limit 1;
            """;

        PreparedStatement stmt = this.getPreparedStatement(conn, new SQLStmt(q22));

        return stmt;
    }
}
