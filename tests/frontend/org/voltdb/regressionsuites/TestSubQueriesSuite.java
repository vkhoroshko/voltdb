/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSubQueriesSuite extends RegressionSuite {
    public TestSubQueriesSuite(String name) {
        super(name);
    }

    private void loadData(Client client) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr = null;

        String [] tbsData =  {"R1","R2","P1","P2","P3"};

        // Empty data from table.
        for (String tb: tbsData) {
            cr = client.callProcedure("@AdHoc", "delete from " + tb);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());

            // Insert records into the table.
            String proc = tb + ".insert";
            // id, wage, dept, tm
            cr = client.callProcedure(proc, 1,  10,  1 , "2013-06-18 02:00:00.123457");
            cr = client.callProcedure(proc, 2,  20,  1 , "2013-07-18 02:00:00.123457");
            cr = client.callProcedure(proc, 3,  30,  1 , "2013-07-18 10:40:01.123457");
            cr = client.callProcedure(proc, 4,  40,  2 , "2013-08-18 02:00:00.123457");
            cr = client.callProcedure(proc, 5,  50,  2 , "2013-09-18 02:00:00.123457");
        }
    }

    private final String[] procs = {"R1.insert", "R2.insert", /* "P1.insert", "P2.insert", "P3.insert"*/};
    private final String [] tbs =  {"R1","R2"/*,"P1","P2","P3"*/};

    /**
     * Simple sub-query
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubSelects_Simple() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);
        VoltTable vt;

        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM (SELECT ID, DEPT FROM "+ tb +") T1 " +
                    "WHERE T1.ID > 4;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM (SELECT ID, DEPT FROM "+ tb +") T1 " +
                    "WHERE ID < 3 ORDER BY ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}, {2, 1}});

            // Nested
            vt = client.callProcedure("@AdHoc",
                    "select A2 FROM (SELECT A1 AS A2 FROM (SELECT ID AS A1 FROM "+ tb +") T1 WHERE T1.A1 - 2 > 0) T2 " +
                    "WHERE T2.A2 < 6 ORDER BY A2").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3}, {4}, {5}});

            vt = client.callProcedure("@AdHoc",
                    "select A2 + 10 FROM (SELECT A1 AS A2 FROM (SELECT ID AS A1 FROM "+ tb +" WHERE ID > 3) T1 ) T2 " +
                    "WHERE T2.A2 < 6 ORDER BY A2").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{14}, {15}});
        }


    }

    /**
     * SELECT FROM SELECT FROM SELECT
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubSelects_Aggregations() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);
        VoltTable vt;

        for (String tb: procs) {
            client.callProcedure(tb, 6,  10,  2 , "2013-07-18 02:00:00.123457");
            client.callProcedure(tb, 7,  40,  2 , "2013-07-18 02:00:00.123457");
        }

        // Test group by queries, order by, limit
        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc", "select * from ( SELECT dept, sum(wage) as sw, sum(wage)/count(wage) as avg_wage " +
                    "from " + tb + " GROUP BY dept) T1 ORDER BY dept DESC;").getResults()[0];
            System.out.println(vt.toString());
            validateTableOfLongs(vt, new long[][] {{2, 140, 35}, {1, 60, 20} });

            // derived aggregated table + aggregation on subselect
            vt = client.callProcedure("@AdHoc",
                    " select a4, sum(wage) " +
                    " from (select wage, sum(id)+1 as a1, sum(id+1) as a2, sum(dept+3)/count(distinct dept) as a4 " +
                    "       from " + tb +
                    "       GROUP BY wage ORDER BY wage ASC LIMIT 4) T1" +
                    " Group by a4 order by a4;").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{4, 60}, {10, 40}});

            // groupby from groupby
            vt = client.callProcedure("@AdHoc",
                    "select dept_count, count(*) from (select dept, count(*) as dept_count from R1 group by dept) T1 " +
                    "group by dept_count order by dept_count").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3, 1}, {4, 1}});

            // groupby from groupby + limit
            vt = client.callProcedure("@AdHoc",
                    "select dept_count, count(*) " +
                    "from (select dept, count(*) as dept_count " +
                    "       from (select dept, id from " + tb + " order by dept limit 6) T1 group by dept) T2 " +
                    "group by dept_count order by dept_count").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3, 2}});
        }

    }

    /**
     * Join two sub queries
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSubSelects_Joins() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);

        VoltTable vt;

        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc",
                    "select newid, id  " +
                    "FROM (SELECT id, wage FROM R1) T1, (SELECT id as newid, dept FROM "+ tb +" where dept > 1) T2 " +
                    "WHERE T1.id = T2.dept ORDER BY newid").getResults()[0];
            System.out.println(vt.toString());
            validateTableOfLongs(vt, new long[][] {{4, 2}, {5, 2}});

            vt = client.callProcedure("@AdHoc",
                    "select id, wage, dept_count " +
                    "FROM R1, (select dept, count(*) as dept_count " +
                    "          from (select dept, id " +
                    "                from "+tb+" order by dept limit 5) T1 " +
                    "          group by dept) T2 " +
                    "WHERE R1.wage / T2.dept_count > 10 ORDER BY wage,dept_count").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3, 30, 2}, {4, 40, 2}, {4, 40, 3},{5, 50, 2},{5, 50, 3}});

            if (!isHSQL()) {
                vt = client.callProcedure("@AdHoc",
                        "select id, newid  " +
                        "FROM (SELECT id, wage FROM R1) T1 " +
                        "   LEFT OUTER JOIN " +
                        "   (SELECT id as newid, dept FROM "+ tb +" where dept > 1) T2 " +
                        "   ON T1.id = T2.dept " +
                        "ORDER BY id, newid").getResults()[0];
                System.out.println(vt.toString());
                validateTableOfLongs(vt, new long[][] { {1, Long.MIN_VALUE}, {2, 4}, {2, 5},
                        {3, Long.MIN_VALUE}, {4, Long.MIN_VALUE}, {5, Long.MIN_VALUE}});
            }

            vt = client.callProcedure("@AdHoc",
                    "select T2.id " +
                    "FROM (SELECT id, wage FROM R1) T1, R1 T2 " +
                    "ORDER BY T2.id").getResults()[0];
            System.out.println(vt.toString());
            validateTableOfLongs(vt, new long[][] { {1}, {1}, {1}, {1}, {1}, {2}, {2}, {2}, {2}, {2},
                    {3}, {3}, {3}, {3}, {3}, {4}, {4}, {4}, {4}, {4}, {5}, {5}, {5}, {5}, {5}});
        }
    }

    public void testSubSelects_from_replicated() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);
        VoltTable vt;


        for (String tb: tbs) {
            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" where ID in " +
                    " (select ID from " + tb + " WHERE ID > 3);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {4,2}, {5,2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where ID in " +
                    " (select ID from " + tb + " WHERE ID > 4) " +
                    "and exists (select 1 from " + tb + " where ID * T1.DEPT  = 10);").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {5, 2}});

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where " +
                    "not exists (select 1 from " + tb + " where ID * T1.DEPT  = 10) " +
                    "and T1.ID < 3 order by ID;").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}, {2, 1} });

            vt = client.callProcedure("@AdHoc", "select ID, DEPT FROM "+ tb +" T1 where " +
                    "(ID, DEPT) IN (select DEPT, WAGE/10 from " + tb + ") ").getResults()[0];
            validateTableOfLongs(vt, new long[][] { {1, 1}});

            // Nested
            vt = client.callProcedure("@AdHoc",
                    "select ID from " + tbs[0] + " T1 where exists " +
                            "(SELECT 1 FROM " + tbs[1] + " T2 where exists " +
                            "(SELECT ID FROM "+ tbs[1] +" T3 WHERE T1.ID * T3.ID  = 9))").getResults()[0];
            validateTableOfLongs(vt, new long[][] {{3}});
        }

    }

  /**
  * SELECT FROM SELECT FROM SELECT
  * @throws NoConnectionsException
  * @throws IOException
  * @throws ProcCallException
  */
public void testSubExpressions_Aggregations() throws NoConnectionsException, IOException, ProcCallException
{
    Client client = getClient();
    loadData(client);
    VoltTable vt;

    for (String tb: procs) {
        client.callProcedure(tb, 6,  10,  2 , "2013-07-18 02:00:00.123457");
        client.callProcedure(tb, 7,  40,  2 , "2013-07-18 02:00:00.123457");
    }

    // Test group by queries, order by, limit
    for (String tb: tbs) {
        vt = client.callProcedure("@AdHoc",
                "select dept, sum(wage) as sw1 from " + tb + " where (id, dept) in " +
                        "( SELECT dept, count(dept) " +
                        "from " + tb + " GROUP BY dept ORDER BY dept DESC) GROUP BY dept;").getResults()[0];

        vt = client.callProcedure("@AdHoc",
                "select P1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.ID and T1.ID < 4 order by P1.ID;").getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][] { {1,10}, {2, 20}, {3, 30}});

        vt = client.callProcedure("@AdHoc",
                "select P1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.ID and T1.ID = 3 order by P1.ID;").getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][] { {3, 30}});

        vt = client.callProcedure("@AdHoc",
                "select P1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.ID and P1.ID = 3 order by P1.ID;").getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][] { {3, 30}});


        vt = client.callProcedure("@AdHoc",
                "select T1.ID, P1.WAGE FROM (SELECT ID, DEPT FROM R1) T1, P1 " +
                "where T1.ID = P1.WAGE / 10 order by P1.ID;").getResults()[0];
        System.err.println(vt);
        validateTableOfLongs(vt, new long[][] { {1, 10}, {2, 20}, {3, 30}, {4, 40}, {5, 50}});
    }
}


    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSubQueriesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        final String literalSchema =
                "CREATE TABLE R1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE R2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER, " +
                "DEPT INTEGER, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +

                "CREATE TABLE P1 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID) );" +
                "PARTITION TABLE P1 ON COLUMN ID;" +

                "CREATE TABLE P2 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL ASSUMEUNIQUE, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID, DEPT) );" +
                "PARTITION TABLE P2 ON COLUMN DEPT;" +

                "CREATE TABLE P3 ( " +
                "ID INTEGER DEFAULT 0 NOT NULL ASSUMEUNIQUE, " +
                "WAGE INTEGER NOT NULL, " +
                "DEPT INTEGER NOT NULL, " +
                "TM TIMESTAMP DEFAULT NULL, " +
                "PRIMARY KEY (ID, WAGE) );" +
                "PARTITION TABLE P3 ON COLUMN WAGE;"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }
        boolean success;

        config = new LocalCluster("subselect-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        config = new LocalCluster("subselect-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("subselect-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }

}
