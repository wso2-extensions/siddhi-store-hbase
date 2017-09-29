/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.extension.siddhi.store.hbase;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.hbase.exception.HBaseTableException;
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.COLUMN_FAMILY;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_CLIENT_PORT;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_QUORUM;

public class UpdateHBaseTableTestCase {
    private static final Logger log = Logger.getLogger(UpdateHBaseTableTestCase.class);
    private int inEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        eventArrived = false;
        HBaseTableTestUtils.initializeTable(TABLE_NAME, COLUMN_FAMILY);
    }

    @BeforeClass
    public static void startTest() {
        log.info("== HBase Table UPDATE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== HBase Table UPDATE tests completed ==");
    }

    @Test
    public void updateFromTableTest1() throws InterruptedException {
        //Check for update event data in HBase table when a primary key condition is true.
        log.info("updateFromTableTest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price string, volume long); " +
                "define stream UpdateStockStream (symbol string, price string, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price string, volume long); ";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2') " +
                "from UpdateStockStream\n" +
                "select symbol, price, volume\n" +
                "update StockTable\n" +
                "on (StockTable.symbol == symbol);";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "55.6", 100L});
        stockStream.send(new Object[]{"IBM", "75.6", 100L});
        stockStream.send(new Object[]{"WSO2", "57.6", 100L});
        updateStockStream.send(new Object[]{"IBM", "57.6", 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 2, "Update failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest1")
    public void updateFromTableTest2() throws InterruptedException {
        //Check for update event data in HBase table when multiple key conditions are true.
        log.info("updateFromTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price double, volume long); " +
                "define stream UpdateStockStream (symbol string, price double, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol, volume\")" +
                "define table StockTable (symbol string, price double, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update StockTable " +
                "   on (StockTable.symbol == symbol and StockTable.volume == volume) ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6, 50L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 200L});
        updateStockStream.send(new Object[]{"WSO2", 57.6, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 3, "Update failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest2")
    public void updateFromTableTest3() throws InterruptedException {
        log.info("updateFromTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 2, "Update operation failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest3")
    public void updateFromTableTest4() throws InterruptedException {
        log.info("updateFromTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable; " +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"MSFT", 22.9f, 200L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 3, "Update operation failed");
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "updateFromTableTest4", expectedExceptions = HBaseTableException.class)
    // Test updating a non-existent record.
    public void updateFromTableTest5() throws InterruptedException {
        log.info("updateFromTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"BSD", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 3, "Update operation failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest5")
    public void updateFromTableTest6() throws InterruptedException {
        log.info("updateFromTableTest6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream[(StockTable.symbol==symbol) in StockTable] " +
                "insert into OutStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                    }
                    eventArrived = true;
                }

            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 55.6f, 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        checkStockStream.send(new Object[]{"WSO2", 100L});
        checkStockStream.send(new Object[]{"IBM", 100L});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest6", expectedExceptions = OperationNotSupportedException.class)
    public void updateFromTableTest8() throws InterruptedException {
        // Check update operations with keys other than the defined primary keys.
        log.info("updateFromTableTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream UpdateStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "update StockTable " +
                "   on StockTable.volume == volume ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        updateStockStream.send(new Object[]{"IBM", 57.6f, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "updateFromTableTest8", expectedExceptions = OperationNotSupportedException.class)
    public void updateFromTableTest9() throws InterruptedException {
        // Check update operations with not all primary keys in EQUALS form.
        log.info("updateFromTableTest9");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price double, volume long); " +
                "define stream UpdateStockStream (symbol string, price double, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol, volume\")" +
                "define table StockTable (symbol string, price double, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select symbol, price, volume " +
                "update StockTable " +
                "   on (StockTable.symbol == symbol and StockTable.volume > volume) ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6, 50L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 200L});
        updateStockStream.send(new Object[]{"WSO2", 85.6, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = OperationNotSupportedException.class)
    public void updateFromTableTest10() throws InterruptedException {
        // Check update operations with primary keys not in EQUALS form.
        log.info("updateFromTableTest10");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price double, volume long); " +
                "define stream UpdateStockStream (symbol string, price double, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"volume\")" +
                "define table StockTable (symbol string, price double, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from UpdateStockStream " +
                "select symbol, price, volume " +
                "update StockTable " +
                "   on (StockTable.volume > volume) ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler updateStockStream = siddhiAppRuntime.getInputHandler("UpdateStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6, 50L});
        stockStream.send(new Object[]{"IBM", 75.6, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6, 200L});
        updateStockStream.send(new Object[]{"WSO2", 85.6, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }
}
