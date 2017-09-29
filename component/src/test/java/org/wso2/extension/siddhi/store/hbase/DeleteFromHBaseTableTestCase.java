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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.COLUMN_FAMILY;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_CLIENT_PORT;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_QUORUM;

public class DeleteFromHBaseTableTestCase {
    private static final Log log = LogFactory.getLog(DeleteFromHBaseTableTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== HBase Table DELETE tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== HBase Table DELETE tests completed ==");
    }

    @BeforeMethod
    public void init() {
        HBaseTableTestUtils.initializeTable(TABLE_NAME, COLUMN_FAMILY);
    }

    @Test(description = "deleteFromHBaseTableTest1")
    public void deleteFromHBaseTableTest1() throws InterruptedException {
        // Testing simple deletion with primary keys.
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
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
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 0, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromHBaseTableTest1")
    public void deleteFromHBaseTableTest2() throws InterruptedException {
        // Testing simple deletion with primary keys, operands in different order.
        log.info("deleteFromHBaseTableTest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
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
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on symbol == StockTable.symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 0, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "deleteFromHBaseTableTest1")
    public void deleteFromHBaseTableTest3() throws InterruptedException {
        // Testing simple deletion with primary keys with one operand as a constant.
        log.info("deleteFromHBaseTableTest3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
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
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == 'IBM'  ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 1, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromHBaseTableTest3")
    public void deleteFromHBaseTableTest4() throws InterruptedException {
        // Testing simple deletion with primary keys with one operand as a constant, with the operand orders reversed.
        log.info("deleteFromHBaseTableTest4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
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
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on 'IBM' == StockTable.symbol  ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 1, "Deletion failed");
        siddhiAppRuntime.shutdown();
    }


    @Test(dependsOnMethods = "deleteFromHBaseTableTest4", expectedExceptions = OperationNotSupportedException.class)
    public void deleteFromHBaseTableTest5() throws InterruptedException {
        // Testing simple deletion with conditions. Expected to throw an exception since it is not supported.
        log.info("deleteFromHBaseTableTest5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
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
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol==symbol and StockTable.price > price and  StockTable.volume == volume  ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test(description = "deleteFromHBaseTableTest6")
    public void deleteFromHBaseTableTest6() throws InterruptedException {
        // Testing simple deletion with primary keys with stream variable name aliases.
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol_q string, price float, volume long); " +
                "define stream DeleteStockStream (symbol_q string, price float, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "select symbol_q as symbol, price, volume " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol_q ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 0, "Deletion failed");

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromHBaseTableTest5", expectedExceptions = OperationNotSupportedException.class)
    public void deleteFromHBaseTableTest7() throws InterruptedException {
        // Testing simple deletion without primary keys. Expected to throw an exception since it is not supported.
        log.info("deleteFromHBaseTableTest7");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "deleteFromHBaseTableTest7", expectedExceptions = OperationNotSupportedException.class)
    public void deleteFromHBaseTableTest8() throws InterruptedException {
        // Testing simple deletion without all of the primary keys. Expected to throw an exception since 
        // it is not supported.
        log.info("deleteFromHBaseTableTest8");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream DeleteStockStream (symbol string, price float, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol, price\")" +
                "define table StockTable (symbol string, price float, volume long); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from DeleteStockStream " +
                "delete StockTable " +
                "   on StockTable.symbol == symbol ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler deleteStockStream = siddhiAppRuntime.getInputHandler("DeleteStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6F, 100L});
        deleteStockStream.send(new Object[]{"IBM", 57.6F, 100L});
        Thread.sleep(1000);

        siddhiAppRuntime.shutdown();
    }
}
