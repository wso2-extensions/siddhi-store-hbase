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
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.COLUMN_FAMILY;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_CLIENT_PORT;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_QUORUM;

public class InsertIntoHBaseTableTestCase {


    private static final Logger log = Logger.getLogger(InsertIntoHBaseTableTestCase.class);

    @BeforeClass
    public static void startTest() {
        log.info("== HBase Table INSERTION tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== HBase Table INSERTION tests completed ==");
    }

    @BeforeMethod
    public void init() {
        HBaseTableTestUtils.initializeTable(TABLE_NAME, COLUMN_FAMILY);
    }

    @Test(testName = "hbasetableinsertiontest1", description = "Testing table creation.")
    public void hbasetableinsertiontest1() throws InterruptedException {
        //Configure siddhi to insert events data to the HBase table only from specific fields of the stream.
        log.info("hbasetableinsertiontest1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "define table StockTable (symbol string, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream\n" +
                "select symbol, volume\n" +
                "insert into StockTable ;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 3, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

    @Test(testName = "hbasetableinsertiontest1", description = "Testing table creation.")
    public void hbasetableinsertiontest2() throws InterruptedException {
        //Testing table creation with a compound primary key field
        log.info("hbasetableinsertiontest2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"symbol, price\")" +
                "define table StockTable (symbol string, price float, volume long); ";

        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream   " +
                "insert into StockTable ;";

        log.info(streams + query);

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6F, 100L});
        stockStream.send(new Object[]{"IBM", 75.6F, 100L});
        stockStream.send(new Object[]{"MSFT", 57.6F, 100L});
        stockStream.send(new Object[]{"WSO2", 58.6F, 100L});
        Thread.sleep(1000);

        long totalRowsInTable = HBaseTableTestUtils.getRowsInTable(TABLE_NAME, COLUMN_FAMILY);
        Assert.assertEquals(totalRowsInTable, 4, "Definition/Insertion failed");
        siddhiAppRuntime.shutdown();
    }

}
