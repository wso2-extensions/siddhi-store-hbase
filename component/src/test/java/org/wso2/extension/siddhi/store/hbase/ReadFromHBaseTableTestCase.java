/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.query.compiler.exception.SiddhiParserException;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.COLUMN_FAMILY;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_CLIENT_PORT;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_QUORUM;

public class ReadFromHBaseTableTestCase {
    private static final Logger log = Logger.getLogger(ReadFromHBaseTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeClass
    public static void startTest() {
        log.info("== HBase Table READ tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== HBase Table READ tests completed ==");
    }

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        HBaseTableTestUtils.initializeTable(TABLE_NAME, COLUMN_FAMILY);
    }

    @Test
    public void readEventHBaseTableTestCase1() throws InterruptedException {
        //Read events from a HBase table successfully
        log.info("readEventHBaseTableTestCase1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string, category string, volume long);\n" +
                "define stream StockStream (itemId string, type string, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory string, checkVolume long);\n" +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"itemId\")" +
                "define table StockTable (itemId string, type string, volume long);\n";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n" +
                "from FooStream#window.length(1) join StockTable on FooStream.name==StockTable.itemId \n" +
                "select FooStream.name as checkName, StockTable.type as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "type1", 100L});
                                break;
                            case 2:
                                Assert.assertEquals(event.getData(), new Object[]{"IBM", "type3", 10L});
                                break;
                            default:
                                Assert.assertSame(2, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "type1", 100L});
        stockStream.send(new Object[]{"CSC", "type2", 10L});
        stockStream.send(new Object[]{"IBM", "type3", 10L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});

        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class, dependsOnMethods = "readEventHBaseTableTestCase1")
    public void readEventHBaseTableTestCase2() throws InterruptedException {
        //Read events from a non existing HBase table unsuccessfully
        log.info("readEventHBaseTableTestCase2");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "define stream FooStream (name string, category string, volume long);\n";

        String query =
                "@info(name = 'query1')\n" +
                        "from FooStream#window.length(1) join StockTable on FooStream.name==StockTable.itemId \n" +
                        "select FooStream.name as checkName, StockTable.type as checkCategory, " +
                        "StockTable.volume as checkVolume\n" +
                        "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.start();
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(expectedExceptions = SiddhiParserException.class, dependsOnMethods = "readEventHBaseTableTestCase2")
    public void readEventHBaseTableTestCase3() throws InterruptedException {
        //Read events from a HBase table through a non existing stream unsuccessfully
        log.info("readEventHBaseTableTestCase3");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (itemId string, type string, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory string, checkVolume long);\n" +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"itemId\")" +
                "define table StockTable (itemId string, type string, volume long);\n";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n" +
                "from FooStream#window.length(1) join StockTable on FooStream.name==StockTable.itemId \n" +
                "select FooStream.name as checkName, StockTable.type as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", "type1", 100L});
        stockStream.send(new Object[]{"CSC", "type2", 10L});
        stockStream.send(new Object[]{"IBM", "type3", 10L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"IBM"});
        Thread.sleep(1000);
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readEventHBaseTableTestCase3")
    public void readEventHBaseTableTestCase4() throws InterruptedException {
        //Read multiple events from a HBase table successfully with windows.length.
        log.info("readEventHBaseTableTestCase4");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string, category string, volume long);\n" +
                "define stream StockStream (itemId string, type string, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory string, checkVolume long);\n" +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"itemId\")" +
                "define table StockTable (itemId string, type string, volume long);\n";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n" +
                "from FooStream#window.length(5) join StockTable on FooStream.name==StockTable.itemId \n" +
                "select FooStream.name as checkName, StockTable.type as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", "type1", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"CSC", "type2", 10L}, event.getData());
                                break;
                            case 3:
                                Assert.assertEquals(new Object[]{"IBM", "type3", 10L}, event.getData());
                                break;
                            case 4:
                                Assert.assertEquals(new Object[]{"MSFT", "type4", 10L}, event.getData());
                                break;
                            case 5:
                                Assert.assertEquals(new Object[]{"MIT", "type5", 10L}, event.getData());
                                break;
                            default:
                                Assert.assertSame(5, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "type1", 100L});
        stockStream.send(new Object[]{"CSC", "type2", 10L});
        stockStream.send(new Object[]{"IBM", "type3", 10L});
        stockStream.send(new Object[]{"MSFT", "type4", 10L});
        stockStream.send(new Object[]{"MIT", "type5", 10L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"CSC"});
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"MSFT"});
        fooStream.send(new Object[]{"MIT"});

        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 5, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test(dependsOnMethods = "readEventHBaseTableTestCase4")
    public void readEventHBaseTableTestCase5() throws InterruptedException {
        //Read multiple events from a HBase table successfully with windows.length.
        log.info("readEventHBaseTableTestCase5");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string, category string, volume long);\n" +
                "define stream StockStream (itemId string, type string, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory string, checkVolume long);\n" +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "@PrimaryKey(\"itemId\")" +
                "define table StockTable (itemId string, type string, volume long);\n";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n" +
                "from FooStream#window.time(5 sec) join StockTable on FooStream.name==StockTable.itemId \n" +
                "select FooStream.name as checkName, StockTable.type as checkCategory, " +
                "StockTable.volume as checkVolume\n" +
                "insert into OutputStream;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", "type1", 100L}, event.getData());
                                break;
                            case 2:
                                Assert.assertEquals(new Object[]{"CSC", "type2", 10L}, event.getData());
                                break;
                            case 3:
                                Assert.assertEquals(new Object[]{"IBM", "type3", 10L}, event.getData());
                                break;
                            default:
                                Assert.assertSame(3, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "type1", 100L});
        stockStream.send(new Object[]{"CSC", "type2", 10L});
        stockStream.send(new Object[]{"IBM", "type3", 10L});
        fooStream.send(new Object[]{"WSO2"});
        fooStream.send(new Object[]{"CSC"});
        Thread.sleep(1000);
        fooStream.send(new Object[]{"IBM"});
        fooStream.send(new Object[]{"MSFT"});
        Thread.sleep(2000);
        fooStream.send(new Object[]{"MIT"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 3, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @Test(dependsOnMethods = "readEventHBaseTableTestCase5")
    public void readEventHBaseTableTestCase6() throws InterruptedException {
        //Read events from a HBase table successfully with aggregate function.
        log.info("readEventHBaseTableTestCase6");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream FooStream (name string, category string, volume long);\n" +
                "define stream StockStream (name string, type string, volume long);\n" +
                "define stream OutputStream (checkName string, checkCategory string, checkVolume double);\n" +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "define table StockTable (name string, type string, volume long);\n";

        String query = "" +
                "@info(name = 'query1')\n" +
                "from StockStream\n" +
                "select *\n" +
                "insert into StockTable;\n" +
                "@info(name = 'query2')\n" +
                "from FooStream#window.length(1) join StockTable\n" +
                "select FooStream.name as checkName, StockTable.type as checkCategory, avg(StockTable.volume) " +
                "as checkVolume\n" +
                "group by StockTable.name having checkVolume>50 \n" +
                "insert into OutputStream;\n";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(new Object[]{"WSO2", "type1", 150.0}, event.getData());
                                break;
                            default:
                                Assert.assertSame(1, inEventCount);
                        }
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }
        });

        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", "type1", 100L});
        stockStream.send(new Object[]{"WSO2", "type1", 200L});
        stockStream.send(new Object[]{"IBM", "type2", 30L});
        fooStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }
}
