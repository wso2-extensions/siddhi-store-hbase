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

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.util.EventPrinter;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.COLUMN_FAMILY;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.TABLE_NAME;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_CLIENT_PORT;
import static org.wso2.extension.siddhi.store.hbase.util.HBaseTableTestUtils.ZK_QUORUM;

public class JoinHBaseTableTestCase {
    private static final Logger log = Logger.getLogger(JoinHBaseTableTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private boolean eventArrived;

    @BeforeMethod
    public void init() {
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
        HBaseTableTestUtils.initializeTable(HBaseTableTestUtils.TABLE_NAME, COLUMN_FAMILY);
    }

    @BeforeClass
    public static void startTest() {
        log.info("== HBase Table JOIN tests started ==");
    }

    @AfterClass
    public static void shutdown() {
        log.info("== HBase Table JOIN tests completed ==");
    }

    @Test
    public void testTableJoinQuery1() throws InterruptedException {
        log.info("testTableJoinQuery1 - OUT 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        Set<List<Object>> actualEvents = new HashSet<>();
        actualEvents.add(Arrays.asList(new Object[]{"WSO2", "WSO2", 100L}));
        actualEvents.add(Arrays.asList(new Object[]{"WSO2", "IBM", 10L}));
        Set<List<Object>> receivedEvents = new HashSet<>();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        receivedEvents.add(Arrays.asList(event.getData()));
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(actualEvents, receivedEvents);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery2() throws InterruptedException {
        log.info("testTableJoinQuery2 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                " on CheckStockStream.symbol==StockTable.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(true, eventArrived, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery3() throws InterruptedException {
        log.info("testTableJoinQuery3 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol1 string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                " on symbol1== symbol " +
                "select symbol1 as checkSymbol, symbol as symbol, volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery4() throws InterruptedException {
        log.info("testTableJoinQuery4 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) as c join StockTable as s " +
                " on c.symbol==s.symbol " +
                "select c.symbol as checkSymbol, s.symbol as symbol, s.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery5() throws InterruptedException {
        log.info("testTableJoinQuery5 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable as s " +
                " on CheckStockStream.symbol==s.symbol " +
                "select CheckStockStream.symbol as checkSymbol, s.symbol as symbol, s.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery6() throws InterruptedException {
        log.info("testTableJoinQuery6 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable as s " +
                " on CheckStockStream.symbol==s.symbol " +
                "select CheckStockStream.symbol as checkSymbol, s.symbol as symbol, s.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery7() throws InterruptedException {
        log.info("testTableJoinQuery7 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                " on StockTable.symbol==CheckStockStream.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2", 100L});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTableJoinQuery8() throws InterruptedException {
        log.info("testTableJoinQuery8 - OUT 1");
        SiddhiManager siddhiManager = new SiddhiManager();
        String streams = "" +
                "define stream StockStream (symbol string, price float); " +
                "define stream CheckStockStream (symbol string); " +
                "@Store(type=\"hbase\", table.name=\"" + TABLE_NAME + "\", column.family=\"" + COLUMN_FAMILY + "\", " +
                "hbase.zookeeper.quorum=\"" + ZK_QUORUM + "\", hbase.zookeeper.property.clientPort=\""
                + ZK_CLIENT_PORT + "\")" +
                "define table StockTable (symbol string, price float); ";
        String query = "" +
                "@info(name = 'query1') " +
                "from StockStream " +
                "insert into StockTable ;" +
                "" +
                "@info(name = 'query2') " +
                "from CheckStockStream#window.length(1) join StockTable " +
                " on StockTable.symbol==CheckStockStream.symbol " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        switch (inEventCount) {
                            case 1:
                                Assert.assertEquals(event.getData(), new Object[]{"WSO2", "WSO2"});
                                break;
                            default:
                                Assert.assertSame(inEventCount, 1);
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

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f});
        stockStream.send(new Object[]{"IBM", 75.6f});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 1, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTableJoinQuery9() throws InterruptedException {
        log.info("testTableJoinQuery9 - OUT 2");
        SiddhiManager siddhiManager = new SiddhiManager();
        Set<List<Object>> actualEvents = new HashSet<>();
        actualEvents.add(Arrays.asList(new Object[]{"WSO2", "WSO2", 100L}));
        actualEvents.add(Arrays.asList(new Object[]{"WSO2", "IBM", 10L}));
        Set<List<Object>> receivedEvents = new HashSet<>();

        String streams = "" +
                "define stream StockStream (symbol string, price float, volume long); " +
                "define stream CheckStockStream (symbol string); " +
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
                "from CheckStockStream#window.length(1) join StockTable " +
                "select CheckStockStream.symbol as checkSymbol, StockTable.symbol as symbol, " +
                "StockTable.volume as volume  " +
                "insert into OutputStream ;";

        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        siddhiAppRuntime.addCallback("query2", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                if (inEvents != null) {
                    for (Event event : inEvents) {
                        inEventCount++;
                        receivedEvents.add(Arrays.asList(event.getData()));
                    }
                    eventArrived = true;
                }
                if (removeEvents != null) {
                    removeEventCount = removeEventCount + removeEvents.length;
                }
                eventArrived = true;
            }

        });

        InputHandler stockStream = siddhiAppRuntime.getInputHandler("StockStream");
        InputHandler checkStockStream = siddhiAppRuntime.getInputHandler("CheckStockStream");
        siddhiAppRuntime.start();

        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 10L});
        checkStockStream.send(new Object[]{"WSO2"});
        Thread.sleep(1000);

        Assert.assertEquals(inEventCount, 2, "Number of success events");
        Assert.assertEquals(removeEventCount, 0, "Number of remove events");
        Assert.assertEquals(eventArrived, true, "Event arrived");
        Assert.assertEquals(actualEvents, receivedEvents);
        siddhiAppRuntime.shutdown();
    }

}
