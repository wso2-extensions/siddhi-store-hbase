# API Docs - v4.0.0-M14

## Store

### hbase *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*

<p style="word-wrap: break-word">This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected datasources.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@Store(type="hbase", any.hbase.property="<STRING>", table.name="<STRING>", column.family="<STRING>")
@PrimaryKey("PRIMARY_KEY")
@Index("INDEX")
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">any.hbase.property</td>
        <td style="vertical-align: top; word-wrap: break-word">Any property that can be specified for <code>HBase</code> connectivity in hbase-site.xml is also accepted by the <code>HBase Store</code> implementation.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The name with which the event table should be persisted in the store. If no name is specified via this parameter, the event table is persisted with the same name as the Siddhi table.</td>
        <td style="vertical-align: top">The table name defined in the Siddhi Application query.</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">column.family</td>
        <td style="vertical-align: top; word-wrap: break-word">The number of characters that the values for fields of the <code>STRING</code> type in the table definition must contain. If this is not specified, the default number of characters specific to the database type is considered.</td>
        <td style="vertical-align: top">'wso2.sp'</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
define stream StockStream (symbol string, price float, volume long); @Store(type="hbase", table.name="StockTable", column.family="StockCF", hbase.zookeeper.quorum="localhost", hbase.zookeeper.property.clientPort="2181")define table StockTable (symbol string, price float, volume long);
```
<p style="word-wrap: break-word">This definition creates an event table named <code>StockTable</code> with a column family <code>StockCF</code> on the HBase instance if it does not already exist (with 3 attributes named <code>symbol</code>, <code>price</code>, and <code>volume</code> of the <code>string</code>, <code>float</code> and <code>long</code> types respectively). The connection is made as specified by the parameters configured for the '@Store' annotation. The <code>symbol</code> attribute is considered a unique field, and the values for this attribute are the HBase row IDs.</p>

