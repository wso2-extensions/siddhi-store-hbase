siddhi-store-hbase
======================================

The **siddhi-store-hbase extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that  can be used to persist events to a HBase instance of the users choice.
Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-hbase">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-hbase/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-hbase/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-hbase/api/5.0.0">5.0.0</a>.

## Prerequisites

 * A HBase server instance should be started and available for connection via the HBase Java API.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped out-of-the-box with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-hbase/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.store.hbase</groupId>
        <artifactId>siddhi-store-hbase</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-hbase/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-hbase/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-hbase/api/5.0.0/#hbase-store">hbase</a> *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#store">(Store)</a>*<br><div style="padding-left: 1em;"><p>This extension assigns data sources and connection instructions to event tables. It also implements read-write operations on connected datasources. Including one or more primary keys in the event table configuration allows any record to by identified by a unique combination of primary key values. Read-write operations can be performed for the records once they are thus identified.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-hbase/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-hbase/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
