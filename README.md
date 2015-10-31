SQP - Simple Query Protocol
===========================
A simple protocol to query databases based on JSON and WebSocket. It's implemented as a Java client
library and a proxy server that can be used to easily access SQL databases in a standardized manner.
The proxy server is currently able to communicate with Transbase and PostgreSQL databases.

The project started as a [master thesis](Design_of_a_Portable_SQL_Wire_Protocol.pdf) by Stefan
Burnicki in 2015. Therefore it might not be stable or complete, yet.

Structure
---------
The project uses gradle as build system. Consequently, the project's file structure is a typical
gradle structure:

Java code can be found in `src/main/java`, while test code can be found in `src/test/java`.
Resources are in `src/main/resources` and test resources in `src/test/resources`.
The folder libs/ contains non-maven dependencies.

This project consists of several main modules, namely:
  - The core (io.sqp.core), which contains definitions for the SQP data types and messages in Java
  - The SQP proxy server (io.sqp.proxy) that is able to understand the SQP, talk to a database
    and answer the client
  - The backend module (io.sqp.backend) contains interfaces and some utilities to write a database
    backend for the server
  - The PostgreSQL SQP proxy server backend (org.postgres.sqp) which enables the proxy to talk to
    PostgreSQL databases
  - The Transbase SQP proxy server backend (io.sqp.transbase) which enables the proxy to talk to
    Transbase databases
  - The SchemaMatcher (io.sqp.schemamatcher) which is used by the proxy to match custom JSON
    schemas to existing ones
  - The client library (io.sqp.client) can be used to communicate with an SQP server, as the proxy
    server.


Involved Technologies & Dependencies
------------------------------------
To understand this project, you probably need to get an overview of some technologies involved in
this project first.

In general:
  - `git` as the version control system
  - `gradle` as the build system in use
  - `Java 8` as main programming language
  - unit testing

For unit testing with mocks:
  - `TestNG` as unit test framework
  - `Mockito` as framework for mock objects
  - `Hamcrest` for matchers used in test assertions

For the protocol/core:
  - `WebSocket` as the underlying protocol
  - `JSON` as the standard serialization/message format
  - `MsgPack` as the binary equivalent for serialization/messages
  - `Jackson` as the serialization framework for both JSON and MsgPack used in the Java
    implementation
  - `JSON Schema` for data type descriptions

For the proxy:
  - Event-loop/callback based, asynchronous (non-blocking) programming
  - `Vert.x 3.0` as the event-loop and netty based server platform
  - `JSON Schema Validator` as a validator input against a custom JSON Schema

For the client:
  - `JSR 356` specification for Java WebSocket support
  - `Tyrus` as a glassfish-based reference implementation of JSR 356
  - Java 8's `CompletableFuture`s for non-blocking, but not event-loop/callback based programming

If you run the gradle build for the first time you will notice that quite a bunch of dependency
packages are downloaded.
Some packages are dependencies of others that get downloaded, like Netty as the basis of Vert.x.
Other package are needed for tests only, like hamcrest, mockito, and TestNG.
The "JSON Schema Validator" project pulls pretty much dependencies. So it's probably a good idea
to replace this package in the future if the amount of dependencies packages need to be reduced.

Another dependency is the "tbjdbc.jar" that is not available in a maven repository and thus does
not get downloaded automatically. However, this package is needed for the Transbase SQP proxy
backend.

Building
--------
Make sure you have Java 8 installed and a working internet connection so dependencies can get
downloaded. Then just execute the gradle wrapper with the build task:

    ./gradlew build

Transbase Backend
-----------------
The Transbase backend is only built optionally. You can either install the eligible Transbase JDBC
driver to `libs/tbjdbc.jar` or define its location on the cmdline:

    ./gradlew -Ptbjdbc=/path/to/tbjdbc.jar


IDE integration
---------------
The gradle plugins "eclipse" and "idea" are used to generate project files for the Eclipse or
IntelliJ IDEA IDEs. Just run

    ./gradlew idea

or

    ./gradlew eclipse

Check out the plugin documentation for more details on this.

Configuration of the server
---------------------------
In order for the proxy to work, you need to provide a configuration. See the
[config.json](config.json) file for an example.
The proxy server itself needs to be configured in a number of ways:
  - The `port` to listen on for WebSocket connections. It's optional and the default is `8080`.
  - The `path` to match for incoming connections. It's optional  and the default is `/`.
  - The `connectionPoolSize`, i.e. the maximum number of concurrently open connections.
    It's optional and the default is `30`.
  - The `backends` array which contains backend configurations.
    Currently only the first is used and the rest is ignored. This is mandatory as there are no
    defaults.

A backend configuration needs to include two fields:
  - The `type` is the full class name of the Backend implementation. E.g.
    `io.sqp.postgresql.PostgreSQLBackend`.
  - The `config` needs to be an object whose values depend on the concrete backend.
    Common fields are server credentials of the DBMS.


Run the Server from IDE
-----------------------
You can run it directly in your IDE by creating a run configuration that uses the main class
`io.vertx.core.Starter` and passes in the arguments `-config <conffile>.json run
io.sqp.proxy.ServerVerticle`. "<conffile>.json" should be the path to the proxy's configuration
file, which is mandatory to connect to a database. For details on the configuration file, see the
section "Proxy Configuration" below.

To use a specific Transbase JDBC driver, you might define it just like stated for the building process.

Creating and using a fat jar
----------------------------
The build.gradle uses the Gradle shadowJar plugin to assemble the application and all it's
dependencies into a single "fat" jar.
To build the "fat jar", run

    ./gradlew shadowJar

To run the fat jar, run

    java -jar build/libs/sqp-1.0-fat.jar -conf config.json

(You can take that jar and run it anywhere there is a Java 8+ JDK. It contains *all* the
dependencies it needs so you don't need to install Vert.x or any other libraries on the target
machine).

Scaling the server
------------------
The server is implemented as a Vert.x verticle, which allows easy scaling of the server.
E.g. let's say you have 8 cores on your server and you want to utilise them all, you can deploy 8
instances as follows:

    java -jar build/libs/sqp-1.0-fat.jar -conf config.json -instances 8

You can also enable clustering and ha at the command line, e.g.

    java -jar build/libs/sqp-1.0-fat.jar -conf config.json -cluster
    java -jar build/libs/sqp-1.0-fat.jar -conf config.json -ha

Please see the Vert.x docs for a full list of Vert.x command line options.

Running the tests
-----------------
To run the tests you need to care about some prerequisites:
  1. Install and start a PostgreSQL server.
  2. Set up a PostgreSQL user called `proxyuser` with password `proxypw`.
    If you want to use another user, you need to modify the
    [test configuration](src/test/resources/backendConfiguration.json) and the
    [run configuration](config.json) accordingly.
  3. Create a `proxytest` PostgreSQL database.
  4. Install and start a Transbase server.
  5. Create a `proxytest` Transbase database.
  6. For both Transbase and PostgreSQL: Create tables with statements that can be found in
    `src/test/resources/*Table.sql`.
  7. Run the tests by executing
            ./gradlew test
    The test results can be seen on the console or as a HTML summary at
    `build/reports/tests/index.html`.

If you want to exclude a backend from the tests, you need to comment out the backend related
sections in the [common test configuration](src/test/resources/testng-common.xml) and exclude the
backend-falvored tests from being run in the [build.gradle](build.gradle) file.
In the console output you might see backtraces and logs. This does not mean that tests failed, as
some of them tests correct behavior on failure.

Running the Server with Transbase Free
--------------------------------------
Transbase has a free edition that can be used with SQP. To do so, install it at a specific location
and copy the eligible Transbase JDBC driver to that location.
You need to then build and run the server by explicitly defining the location of the `tbjdbc.jar` in the
installation location of Transbase Free.
You can also try to define the location of the Transbase Free installation by setting the Java property
`LINKED_IN_PATH`. Then you don't need to explicitly copy and define the location of `tbjdbc.jar`.

The backend configuration must then set `"pipe": true` instead of a host and port.
The integration tests currently do not work with Transbase Free.

Due to some limitations of Transbase Free, the integration tests currently fail with it.

Further Documentation
---------------------
The ideas behind the project are explained in the [master thesis](Design_of_a_Portable_SQL_Wire_Protocol.pdf).

You can build and read the Javadoc documentation by executing the `javadoc` gradle task:

    ./gradlew javadoc

The documentation can then be found in `build/docs`.

License
-------
SQP uses "dual licensing". Under this model, developers can freely choose to use SQP under the free
software/Open Source GNU Affero General Public License Version 3 (commonly known as the "AGPLv3")
or under a commercial license.

A copy of AGPLv3 can be found in the [license file](LICENSE).
For a commercial license, please contact Rothmeyer Consulting (see below).

Contact
-------
Copyright Holder: [Rothmeyer Consulting](http://www.rothmeyer.com/)

Author: Stefan Burnicki <stefan.burnicki@burnicki.net>
