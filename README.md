# Metabase Driver: Databricks SQL Warehouse

## Build the driver

Docs: https://github.com/metabase/sudoku-driver?tab=readme-ov-file#building-the-driver

### 1. Install the Clojure CLI

Follow the instructions here: https://clojure.org/guides/install_clojure

### 2. Clone this repo (if you didn't do it yet)

```
git clone git@github.com:flash-tecnologia/dataflash.driver.metabase-databricks.git
```

### 3. Clone the Metabase repo, on the same directory 

```
git clone https://github.com/metabase/metabase
```

Your folder structure should look like this:

```
root
│
└───dataflash.driver.metabase-databricks
│   │   ...
│
└───metabase
│   │   ...
│
...
```

### 4. Run build script

Change to the driver directory:

```
cd dataflash.driver.metabase-databricks
```

Run the `build_after_v046.sh` script, if you are using a Metabase version greater than v0.46 (otherwise, run the `build_before_v046.sh` script)

```
sh build_after_v046.sh
```

**The driver jar will be generated at /target folder**

## Run Metabase locally with Databricks driver installed 

### Using Docker (not tested)

I never tried this way, but if you feel more comfortable with it, here is the docs.

https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker

---
### Without Docker

Docs (only for reference, you might not need it if you follow the steps below): https://www.metabase.com/docs/latest/installation-and-operation/running-the-metabase-jar-file

To test the driver on a local Metabase instance, without using Docker, you can follow these steps:

#### Prereq: Install Open JDK (11+)

You should be able to run the command:

```
java --version
```

#### 1. Download the jar file

You can find all releases and the jar files here: https://github.com/metabase/metabase/releases

Always use "Metabase v0.xx.x" versions (without "RC" or "Enterprise" on its name)

#### 2. Create a folder for testing 

Create a specific folder when the Metabase files will be stored, for instance `~/metabase_local`.

Put the `metabase.jar` file you downloaded on this folder.

#### 3. Add the plugin/driver

Create a new folder named `plugins` inside the previous one, for instance `~/metabase_local/plugins`.

Copy the plugin jar to this plugins folder.

Your file structure should look like this:

```
test_metabase
│   metabase.jar
│
└───plugins
    │   databricks-sql.metabase-driver.jar
```

#### 4. Run the jar

Change into your directory:
```
cd ~/test_metabase
```

And finally run the jar file:
```
java -jar metabase.jar
```

After a few seconds, you can access and test your Metabase at http://localhost:3000