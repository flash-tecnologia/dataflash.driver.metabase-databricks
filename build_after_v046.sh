# DOCS
# https://github.com/metabase/sudoku-driver?tab=readme-ov-file#build-it-updated-for-build-script-changes-in-metabase-0460 

cd ../metabase

# get path to the driver project directory
DRIVER_PATH=`readlink -f ../dataflash.driver.metabase-databricks`

# Build driver
clojure \
  -Sdeps "{:aliases {:databricks-sql {:extra-deps {com.metabase/databricks-sql-driver {:local/root \"$DRIVER_PATH\"}}}}}"  \
  -X:build:databricks-sql \
  build-drivers.build-driver/build-driver! \
  "{:driver :databricks-sql, :project-dir \"$DRIVER_PATH\", :target-dir \"$DRIVER_PATH/target\"}"

# JAR with the databricks driver will be saved in target/ folder