(ns metabase.driver.databricks-sql
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [honeysql.core :as hsql]
            [java-time :as t]
            [medley.core :as m]
            [metabase.driver :as driver] 
            [metabase.driver.ddl.interface :as ddl.i]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.driver.sql.util :as sql.u]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.util :as mbql.u]
            [metabase.query-processor.util :as qp.util]
            [metabase.util.honeysql-extensions :as hx])
  (:import [java.sql Connection ResultSet]
           [java.time OffsetDateTime ZonedDateTime]))

(driver/register! :databricks-sql, :parent :sql-jdbc)

(defmethod sql-jdbc.conn/connection-details->spec :databricks-sql
  [_ {:keys [host http-path password db catalog]}]
  {:classname        "com.databricks.client.jdbc.Driver"
   :subprotocol      "databricks"
   :subname          (str "//" host ":443/" db)
   :transportMode    "http"
   :ssl              1
   :AuthMech         3
   :httpPath         http-path
   :uid              "token"
   :pwd              password
   :connCatalog      catalog
   })

(defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :databricks-sql
  [driver database]
  ;; The Hive JDBC driver doesn't support `Connection.isValid()`, so we need to supply a test query for c3p0 to use to
  ;; validate connections upon checkout.
  (merge
   ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :sql-jdbc) driver database)
   {"preferredTestQuery" "SELECT 1"}))

(defmethod sql-jdbc.sync/database-type->base-type :databricks-sql
  [_ database-type]
  (condp re-matches (name database-type)
    #"(?i)BOOLEAN"   :type/Boolean
    #"(?i)BYTE"      :type/Integer
    #"(?i)TINYINT"   :type/Integer
    #"(?i)SHORT"     :type/Integer
    #"(?i)SMALLINT"  :type/Integer
    #"(?i)INT"       :type/Integer
    #"(?i)INTEGER"   :type/Integer
    #"(?i)LONG"      :type/BigInteger
    #"(?i)BIGINT"    :type/BigInteger
    #"(?i)FLOAT"     :type/Float
    #"(?i)REAL"      :type/Float
    #"(?i)DOUBLE"    :type/Float
    #"(?i)DATE"      :type/Date
    #"(?i)TIMESTAMP" :type/DateTimeWithLocalTZ ; stored as UTC in the database
    #"(?i)STRING"    :type/Text
    #"(?i)BINARY"    :type/*
    #"(?i)DECIMAL.*" :type/Decimal
    #"(?i)DEC"       :type/Decimal
    #"(?i)NUMERIC"   :type/Decimal
    #"(?i)INTERVAL"  :type/*
    #"(?i)ARRAY.*"   :type/Array
    #"(?i)STRUCT.*"  :type/*
    #"(?i)MAP"       :type/*))

(defmethod sql.qp/date [:databricks-sql :minute]          [_ _ expr] (hsql/call :date_trunc "minute" expr))
(defmethod sql.qp/date [:databricks-sql :minute-of-hour]  [_ _ expr] (hsql/call :minute expr))
(defmethod sql.qp/date [:databricks-sql :hour]            [_ _ expr] (hsql/call :date_trunc "hour" expr))
(defmethod sql.qp/date [:databricks-sql :hour-of-day]     [_ _ expr] (hsql/call :hour expr))
(defmethod sql.qp/date [:databricks-sql :day]             [_ _ expr] (hsql/call :to_date expr))
(defmethod sql.qp/date [:databricks-sql :day-of-week]     [_ _ expr] (hsql/call :dayofweek expr))
(defmethod sql.qp/date [:databricks-sql :day-of-month]    [_ _ expr] (hsql/call :dayofmonth expr))
(defmethod sql.qp/date [:databricks-sql :day-of-year]     [_ _ expr] (hsql/call :dayofyear expr))
(defmethod sql.qp/date [:databricks-sql :week]            [_ _ expr] (hsql/call :date_trunc "week" expr))
(defmethod sql.qp/date [:databricks-sql :week-of-year]    [_ _ expr] (hsql/call :weekofyear expr))
(defmethod sql.qp/date [:databricks-sql :month]           [_ _ expr] (hsql/call :trunc expr "MM"))
(defmethod sql.qp/date [:databricks-sql :month-of-year]   [_ _ expr] (hsql/call :month expr))
(defmethod sql.qp/date [:databricks-sql :quarter]         [_ _ expr] (hsql/call :date_trunc "quarter" expr))
(defmethod sql.qp/date [:databricks-sql :quarter-of-year] [_ _ expr] (hsql/call :quarter expr))
(defmethod sql.qp/date [:databricks-sql :year]            [_ _ expr] (hsql/call :date_trunc "year" expr))

(defmethod sql.qp/add-interval-honeysql-form :databricks-sql
  [_ hsql-form amount unit]
  ;; (hx/+ (hx/->timestamp hsql-form) (hsql/raw (format "(INTERVAL '%d' %s)" (int amount) (name unit)))))
  ( let [
      interval (if (= unit :quarter) 
        {:amount (* 3 (int amount)), :unit "month"}
        {:amount (int amount), :unit (name unit)})
      ]
     (hx/+ (hx/->timestamp hsql-form) (hsql/raw (format "(INTERVAL '%d' %s)" (:amount interval) (:unit interval))))))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
(defmethod driver/describe-database :databricks-sql
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
      (for [{:keys [database tablename], table-namespace :namespace} (jdbc/query {:connection conn} ["show tables"])]
        {:name   tablename
         :schema (or (not-empty database)
                     (not-empty table-namespace))})))})

;; Hive describe table result has commented rows to distinguish partitions
(defn- valid-describe-table-row? [{:keys [col_name data_type]}]
  (every? (every-pred (complement str/blank?)
                      (complement #(str/starts-with? % "#")))
          [col_name data_type]))

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"-" "_")))

;; workaround for SPARK-9686 Spark Thrift server doesn't return correct JDBC metadata
(defmethod driver/describe-table :databricks-sql
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query {:connection conn} [(format
                                                    "describe %s"
                                                    (sql.u/quote-name driver :table
                                                                      (dash-to-underscore schema)
                                                                      (dash-to-underscore table-name)))])]
       (set
        (for [[idx {col-name :col_name, data-type :data_type, :as result}] (m/indexed results)
              :while (valid-describe-table-row? result)]
          {:name              col-name
           :database-type     data-type
           :base-type         (sql-jdbc.sync/database-type->base-type :databricks-sql (keyword data-type))
           :database-position idx}))))})

(def ^:dynamic *param-splice-style*
  "How we should splice params into SQL (i.e. 'unprepare' the SQL). Either `:friendly` (the default) or `:paranoid`.
  `:friendly` makes a best-effort attempt to escape strings and generate SQL that is nice to look at, but should not
  be considered safe against all SQL injection -- use this for 'convert to SQL' functionality. `:paranoid` hex-encodes
  strings so SQL injection is impossible; this isn't nice to look at, so use this for actually running a query."
  :friendly)

;; bound variables are not supported in Spark SQL (maybe not Hive either, haven't checked)
(defmethod driver/execute-reducible-query :databricks-sql
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [inner-query (-> (assoc inner-query
                               :remark (qp.util/query->remark :databricks-sql outer-query)
                               :query  (if (seq params)
                                         (binding [*param-splice-style* :paranoid]
                                           (unprepare/unprepare driver (cons sql params)))
                                         sql)
                               :max-rows (mbql.u/query->max-rows-limit outer-query))
                        (dissoc :params))
        query       (assoc outer-query :native inner-query)]
    ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))

;; 1.  SparkSQL doesn't support `.supportsTransactionIsolationLevel`
;; 2.  SparkSQL doesn't support session timezones (at least our driver doesn't support it)
;; 3.  SparkSQL doesn't support making connections read-only
;; 4.  SparkSQL doesn't support setting the default result set holdability
(defmethod sql-jdbc.execute/connection-with-timezone :databricks-sql
  [driver database _timezone-id]
  (let [conn (.getConnection (sql-jdbc.execute/datasource-with-diagnostic-info! driver database))]
    (try
      (.setTransactionIsolation conn Connection/TRANSACTION_READ_UNCOMMITTED)
      conn
      (catch Throwable e
        (.close conn)
        (throw e)))))

;; 1.  SparkSQL doesn't support setting holdability type to `CLOSE_CURSORS_AT_COMMIT`
(defmethod sql-jdbc.execute/prepared-statement :databricks-sql
  [driver ^Connection conn ^String sql params]
  (let [stmt (.prepareStatement conn sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))

;; the current HiveConnection doesn't support .createStatement
(defmethod sql-jdbc.execute/statement-supported? :databricks-sql [_] false)

(doseq [feature [:basic-aggregations
                 :binning
                 :expression-aggregations
                 :expressions
                 :native-parameters
                 :nested-queries
                 :standard-deviation-aggregations]]
  (defmethod driver/supports? [:databricks-sql feature] [_ _] true))

;; only define an implementation for `:foreign-keys` if none exists already. In test extensions we define an alternate
;; implementation, and we don't want to stomp over that if it was loaded already
(when-not (get (methods driver/supports?) [:databricks-sql :foreign-keys])
  (defmethod driver/supports? [:databricks-sql :foreign-keys] [_ _] true))

(defmethod sql.qp/quote-style :databricks-sql [_] :mysql)

(defmethod unprepare/unprepare-value [:databricks-sql OffsetDateTime]
  [_ t]
  (format "timestamp '%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))

(defmethod unprepare/unprepare-value [:databricks-sql ZonedDateTime]
  [_ t]
  (format "timestamp '%s'" (t/format "yyyy-MM-dd HH:mm:ss.SSSZZZZZ" t)))
