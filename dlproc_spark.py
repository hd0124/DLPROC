from pyspark.sql import SparkSession, HiveContext
import sys
from datetime import datetime

# Switch to control logging: 'postgres' or 'hive'
LOGGING_MODE = 'postgres'

# Function to get Spark application ID
def get_spark_application_id(spark):
    return spark.sparkContext.applicationId
    
def get_db_connection():
    try:
        import psycopg2
        connection = psycopg2.connect(
            user="dlproc",
            password="DLPROC@123",
            host="master-02",
            port="5432",
            database="datalake_monitoring"
        )
        return connection
    except ImportError:
        print("Error: psycopg2 package is not installed. Please install it to use PostgreSQL logging.")
        sys.exit(1)
    except Exception as e:
        print(f"Error connecting to PostgreSQL database: {e}")
        sys.exit(1)

# Function to log audit entries into PostgreSQL
def log_audit_entries_postgres(connection, audit_entries):
    try:
        import psycopg2
        cursor = connection.cursor()
        insert_query = """
        INSERT INTO datalake_monitoring.dlproc_audit (flowname, step, status, message, audit_timestamp, spark_application_id)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(insert_query, audit_entries)
        connection.commit()
    except ImportError:
        print("Error: psycopg2 package is not installed. Please install it to use PostgreSQL logging.")
        sys.exit(1)
    except Exception as e:
        print(f"Error inserting audit entries into PostgreSQL: {e}")
        connection.rollback()
    finally:
        cursor.close()

# Function to log audit entries into Hive (placeholder, not implemented fully)
def log_audit_entries_hive(spark, audit_entries):
    try:
        df = spark.createDataFrame(audit_entries, ["flowname", "step", "status", "message", "audit_timestamp", "spark_application_id"])
        df.write.insertInto("datalake_monitoring.dlproc_audit", overwrite=False)
    except Exception as e:
        print(f"Error inserting audit entries into Hive: {e}")

# Function to log audit entries based on the chosen mode
def log_audit_entries(connection, spark, audit_entries):
    if LOGGING_MODE == 'postgres':
        log_audit_entries_postgres(connection, audit_entries)
    elif LOGGING_MODE == 'hive':
        log_audit_entries_hive(spark, audit_entries)

# Function to process property file and collect audit entries
def process_property_file(spark, propertyfilename):
    hadoop_path = f"hdfs:///dlproc/property/{propertyfilename}"
    sqlContext = HiveContext(spark.sparkContext)
    lines = spark.sparkContext.textFile(hadoop_path).collect()
    audit_entries = []
    step_number = None
    sql_query = None
    temp_table_name = None
    flowname = propertyfilename.split(".")[0]  # Extract flowname from property filename
    spark_application_id = get_spark_application_id(spark)  # Fetch Spark application ID once
    
    reading_sql = False
    
    try:
        iterator = iter(lines)
        for line in iterator:
            line = line.strip()
            if line.startswith("STEP_") and line.endswith("_SQLQUERY"):
                if sql_query:
                    # Save the previous step's SQL query
                    if reading_sql:
                        reading_sql = False
                step_number = line.split("_")[1]
                sql_query = ""
                reading_sql = True
            elif line.startswith("STEP_") and (line.endswith("_TEMPTABLE") or line.endswith("_TGTQUERY")):
                if line.endswith("_TEMPTABLE"):
                    temp_table_name = next(iterator).strip()
                    if reading_sql and sql_query:
                        try:
                            spark.sql(sql_query).createOrReplaceTempView(temp_table_name)
                            audit_entries.append((flowname, step_number, "SUCCESS", f"Temporary table {temp_table_name} created.", datetime.now(), spark_application_id))
                        except Exception as e:
                            error_message = str(e).split("\n")[0]  # Extract the first line of the error message
                            audit_entries.append((flowname, step_number, "ERROR", f"Error creating temporary table {temp_table_name}: {error_message}", datetime.now(), spark_application_id))
                            raise e
                        reading_sql = False
                elif line.endswith("_TGTQUERY"):
                    sql_query = ""
                    executable_query = ""
                    while True:
                        try:
                            next_line = next(iterator)
                            if next_line.startswith("STEP_"):
                                break
                            sql_query += next_line.strip() + " "
                        except StopIteration:
                            break
                    try:
                        spark.sql(sql_query)
                        audit_entries.append((flowname, step_number, "SUCCESS", "Target query executed.", datetime.now(), spark_application_id))
                    except Exception as e:
                        error_message = str(e).split("\n")[0]  # Extract the first line of the error message
                        audit_entries.append((flowname, step_number, "ERROR", f"Error executing target query: {error_message}", datetime.now(), spark_application_id))
                        raise e
            elif line.startswith("END"):
                if reading_sql and sql_query:
                    reading_sql = False
                break
            elif reading_sql:
                sql_query += line + " "
    except Exception as e:
        # Log the overall error encountered during property file processing
        error_message = str(e).split("\n")[0]  # Extract the first line of the error message
        audit_entries.append(("UNKNOWN", "UNKNOWN", "ERROR", f"Error processing the property file: {error_message}", datetime.now(), "UNKNOWN"))
        raise e
    
    return audit_entries

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <propertyfilename>")
        sys.exit(1)

    propertyfilename = sys.argv[1]
    spark = SparkSession.builder \
        .appName(propertyfilename) \
        .enableHiveSupport() \
        .getOrCreate()
    
    connection = None
    if LOGGING_MODE == 'postgres':
        connection = get_db_connection()  # Assuming get_db_connection is defined correctly
    
    audit_entries = []
    try:
        audit_entries = process_property_file(spark, propertyfilename)
    except Exception as e:
        flowname = propertyfilename.split(".")[0]  # Extract flowname from property filename
        spark_application_id = get_spark_application_id(spark)  # Fetch Spark application ID once
        # Log the error encountered during property file processing
        print(f"Error processing the property file: {e}")
        error_message = str(e)
        error_message = error_message.split("\n")[0]  # Take only the first line
        audit_entries.append(("UNKNOWN", "UNKNOWN", "ERROR", f"Error processing the property file: {error_message}", datetime.now(), "UNKNOWN"))
    finally:
        try:
            log_audit_entries(connection, spark, audit_entries)
        except Exception as log_error:
            print(f"Error logging audit entries: {log_error}")
        spark.stop()
        if connection:
            connection.close()