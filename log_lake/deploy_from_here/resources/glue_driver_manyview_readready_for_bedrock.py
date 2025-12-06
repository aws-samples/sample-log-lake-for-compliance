import logging
import re
from typing import Dict, Any, Tuple, Optional, List
from datetime import datetime, timezone
import traceback
from pyspark.sql import DataFrame, SparkSession
from botocore.exceptions import ClientError
import sys
from awsglue.utils import getResolvedOptions


partcols = ["logcalendarday", "loghour"]


def parse_arguments() -> dict:
    """
    Parse job arguments using Glue's getResolvedOptions.
    """
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "limitForBoundedExecution",
            "dbForLogLake",
            "relativeDaysBeginForBound",
            "aws_region",
            "aws_account_id",
            "LocationOut",
        ],
    )

    assert 1 <= int(args['relativeDaysBeginForBound']) <= 999, "relativeDaysBeginForBound must be between 1 and 999"
    
    assert 1 <= int(args['limitForBoundedExecution']) <= 999999999, "limitForBoundedExecution must be between 1 and 999,999,999"
    
    assert re.fullmatch(r'\d{12}', args["aws_account_id"]), f"aws_account_id must be 12 digits: {args['aws_account_id']}"
    
    assert re.fullmatch(r'[a-z0-9_-]{1,255}', args["dbForLogLake"]), f"Glue Database Name must contain lowercase letters, numbers, underscores, or hyphens: {args['dbForLogLake']}"
    
    assert args["LocationOut"].startswith("s3://"), f"LocationOut must be an S3 URL: {args['LocationOut']}"
    
    assert re.fullmatch(r'[a-z0-9-]+', args["aws_region"]), f"Invalid aws_region, must contain only lowercase letters, numbers, and dashes: {args['aws_region']}"

    return args


def set_logging(logsig: str) -> logging.Logger:
    """Configure logging to CloudWatch."""

    my_logging_signature = logsig
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(funcName)s %(levelname)-8s %(message)s",
        handlers=[stream_handler],
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    # retrieve logger instance
    logger = logging.getLogger(
        my_logging_signature
    )  # root is the default logger name if not specified otherwise
    return logger


def get_now_as_time() -> Tuple[datetime, str]:
    """
    Generate standardized timestamp in UTC ISO8601 format for consistency
    across locations.
    For example, September 27, 2022 at 6 p.m. is represented as 2022-09-27
    18:00:00.000.
    ref:https://www.iso.org/iso-8601-date-and-time-format.html
    """
    # for general use
    t = datetime.now(timezone.utc)
    # for timestamps in data or partitioning
    t_as_isostandard = f"{t.strftime('%Y-%m-%dT%H:%M:%S')}.{str(t.microsecond)[:3]}Z"
    return t, t_as_isostandard


def set_spark_session(args, app_name) -> SparkSession:
    """
    Initialize Spark session
    """
    # must be set to interact with hive tables
    hive_catalog = "hive_catalog"
    # AWS Glue Data Catalog is pre-configured for use by the Spark libraries as glue_catalog
    glue_catalog = "glue_catalog"

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "32MB")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config(f"spark.sql.catalog.{glue_catalog}.client.region", args["aws_region"])
        .config(
            f"spark.sql.catalog.{glue_catalog}.glue.account-id", args["aws_account_id"]
        )
        .config(
            f"spark.sql.catalog.{hive_catalog}", "org.apache.spark.sql.hive.HiveCatalog"
        )
        .config(f"spark.sql.catalog.{hive_catalog}.type", "hive")
        .config(
            "spark.hadoop.hive.metastore.client.factory.class",
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .config("spark.sql.catalog.default", hive_catalog)
        .config("hive.exec.dynamic.partition.mode", "nonstrict")
        .enableHiveSupport()
        .getOrCreate()
    )
    return spark


def write_to_s3(
    logger: logging.Logger,
    df_to_write: DataFrame,
    path_for_save: str,
    partition_cols: list = None,
) -> None:
    """Write DataFrame to specified location. Job author can specify
    any valid spark write.mode format & compression option.

    For save to s3, format should be "s3://somebucket/somepath/"
    """

    stripesize = "256MB"
    compressionblocksize = "256MB"
    # maxrecordsperfile = 5000000
    writemode = "overwrite"  # append, overwrite. we use overwrite for demos & append w/where not exists for regular runs
    fileformat = "orc"
    compression = "zlib"

    # first we do just configuration, no files/emitting yet
    w = (
        df_to_write.write.mode(writemode)
        .format(fileformat)
        .option("compression", compression)
        .option("stripeSize", stripesize)
        .option("compressionBlockSize", compressionblocksize)
        .option("maxRecordsPerFile", 5000000)
    )

    if partition_cols is not None:
        logger.info(f"Partition column(s) list found: {partition_cols}")
        w = w.partitionBy(*partition_cols)
        logger.info(f"Partition columns added to writer...")

    # emit files (this means persist physically outside runtime)
    w.save(path_for_save)
    logger.info(
        f"Data frame saved to files under {path_for_save}; files were writtin with mode {writemode}, fileformat {fileformat}, compression {compression}..."
    )


def create_view_from_sqlstatement(
    logger: logging.Logger, spark: SparkSession, sqlstatement: str, name_of_view: str
) -> None:
    """
    Create a view from a SQL statement.
    """
    result_as_df = spark.sql(sqlstatement)
    result_as_df.createOrReplaceTempView(name_of_view)
    logger.info(f"created view {name_of_view} from sqlstatement...")


def run() -> None:
    """
    Main execution flow for the job.
    """
    args = parse_arguments()

    logger = set_logging(args["JOB_NAME"])
    logger.info(f"logging set...")
    logger.info(f"args: {args}")

    # add args (not passed)
    passed_limit = str(args["limitForBoundedExecution"])
    days_begin = str(args["relativeDaysBeginForBound"])
    my_db = args["dbForLogLake"]

    logger.info(f"added args...")

    try:
        logger.info(f"job {args['JOB_NAME']} started...")
        spark = set_spark_session(args=args, app_name=args["JOB_NAME"])

        _, t_as_isostandard = get_now_as_time()
        logger.info(f"Job initialized at {t_as_isostandard}")

        # prepare views

        # mybounds_as_view
        name_of_view = "mybounds_as_view"
        sql_statement = f"""
SELECT (current_timestamp() - INTERVAL {days_begin} DAY) floor_as_time
,cast((current_timestamp() - INTERVAL {days_begin} DAY) AS date) floor_as_date
,cast((current_timestamp() + INTERVAL  '2' DAY) AS date) ceiling_as_date
LIMIT 1
"""
        create_view_from_sqlstatement(
            logger,
            spark,
            sql_statement,
            name_of_view,
        )

        name_of_view = "mydates_expanded_as_view"
        sql_statement = """ 
    select cast(thedate as date) thedate
        ,cast(year(thedate) as int) as y
        ,cast(month(thedate) as int) as m
        ,cast(day(thedate) as int) as d
    from (
        select sequence(floor_as_date, ceiling_as_date, interval '1' day) as dates
        from mybounds_as_view
        ) as mydates
    lateral view explode (mydates.dates) exploded_view as thedate
"""
        create_view_from_sqlstatement(
            logger,
            spark,
            sql_statement,
            name_of_view,
        )

        # final sql statement. present columns in order of target table's schema.
        sql_statement_for_job = f"""
SELECT schematype,
schemaversion,
cast(accountId AS long) as logaccountid,
cast(timestamp as timestamp) as request_time,
cast(requestId as string) as requestid,
cast(operation as string) operation,
cast(modelId as string) as modelid,
cast(input.inputBodyJson.messages as string) AS input_messages,
to_json(input.inputBodyJson.tools) AS input_tools,
cast(input.inputTokenCount as int) AS input_inputtokencount,
cast(input as string) as input_orig,
cast(output.outputBodyJson.id as string) AS output_outputbodyjson_id,
cast(output.outputBodyJson.type as string) AS output_outputbodyjson_type,
cast(output.outputBodyJson.role as string) AS output_outputbodyjson_role,
cast(output.outputBodyJson.model as string) AS output_outputbodyjson_model,
cast( output.outputBodyJson.content as string) as output_outputbodyjson_content,
cast(output.outputBodyJson.stop_reason as string) AS output_outputbodyjson_stopreason,
cast(output.outputBodyJson.stop_sequence as string) AS output_outputbodyjson_stopsequence,
output.outputTokenCount AS output_outputtokencount,
cast(output as string) as output_orig,
inferenceRegion as awsregion,
cast(concat(
    cast(logyear as string),
    lpad(cast(logmon as string), 2, '0'),
    lpad(cast(logday as string), 2, '0')
) as bigint) as logcalendarday,
loghour
from {my_db}.from_bedrock_raw 
join mydates_expanded_as_view v 
    on {my_db}.from_bedrock_raw.logyear=v.y
    and {my_db}.from_bedrock_raw.logmon=v.m
    and {my_db}.from_bedrock_raw.logday=v.d
--add logic to exclude rows if matched in target table, if desired
"""
        logger.info(f"attempting final sql statement: {sql_statement_for_job}")

        returned_df = spark.sql(sql_statement_for_job)
        logger.info(f"sql ran successfully...")

        logger.info(f"checking if data present for persisting out...")
        first_col = returned_df.columns[0]
        logger.info(f"first column: {first_col}...")
        first_row = returned_df.select(first_col).take(1)
        logger.info(f"first row: {first_row}...")
        is_empty = first_row == []
        logger.info(f"is_empty is {is_empty}")

        if is_empty:
            logger.info(
                "No output data to process. This is expected for write operations (insert/update/delete), which have no output data."
            )
        else:
            logger.info("output data found, i will attempt to write to s3...")
            try:
                logger.info(f"attempting to read output parameters...")
                logger.info(f"partcols set to {partcols}")
                if partcols is None or partcols == "":
                    logger.info(
                        "partcols not specified, i will not partition output data..."
                    )
                elif partcols is not None and partcols != "":
                    logger.info(f"partcols found, i will use it...")
                logger.info(f"output parameters read successfully...")
                write_to_s3(logger, returned_df, args["LocationOut"], partcols)
            except ValueError as e:
                raise e
            except Exception as e:
                raise e

        _, t_as_isostandard = get_now_as_time()
        logger.info(f"Job completed successfully at {t_as_isostandard}")

    except Exception as e:
        logger.info(f"unhandled exception, full traceback is: {traceback.format_exc()}")
        raise e


def main():
    """
    Main entry point for Glue job execution.
    """
    run()


if __name__ == "__main__":
    main()
