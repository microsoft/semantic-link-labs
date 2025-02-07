from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import uuid
import traceback
from sempy_labs.perf_lab import _insert_into_delta_table

class ExecutionTracker:
    """
    ExecutionTracker is a context manager that logs the success or error of a code block execution into a Delta table.

    Attributes:
        table_name : str
            The name of the Delta table where logs will be stored.
        run_id : str
            A unique identifier for the execution run.
        description : str
            A description of the execution run.
        spark : SparkSession
            The Spark session used for executing SQL commands.
        start_time : datetime
            The start time of the execution run.
        end_time : datetime
            The end time of the execution run.

    Methods:
        log_event(status, message)
            Logs an event with the given status and message.
    """

    def __init__(self, table_name, run_id = str(uuid.uuid4()), description=""):
        """
        Initializes the ExecutionTracker with the specified table name.

        Args:
            table_name : str
                The name of the Delta table where logs will be stored.
            run_id : str, Default = random id
                The id of the execution run.
            description : str
                A description of the execution run.
        """
        self.table_name = table_name
        self.run_id = run_id
        self.description = description
        self.spark = SparkSession.builder.appName("ExecutionTracker").getOrCreate()
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        """
        Enters the runtime context related to this object and logs the start time.

        Returns:
            self: The ExecutionTracker instance.
        """
        self.start_time = self.spark.sql("SELECT current_timestamp()").collect()[0][0]
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Exits the runtime context related to this object, logging the success or error of the code block execution and the end time.

        Args:
            exc_type : type
                The exception type.
            exc_value : Exception
                The exception instance.
            exc_traceback : traceback
                The traceback object.

        Returns:
            bool: False to propagate the exception if any.
        """
        self.end_time = self.spark.sql("SELECT current_timestamp()").collect()[0][0]
        if exc_type is None:
            self.log_event("SUCCESS", "Execution completed successfully.")
        else:
            error_message = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.log_event("ERROR", error_message)
        return False  # Propagate exception if any

    def log_event(self, status, message):
        """
        Logs an event with the given status and message into the Delta table.

        Args:
            status : str
                The status of the event (e.g., "SUCCESS" or "ERROR").
            message : str
                The message to log.
        """
        log_df = self.spark.createDataFrame([(self.start_time, self.end_time, status, "ExecutionTracker", message, self.run_id, self.description)], 
                                            ["StartTime", "EndTime", "Status", "FunctionName", "Message", "RunId", "Description"])

        _insert_into_delta_table(log_df, self.table_name)
