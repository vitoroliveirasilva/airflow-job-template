"""Small operational error taxonomy used by jobs and the Airflow adapter"""


class JobError(Exception):
    """Base error for expected job failures"""


class RetryableJobError(JobError):
    """A transient failure that may succeed when Airflow retries the task"""


class NonRetryableJobError(JobError):
    """A permanent failure that should fail the task without consuming retries"""


class JobConfigurationError(NonRetryableJobError):
    """Invalid or missing job configuration"""
