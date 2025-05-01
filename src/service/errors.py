"""
Custom error types for the Delta Lake MCP Server.
"""

# mostly copied from https://github.com/kbase/cdm-task-service/blob/main/cdmtaskservice/errors.py

from enum import Enum


class ErrorType(Enum):
    """
    The type of an error, consisting of an error code and a brief string describing the type.
    :ivar error_code: an integer error code.
    :ivar error_type: a brief string describing the error type.
    """

    AUTHENTICATION_FAILED = (10000, "Authentication failed")
    """ A general authentication error. """

    NO_TOKEN = (10010, "No authentication token")
    """ No token was provided when required. """

    INVALID_TOKEN = (10020, "Invalid token")
    """ The token provided is not valid. """

    INVALID_AUTH_HEADER = (10030, "Invalid authentication header")
    """ The authentication header is not valid. """

    MISSING_ROLE = (10040, "Missing required role")
    """ The user is missing a required role. """

    # ----- Delta Lake specific error types -----
    DELTA_LAKE_ERROR = (20000, "Delta Lake error")
    """ A general error related to Delta Lake. """

    INVALID_S3_PATH = (20010, "Invalid S3 path")
    """ The S3 path format is invalid. """

    DELTA_TABLE_NOT_FOUND = (20020, "Delta table not found")
    """ The Delta table was not found at the specified path. """

    DELTA_SCHEMA_ERROR = (20030, "Delta schema error")
    """ There is an issue with the Delta table schema. """

    S3_ACCESS_ERROR = (20040, "S3 access error")
    """ There was an error accessing S3 storage. """

    DELTA_TABLE_OPERATION_ERROR = (20050, "Delta table operation error")
    """ An operation on a Delta table failed. """

    REQUEST_VALIDATION_FAILED = (30010, "Request validation failed")
    """ A request to a service failed validation of the request. """

    def __init__(self, error_code, error_type):
        self.error_code = error_code
        self.error_type = error_type
