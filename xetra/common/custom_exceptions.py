"""Custom Exceptions"""

class WrongFormatException(Exception):
    """
    WrongFormatException class

    Exception that can be raised when the format parameter given is not supported
    """

class WrongMetaFileException(Exception):
    """
    WrongMetaFileException class

    Exception that can be raised when the meta_file format is not correct
    """