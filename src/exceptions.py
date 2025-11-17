class ArxivAPIException(Exception):
    """Base exception for arxiv API-related errors."""


class ArxivAPITimeoutError(ArxivAPIException):
    """Exception raised when API request times out."""


class ArxivAPIRateLimitError(ArxivAPIException):
    """Exception raised when arxiv API rate limit exceeds."""


class ArxivParseError(ArxivAPIException):
    """Exception raised when arxiv API response parsing fails."""


class PDFDownloadException(Exception):
    """Base exception for PDF download-related errors."""


class PDFDownloadTimeoutError(PDFDownloadException):
    """Exception raised when PDF download times out."""
