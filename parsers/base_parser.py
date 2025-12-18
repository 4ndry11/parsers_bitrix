"""
Base Parser class
All document parsers should inherit from this class
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
from utils.logger import setup_logger

logger = setup_logger("BaseParser")


class BaseParser(ABC):
    """Abstract base class for document parsers"""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def parse(self, azure_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse Azure DI result and extract relevant information

        Args:
            azure_result: Result from Azure Document Intelligence

        Returns:
            Parsed data as dictionary

        Raises:
            Exception if parsing fails
        """
        pass

    @abstractmethod
    def format_for_bitrix(self, parsed_data: Dict[str, Any]) -> str:
        """
        Format parsed data for Bitrix24 display

        Args:
            parsed_data: Parsed data dictionary

        Returns:
            Formatted string (HTML, JSON, or plain text)

        Raises:
            Exception if formatting fails
        """
        pass

    def validate_result(self, azure_result: Dict[str, Any]) -> bool:
        """
        Validate that Azure result contains expected structure

        Args:
            azure_result: Result from Azure Document Intelligence

        Returns:
            True if valid

        Raises:
            ValueError if invalid
        """
        if not azure_result:
            raise ValueError("Azure result is empty")

        if "analyzeResult" not in azure_result:
            raise ValueError("Missing 'analyzeResult' in Azure response")

        if "content" not in azure_result["analyzeResult"]:
            raise ValueError("Missing 'content' in analyzeResult")

        return True
