"""
Azure Document Intelligence Client
Handles document analysis using Azure Form Recognizer
"""
import os
from typing import Dict, Any
from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from utils.logger import setup_logger

logger = setup_logger("AzureClient")


class AzureDocumentIntelligence:
    """Client for Azure Document Intelligence API"""

    def __init__(self):
        self.endpoint = os.getenv("AZURE_DI_ENDPOINT")
        self.key = os.getenv("AZURE_DI_KEY")

        if not self.endpoint or not self.key:
            raise ValueError("Azure DI credentials not configured. Check AZURE_DI_ENDPOINT and AZURE_DI_KEY")

        self.client = DocumentAnalysisClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.key)
        )

        logger.info(f"AzureDocumentIntelligence initialized with endpoint: {self.endpoint}")

    def analyze_document(self, document_bytes: bytes, model_id: str = "prebuilt-layout") -> Dict[str, Any]:
        """
        Analyze document using Azure Document Intelligence

        Args:
            document_bytes: Document content as bytes
            model_id: Model to use (default: prebuilt-layout)

        Returns:
            Analysis result as dictionary

        Raises:
            Exception if analysis fails
        """
        try:
            logger.info(f"Starting document analysis with model: {model_id}")
            logger.info(f"Document size: {len(document_bytes)} bytes")

            # Start analysis
            poller = self.client.begin_analyze_document(
                model_id=model_id,
                document=document_bytes
            )

            logger.info("Waiting for analysis to complete...")
            result = poller.result()

            # Convert result to dictionary format similar to REST API
            result_dict = {
                "status": "succeeded",
                "analyzeResult": {
                    "modelId": result.model_id,
                    "content": result.content,
                    "pages": [],
                    "tables": []
                }
            }

            # Add pages information
            for page in result.pages:
                page_dict = {
                    "pageNumber": page.page_number,
                    "width": page.width,
                    "height": page.height,
                    "unit": page.unit,
                    "lines": []
                }

                # Add lines
                if hasattr(page, 'lines'):
                    for line in page.lines:
                        page_dict["lines"].append({
                            "content": line.content,
                            "polygon": line.polygon
                        })

                result_dict["analyzeResult"]["pages"].append(page_dict)

            # Add tables information
            if hasattr(result, 'tables'):
                for table in result.tables:
                    table_dict = {
                        "rowCount": table.row_count,
                        "columnCount": table.column_count,
                        "cells": []
                    }

                    for cell in table.cells:
                        table_dict["cells"].append({
                            "rowIndex": cell.row_index,
                            "columnIndex": cell.column_index,
                            "content": cell.content,
                            "rowSpan": getattr(cell, 'row_span', 1),
                            "columnSpan": getattr(cell, 'column_span', 1)
                        })

                    result_dict["analyzeResult"]["tables"].append(table_dict)

            logger.info(f"Analysis completed successfully")
            logger.info(f"Pages found: {len(result_dict['analyzeResult']['pages'])}")
            logger.info(f"Tables found: {len(result_dict['analyzeResult']['tables'])}")

            return result_dict

        except Exception as e:
            logger.error(f"Document analysis failed: {e}")
            raise

    def extract_text(self, document_bytes: bytes) -> str:
        """
        Extract plain text from document

        Args:
            document_bytes: Document content as bytes

        Returns:
            Extracted text

        Raises:
            Exception if extraction fails
        """
        try:
            result = self.analyze_document(document_bytes)
            text = result["analyzeResult"]["content"]
            logger.info(f"Extracted {len(text)} characters of text")
            return text

        except Exception as e:
            logger.error(f"Text extraction failed: {e}")
            raise
