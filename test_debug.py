"""
Debug script to test parser with detailed logging
"""
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Update log level to DEBUG for maximum visibility
os.environ["LOG_LEVEL"] = "DEBUG"

from core.bitrix_client import BitrixClient
from core.azure_client import AzureDocumentIntelligence
from parsers.income_statement_parser import IncomeStatementParser
from utils.logger import setup_logger

logger = setup_logger("DebugTest", level="DEBUG")

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_debug.py <deal_id>")
        sys.exit(1)

    deal_id = int(sys.argv[1])

    logger.info(f"=" * 80)
    logger.info(f"DEBUG TEST: Processing deal {deal_id}")
    logger.info(f"=" * 80)

    # Initialize clients
    bitrix_client = BitrixClient()
    azure_client = AzureDocumentIntelligence()

    # Step 1: Download file
    logger.info("STEP 1: Downloading file from Bitrix24")
    file_field = os.getenv("BITRIX_DEAL_FILE_FIELD", "UF_CRM_1765540040027")
    file_content = bitrix_client.download_file_from_field(deal_id, file_field)
    logger.info(f"Downloaded {len(file_content)} bytes")

    # Step 2: Analyze with Azure DI
    logger.info("STEP 2: Analyzing document with Azure DI")
    azure_result = azure_client.analyze_document(file_content, model_id="prebuilt-layout")

    # Log Azure DI results summary
    analyze_result = azure_result.get("analyzeResult", {})
    tables = analyze_result.get("tables", [])
    logger.info(f"Azure DI found {len(tables)} tables")
    for idx, table in enumerate(tables):
        logger.info(f"  Table {idx}: {table['rowCount']} rows x {table['columnCount']} columns, {len(table.get('cells', []))} cells")

    # Step 3: Parse the result
    logger.info("STEP 3: Parsing document")
    parser = IncomeStatementParser()
    parsed_data = parser.parse(azure_result)

    # Log parsed results
    logger.info(f"=" * 80)
    logger.info(f"PARSING RESULTS:")
    logger.info(f"Success: {parsed_data.get('success')}")
    logger.info(f"Data: {parsed_data.get('data')}")
    logger.info(f"Summary: {parsed_data.get('summary')}")
    logger.info(f"=" * 80)

    # Step 4: Format for Bitrix
    logger.info("STEP 4: Formatting for Bitrix24")
    html_output = parser.format_for_bitrix(parsed_data)
    json_output = parser.to_json(parsed_data)

    print("\n" + "=" * 80)
    print("JSON OUTPUT:")
    print(json_output)
    print("\n" + "=" * 80)
    print("HTML OUTPUT:")
    print(html_output)
    print("=" * 80)

if __name__ == "__main__":
    main()
