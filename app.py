"""
Main Flask application
Webhook handler for Bitrix24 document processing
"""
import os
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv

from core.bitrix_client import BitrixClient
from core.azure_client import AzureDocumentIntelligence
from parsers.income_statement_parser import IncomeStatementParser
from utils.logger import setup_logger

# Load environment variables
load_dotenv()

# Initialize Flask app
app = Flask(__name__)
CORS(app)

# Setup logger
logger = setup_logger("FlaskApp", level=os.getenv("LOG_LEVEL", "INFO"))

# Initialize clients
bitrix_client = BitrixClient()
azure_client = AzureDocumentIntelligence()


@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "ok",
        "service": "DI Bitrix Parser",
        "version": "1.0.0"
    })


@app.route('/webhook/process-income-statement', methods=['POST', 'GET'])
def process_income_statement():
    """
    Webhook endpoint for processing income statement documents

    Accepts deal_id via:
    1. URL parameter: ?deal_id=123
    2. JSON body: {"deal_id": 123}

    Returns:
        JSON response with status
    """
    try:
        # Try to get deal_id from URL parameter first
        deal_id = request.args.get('deal_id')

        # If not in URL, try JSON body
        if not deal_id:
            data = request.get_json()
            if data:
                deal_id = data.get("deal_id")

        if not deal_id:
            logger.error("Missing deal_id parameter")
            return jsonify({
                "success": False,
                "error": "Missing deal_id parameter. Use ?deal_id=123 or JSON body"
            }), 400

        # Convert to int
        try:
            deal_id = int(deal_id)
        except ValueError:
            return jsonify({
                "success": False,
                "error": "deal_id must be a number"
            }), 400

        logger.info(f"Processing income statement for deal {deal_id}")

        # Step 1: Download file from Bitrix24 field
        logger.info("Step 1: Downloading file from Bitrix24")
        file_field = os.getenv("BITRIX_DEAL_FILE_FIELD", "UF_CRM_1765540040027")
        file_content = bitrix_client.download_file_from_field(deal_id, file_field)

        # Step 2: Analyze with Azure DI
        logger.info("Step 2: Analyzing document with Azure DI")
        azure_result = azure_client.analyze_document(file_content, model_id="prebuilt-layout")

        # Step 3: Parse the result
        logger.info("Step 3: Parsing document")
        parser = IncomeStatementParser()
        parsed_data = parser.parse(azure_result)

        # Step 4: Format for Bitrix
        logger.info("Step 4: Formatting results for Bitrix24")
        html_output = parser.format_for_bitrix(parsed_data)
        json_output = parser.to_json(parsed_data)

        # Step 5: Save to Bitrix24
        logger.info("Step 5: Saving results to Bitrix24")

        # Save JSON to result field
        result_field = os.getenv("BITRIX_DEAL_RESULT_FIELD", "UF_CRM_1765540114644")
        bitrix_client.update_deal_field(deal_id, result_field, json_output)

        # Add HTML table to timeline
        bitrix_client.add_timeline_comment(deal_id, html_output)

        logger.info("Processing completed successfully")

        return jsonify({
            "success": True,
            "deal_id": deal_id,
            "message": "Document processed successfully",
            "summary": parsed_data.get("summary", {})
        })

    except Exception as e:
        logger.error(f"Error processing document: {e}", exc_info=True)

        # Try to add error to timeline
        try:
            if 'deal_id' in locals():
                error_html = f"<p style='color: red;'><strong>Помилка обробки документа:</strong><br>{str(e)}</p>"
                bitrix_client.add_timeline_comment(deal_id, error_html)
        except:
            pass

        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.route('/webhook/process-document', methods=['POST'])
def process_document():
    """
    Generic webhook endpoint for processing documents
    Determines parser type based on document_type parameter

    Expected JSON payload:
    {
        "deal_id": 123,
        "file_id": 456,
        "document_type": "income_statement"
    }

    Returns:
        JSON response with status
    """
    try:
        data = request.get_json()

        if not data:
            return jsonify({"success": False, "error": "No data provided"}), 400

        document_type = data.get("document_type", "income_statement")

        logger.info(f"Processing document of type: {document_type}")

        # Route to appropriate parser
        if document_type == "income_statement":
            return process_income_statement()
        else:
            return jsonify({
                "success": False,
                "error": f"Unknown document type: {document_type}"
            }), 400

    except Exception as e:
        logger.error(f"Error in generic processor: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        "success": False,
        "error": "Endpoint not found"
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({
        "success": False,
        "error": "Internal server error"
    }), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', 8000))
    debug = os.getenv('FLASK_ENV') == 'development'

    logger.info(f"Starting Flask server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=debug)
