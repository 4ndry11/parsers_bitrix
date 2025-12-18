"""
Income Statement Parser
Parses Ukrainian income statement (Справка про доходи)
"""
import re
import json
from typing import Dict, Any, List, Tuple
from collections import defaultdict
from .base_parser import BaseParser
from utils.logger import setup_logger

logger = setup_logger("IncomeStatementParser")


class IncomeStatementParser(BaseParser):
    """Parser for Ukrainian income statement documents"""

    def __init__(self):
        super().__init__()
        self.logger.info("IncomeStatementParser initialized")

    def parse(self, azure_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse income statement from Azure DI result

        The document has a table with columns:
        - Column 4: рік (year)
        - Column 7: Сума доходу нарахованого (accrued income)
        - Column 13: Код та назва ознаки доходу (income code and name)

        We need to:
        1. Group by year
        2. Within each year, group by income code
        3. Sum accrued income amounts

        Args:
            azure_result: Result from Azure Document Intelligence

        Returns:
            Parsed data structured by year and code
        """
        try:
            self.validate_result(azure_result)

            content = azure_result["analyzeResult"]["content"]
            tables = azure_result["analyzeResult"].get("tables", [])

            self.logger.info(f"Parsing document with {len(tables)} tables")

            # Extract data from tables
            income_data = self._extract_from_tables(tables)

            # If no tables found, try extracting from text
            if not income_data:
                self.logger.warning("No data extracted from tables, trying text extraction")
                income_data = self._extract_from_text(content)

            # Group and sum the data
            grouped_data = self._group_and_sum(income_data)

            result = {
                "success": True,
                "data": grouped_data,
                "summary": self._create_summary(grouped_data)
            }

            self.logger.info(f"Parsing completed successfully. Found {len(grouped_data)} years")
            return result

        except Exception as e:
            self.logger.error(f"Parsing failed: {e}")
            return {
                "success": False,
                "error": str(e),
                "data": {}
            }

    def _extract_from_tables(self, tables: List[Dict]) -> List[Dict[str, Any]]:
        """
        Extract income data from tables

        Returns:
            List of records with year, code, and amount
        """
        records = []

        self.logger.info(f"Processing {len(tables)} tables from Azure DI")

        for table_idx, table in enumerate(tables):
            cells = table.get("cells", [])
            if not cells:
                self.logger.warning(f"Table {table_idx} has no cells")
                continue

            row_count = table["rowCount"]
            col_count = table["columnCount"]
            self.logger.info(f"Table {table_idx}: {row_count} rows x {col_count} columns")

            # Build table structure
            table_data = self._build_table_structure(cells, row_count, col_count)

            # Log first few rows to see what we're working with
            self.logger.info(f"Table {table_idx} first 5 rows:")
            for row_idx in range(min(5, len(table_data))):
                self.logger.info(f"  Row {row_idx}: {table_data[row_idx][:10] if len(table_data[row_idx]) > 10 else table_data[row_idx]}")  # First 10 cells

            # Extract header row to find column indices
            header_row = 0
            col_year = None
            col_amount = None
            col_code = None

            # Find column indices (searching in first few rows for headers)
            for row_idx in range(min(5, len(table_data))):
                row = table_data[row_idx]
                for col_idx, cell_content in enumerate(row):
                    if cell_content and "рік" in cell_content.lower():
                        col_year = col_idx
                        self.logger.info(f"Found 'рік' at column {col_idx}: '{cell_content}'")
                    if cell_content and "нарахованого" in cell_content.lower():
                        col_amount = col_idx
                        self.logger.info(f"Found 'нарахованого' at column {col_idx}: '{cell_content}'")
                    if cell_content and "код" in cell_content.lower() and "ознаки" in cell_content.lower():
                        col_code = col_idx
                        self.logger.info(f"Found 'код...ознаки' at column {col_idx}: '{cell_content}'")

            if col_year is None or col_amount is None or col_code is None:
                self.logger.warning(f"Table {table_idx}: Could not find all required columns (Year:{col_year}, Amount:{col_amount}, Code:{col_code})")
                continue

            self.logger.info(f"Table {table_idx} - Found columns - Year: {col_year}, Amount: {col_amount}, Code: {col_code}")

            # Extract data rows (skip header rows)
            rows_processed = 0
            rows_extracted = 0
            for row_idx in range(header_row + 1, len(table_data)):
                row = table_data[row_idx]
                rows_processed += 1

                try:
                    year_cell = row[col_year] if col_year < len(row) else ""
                    amount_cell = row[col_amount] if col_amount < len(row) else ""
                    code_cell = row[col_code] if col_code < len(row) else ""

                    # Extract year (should be 4 digits)
                    year_match = re.search(r'\b(20\d{2})\b', year_cell)
                    if not year_match:
                        self.logger.debug(f"Row {row_idx}: No year found in '{year_cell}'")
                        continue

                    year = year_match.group(1)

                    # Extract amount (number with optional decimal)
                    amount_cell_clean = amount_cell.replace(' ', '').replace(',', '.')
                    amount_match = re.search(r'(\d+(?:[.,]\d+)?)', amount_cell_clean)
                    if not amount_match:
                        self.logger.debug(f"Row {row_idx}: No amount found in '{amount_cell}'")
                        continue

                    amount = float(amount_match.group(1).replace(',', '.'))

                    # Extract code (3 digits or specific patterns like "101 - Заробітна плата")
                    code_match = re.search(r'\b(\d{3})\b', code_cell)
                    if not code_match:
                        self.logger.debug(f"Row {row_idx}: No code found in '{code_cell}'")
                        continue

                    code = code_match.group(1)

                    # Extract code name if present
                    code_name_match = re.search(r'\d{3}\s*-\s*(.+)', code_cell)
                    code_name = code_name_match.group(1).strip() if code_name_match else ""

                    self.logger.info(f"Row {row_idx}: Extracted - Year:{year}, Code:{code}, Amount:{amount}, Name:{code_name}")

                    records.append({
                        "year": year,
                        "code": code,
                        "code_name": code_name,
                        "amount": amount
                    })
                    rows_extracted += 1

                except Exception as e:
                    self.logger.debug(f"Skipping row {row_idx}: {e}")
                    continue

            self.logger.info(f"Table {table_idx}: Processed {rows_processed} rows, extracted {rows_extracted} records")

        self.logger.info(f"Extracted {len(records)} records from tables")
        return records

    def _extract_from_text(self, content: str) -> List[Dict[str, Any]]:
        """
        Extract income data from text content as fallback

        Returns:
            List of records with year, code, and amount
        """
        records = []

        # Split by lines
        lines = content.split('\n')

        # Pattern to match income records
        # Looking for patterns like: "2022  ...  9387.08  ...  101 - Заробітна плата"
        for line in lines:
            try:
                # Extract year
                year_match = re.search(r'\b(20\d{2})\b', line)
                if not year_match:
                    continue

                year = year_match.group(1)

                # Extract amount (looking for decimal numbers)
                amounts = re.findall(r'\b(\d+\.\d{2})\b', line)
                if not amounts:
                    continue

                # Usually the "нарахованого" amount is repeated or the first one
                amount = float(amounts[0])

                # Extract code
                code_match = re.search(r'\b(\d{3})\s*-\s*([^\n]+)', line)
                if not code_match:
                    continue

                code = code_match.group(1)
                code_name = code_match.group(2).strip()

                records.append({
                    "year": year,
                    "code": code,
                    "code_name": code_name,
                    "amount": amount
                })

            except Exception as e:
                self.logger.debug(f"Skipping line: {e}")
                continue

        self.logger.info(f"Extracted {len(records)} records from text")
        return records

    def _build_table_structure(self, cells: List[Dict], row_count: int, col_count: int) -> List[List[str]]:
        """
        Build 2D table structure from cells

        Args:
            cells: List of cell dictionaries
            row_count: Number of rows
            col_count: Number of columns

        Returns:
            2D list representing table
        """
        table = [["" for _ in range(col_count)] for _ in range(row_count)]

        for cell in cells:
            row_idx = cell.get("rowIndex", 0)
            col_idx = cell.get("columnIndex", 0)
            content = cell.get("content", "")

            if row_idx < row_count and col_idx < col_count:
                table[row_idx][col_idx] = content

        return table

    def _group_and_sum(self, records: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Group records by year and code, sum amounts

        Args:
            records: List of income records

        Returns:
            Nested dictionary: {year: {code: {name, total}}}
        """
        self.logger.info(f"Grouping and summing {len(records)} records")
        grouped = defaultdict(lambda: defaultdict(lambda: {"name": "", "total": 0.0}))

        for idx, record in enumerate(records):
            year = record["year"]
            code = record["code"]
            amount = record["amount"]
            code_name = record["code_name"]

            grouped[year][code]["total"] += amount
            if code_name and not grouped[year][code]["name"]:
                grouped[year][code]["name"] = code_name

            self.logger.debug(f"Record {idx}: {year}/{code} += {amount} (total now: {grouped[year][code]['total']})")

        # Convert to regular dict for JSON serialization
        result = {}
        for year in sorted(grouped.keys()):
            result[year] = {}
            year_total = 0.0

            for code in sorted(grouped[year].keys()):
                result[year][code] = {
                    "name": grouped[year][code]["name"],
                    "amount": round(grouped[year][code]["total"], 2)
                }
                year_total += grouped[year][code]["total"]

            # Add year total
            result[year]["_total"] = round(year_total, 2)
            self.logger.info(f"Year {year} total: {result[year]['_total']} грн ({len([k for k in result[year].keys() if k != '_total'])} codes)")

        return result

    def _create_summary(self, grouped_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create summary statistics

        Args:
            grouped_data: Grouped income data

        Returns:
            Summary dictionary
        """
        total_years = len(grouped_data)
        total_amount = sum(year_data.get("_total", 0) for year_data in grouped_data.values())

        return {
            "total_years": total_years,
            "total_amount": round(total_amount, 2),
            "years": list(grouped_data.keys())
        }

    def format_for_bitrix(self, parsed_data: Dict[str, Any]) -> str:
        """
        Format parsed data as HTML table for Bitrix24 timeline

        Args:
            parsed_data: Parsed income data

        Returns:
            HTML formatted string
        """
        try:
            if not parsed_data.get("success"):
                return f"<p style='color: red;'>Помилка обробки документа: {parsed_data.get('error', 'Unknown error')}</p>"

            data = parsed_data.get("data", {})
            summary = parsed_data.get("summary", {})

            if not data:
                return "<p>Не знайдено даних про доходи у документі</p>"

            # Build HTML table
            html = "<div style='font-family: Arial, sans-serif;'>"
            html += "<h3>Аналіз справки про доходи</h3>"

            # Summary
            html += f"<p><strong>Загальна сума:</strong> {summary.get('total_amount', 0):.2f} грн</p>"
            html += f"<p><strong>Періоди:</strong> {', '.join(summary.get('years', []))}</p>"

            # Table for each year
            for year in sorted(data.keys()):
                year_data = data[year]
                year_total = year_data.get("_total", 0)

                html += f"<h4>{year} рік (Всього: {year_total:.2f} грн)</h4>"
                html += "<table border='1' cellpadding='8' cellspacing='0' style='border-collapse: collapse; width: 100%; margin-bottom: 20px;'>"
                html += "<thead><tr style='background-color: #f0f0f0;'>"
                html += "<th>Код</th><th>Назва</th><th>Сума (грн)</th>"
                html += "</tr></thead><tbody>"

                for code in sorted(year_data.keys()):
                    if code == "_total":
                        continue

                    code_info = year_data[code]
                    html += f"<tr>"
                    html += f"<td>{code}</td>"
                    html += f"<td>{code_info.get('name', '-')}</td>"
                    html += f"<td style='text-align: right;'>{code_info.get('amount', 0):.2f}</td>"
                    html += f"</tr>"

                html += "</tbody></table>"

            html += "</div>"

            return html

        except Exception as e:
            self.logger.error(f"Formatting failed: {e}")
            return f"<p style='color: red;'>Помилка форматування: {str(e)}</p>"

    def to_json(self, parsed_data: Dict[str, Any]) -> str:
        """
        Convert parsed data to JSON string

        Args:
            parsed_data: Parsed income data

        Returns:
            JSON string
        """
        try:
            return json.dumps(parsed_data, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.error(f"JSON conversion failed: {e}")
            return json.dumps({"success": False, "error": str(e)})
