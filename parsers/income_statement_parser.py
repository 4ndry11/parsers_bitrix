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

        The document structure:
        - Text at the beginning
        - Main table after "період" keyword
        - Table ends before "Дата форматування"
        - Table may be split across multiple pages (Azure sees as multiple tables)

        We need to:
        1. Find all table parts between "період" and "Дата форматування"
        2. Merge them into one logical table
        3. Extract year, income code, and accrued amount
        4. Group and sum by year and code

        Args:
            azure_result: Result from Azure Document Intelligence

        Returns:
            Parsed data structured by year and code
        """
        try:
            self.validate_result(azure_result)

            content = azure_result["analyzeResult"]["content"]
            tables = azure_result["analyzeResult"].get("tables", [])
            pages = azure_result["analyzeResult"].get("pages", [])

            self.logger.info(f"Parsing document with {len(tables)} tables, {len(pages)} pages")
            self.logger.info(f"Content length: {len(content)} chars")

            # Find table boundaries in content
            period_pos = content.lower().find("період")
            date_format_pos = content.lower().find("дата форматування")

            self.logger.info(f"Found 'період' at position: {period_pos}")
            self.logger.info(f"Found 'Дата форматування' at position: {date_format_pos}")

            # Filter tables that are between these markers
            relevant_tables = self._filter_relevant_tables(tables, content, period_pos, date_format_pos)

            self.logger.info(f"Found {len(relevant_tables)} relevant tables out of {len(tables)}")

            # Extract data from relevant tables (they are parts of one table)
            income_data = self._extract_from_merged_tables(relevant_tables)

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
            self.logger.error(f"Parsing failed: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "data": {}
            }

    def _filter_relevant_tables(self, tables: List[Dict], content: str, period_pos: int, date_format_pos: int) -> List[Dict]:
        """
        Filter tables that are between 'період' and 'Дата форматування'
        These tables are parts of the main income table split across pages

        Args:
            tables: All tables from Azure DI
            content: Full document content
            period_pos: Position of 'період' in content
            date_format_pos: Position of 'Дата форматування' in content

        Returns:
            List of relevant table parts
        """
        if period_pos == -1:
            self.logger.warning("'період' not found in content, using all tables")
            return tables

        relevant = []

        for idx, table in enumerate(tables):
            cells = table.get("cells", [])
            if not cells:
                self.logger.warning(f"Table {idx} has no cells")
                continue

            # Try to find offset from spans
            cells_with_spans = [cell for cell in cells if cell.get("spans") and len(cell.get("spans", [])) > 0]

            if not cells_with_spans:
                # Fallback: use bounding regions
                self.logger.info(f"Table {idx}: No spans found, using bounding regions")
                bounding_regions = table.get("boundingRegions", [])
                if bounding_regions:
                    page_number = bounding_regions[0].get("pageNumber", 1)
                    self.logger.info(f"Table {idx}: on page {page_number}")
                    # After okres always use all tables as relevant
                    if period_pos > 0:
                        relevant.append(table)
                        self.logger.info(f"Table {idx} is relevant (no offset data, assuming relevant)")
                continue

            # Find min and max offset in table cells
            min_offset = min(cell["spans"][0].get("offset", 999999) for cell in cells_with_spans)
            max_offset = max(cell["spans"][0].get("offset", 0) for cell in cells_with_spans)

            self.logger.info(f"Table {idx}: offset range {min_offset} - {max_offset}")

            # Table is relevant if it's after 'період' and before 'Дата форматування' (or end of doc)
            if min_offset > period_pos:
                if date_format_pos == -1 or max_offset < date_format_pos:
                    relevant.append(table)
                    self.logger.info(f"Table {idx} is relevant (between markers)")
                else:
                    self.logger.info(f"Table {idx} is after 'Дата форматування', skipping")
            else:
                self.logger.info(f"Table {idx} is before 'період', skipping")

        return relevant

    def _extract_from_merged_tables(self, tables: List[Dict]) -> List[Dict[str, Any]]:
        """
        Extract income data from multiple table parts that form one logical table

        The tables are parts of the same table split across pages, so:
        - First table has header row
        - Other tables continue with data rows
        - Column structure is the same across all parts

        Args:
            tables: List of table parts

        Returns:
            List of records with year, code, and amount
        """
        if not tables:
            self.logger.warning("No tables to extract from")
            return []

        records = []
        col_year = None
        col_amount = None
        col_code = None
        header_found = False

        self.logger.info(f"Processing {len(tables)} table parts as one merged table")

        for table_idx, table in enumerate(tables):
            cells = table.get("cells", [])
            if not cells:
                self.logger.warning(f"Table part {table_idx} has no cells")
                continue

            row_count = table["rowCount"]
            col_count = table["columnCount"]
            self.logger.info(f"Table part {table_idx}: {row_count} rows x {col_count} columns")

            # Build table structure
            table_data = self._build_table_structure(cells, row_count, col_count)

            # Log first few rows
            self.logger.info(f"Table part {table_idx} first 3 rows:")
            for row_idx in range(min(3, len(table_data))):
                row_preview = table_data[row_idx][:min(15, len(table_data[row_idx]))]
                self.logger.info(f"  Row {row_idx}: {row_preview}")

            # Find column indices only in first table or if not found yet
            if not header_found:
                for row_idx in range(min(5, len(table_data))):
                    row = table_data[row_idx]
                    for col_idx, cell_content in enumerate(row):
                        if cell_content and "рік" in cell_content.lower():
                            col_year = col_idx
                            self.logger.info(f"Found 'рік' at column {col_idx}: '{cell_content}'")
                            header_found = True
                        if cell_content and "нарахованого" in cell_content.lower():
                            col_amount = col_idx
                            self.logger.info(f"Found 'нарахованого' at column {col_idx}: '{cell_content}'")
                        if cell_content and "код" in cell_content.lower() and "ознаки" in cell_content.lower():
                            col_code = col_idx
                            self.logger.info(f"Found 'код...ознаки' at column {col_idx}: '{cell_content}'")

            if col_year is None or col_amount is None or col_code is None:
                self.logger.warning(f"Table part {table_idx}: Columns not identified yet (Year:{col_year}, Amount:{col_amount}, Code:{col_code})")
                # Continue to next table part, maybe it has header
                continue

            self.logger.info(f"Using columns - Year: {col_year}, Amount: {col_amount}, Code: {col_code}")

            # Extract data rows
            start_row = 1 if table_idx == 0 else 0  # Skip header only in first table
            rows_extracted = 0

            for row_idx in range(start_row, len(table_data)):
                row = table_data[row_idx]

                try:
                    year_cell = row[col_year] if col_year < len(row) else ""
                    amount_cell = row[col_amount] if col_amount < len(row) else ""
                    code_cell = row[col_code] if col_code < len(row) else ""

                    # Skip header rows in continuation tables
                    if "рік" in year_cell.lower() or "період" in year_cell.lower():
                        self.logger.debug(f"Row {row_idx}: Skipping header row")
                        continue

                    # Skip empty rows
                    if not year_cell.strip() and not amount_cell.strip() and not code_cell.strip():
                        continue

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

                    # Extract code (3 digits)
                    code_match = re.search(r'\b(\d{3})\b', code_cell)
                    if not code_match:
                        self.logger.debug(f"Row {row_idx}: No code found in '{code_cell}'")
                        continue

                    code = code_match.group(1)

                    # Extract code name if present
                    code_name_match = re.search(r'\d{3}\s*[-–—]\s*(.+)', code_cell)
                    code_name = code_name_match.group(1).strip() if code_name_match else ""

                    self.logger.info(f"Table {table_idx} Row {row_idx}: Year={year}, Code={code}, Amount={amount}, Name={code_name}")

                    records.append({
                        "year": year,
                        "code": code,
                        "code_name": code_name,
                        "amount": amount
                    })
                    rows_extracted += 1

                except Exception as e:
                    self.logger.debug(f"Table {table_idx} Row {row_idx}: Skipping - {e}")
                    continue

            self.logger.info(f"Table part {table_idx}: Extracted {rows_extracted} records")

        self.logger.info(f"Total extracted {len(records)} records from {len(tables)} table parts")
        return records

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
        Format parsed data as simple text for Bitrix24 timeline

        Args:
            parsed_data: Parsed income data

        Returns:
            Text formatted string
        """
        try:
            if not parsed_data.get("success"):
                return f"❌ Помилка обробки документа: {parsed_data.get('error', 'Unknown error')}"

            data = parsed_data.get("data", {})
            summary = parsed_data.get("summary", {})

            if not data:
                return "⚠️ Не знайдено даних про доходи у документі"

            # Build simple text output
            output = []
            output.append("📊 АНАЛІЗ СПРАВКИ ПРО ДОХОДИ")
            output.append("")
            output.append(f"💰 Загальна сума: {summary.get('total_amount', 0):.2f} грн")
            output.append(f"📅 Періоди: {', '.join(summary.get('years', []))}")
            output.append("")

            # Data for each year
            for year in sorted(data.keys()):
                year_data = data[year]
                year_total = year_data.get("_total", 0)

                output.append("─────────")
                output.append(f"📆 {year} рік • Всього: {year_total:.2f} грн")
                output.append("")

                for code in sorted(year_data.keys()):
                    if code == "_total":
                        continue

                    code_info = year_data[code]
                    name = code_info.get('name', '-')
                    amount = code_info.get('amount', 0)

                    output.append(f"🔹 Код {code}: {name}")
                    output.append(f"   Сума: {amount:.2f} грн")
                    output.append("")

            return "\n".join(output)

        except Exception as e:
            self.logger.error(f"Formatting failed: {e}")
            return f"❌ Помилка форматування: {str(e)}"

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
