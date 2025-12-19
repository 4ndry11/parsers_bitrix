"""
Income Statement Parser
Parses Ukrainian income statement (Справка про доходи)
"""
import re
import json
from typing import Dict, Any, List, Tuple, Optional
from collections import defaultdict
from .base_parser import BaseParser
from utils.logger import setup_logger

logger = setup_logger("IncomeStatementParser")


class IncomeStatementParser(BaseParser):
    """Parser for Ukrainian income statement documents"""

    def __init__(self):
        super().__init__()
        self.logger.info("IncomeStatementParser initialized")

    # ========== Helper Methods ==========

    def _get_cell_value(self, cell: Any, key: str, default: Any = None) -> Any:
        """Get value from cell (supports both dict and object)"""
        if hasattr(cell, key):
            return getattr(cell, key)
        if isinstance(cell, dict):
            return cell.get(key, default)
        return default

    def _table_to_grid(self, table: Any) -> Tuple[Dict, Dict]:
        """
        Convert table to grid structure
        Returns:
            grid: dict[(row_idx, col_idx)] = text
            rows: dict[row_idx] = dict[col_idx]=text
        """
        # Get cells from table
        if hasattr(table, "cells"):
            cells = table.cells
        else:
            cells = table.get("cells", [])

        grid = {}
        rows = defaultdict(dict)

        for cell in cells:
            row_idx = self._get_cell_value(cell, "rowIndex", 0)
            col_idx = self._get_cell_value(cell, "columnIndex", 0)
            content = self._get_cell_value(cell, "content", "") or ""
            content = str(content).strip()

            grid[(row_idx, col_idx)] = content
            rows[row_idx][col_idx] = content

        return grid, rows

    def _find_index_row_and_cols(self, rows: Dict) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        """
        Find row where col[0]='1' and locate columns '4', '7', '13'
        Returns: (index_row, col_year, col_amount, col_code) or (None, None, None, None)
        """
        for row_idx in sorted(rows.keys()):
            if rows[row_idx].get(0, "").strip() != "1":
                continue

            # Found the index row, now find columns
            col_year = None
            col_amount = None
            col_code = None

            for col_idx, value in rows[row_idx].items():
                value_str = str(value).strip()
                if value_str == "4":
                    col_year = col_idx
                elif value_str == "7":
                    col_amount = col_idx
                elif value_str == "13":
                    col_code = col_idx

            if col_year is not None and col_amount is not None and col_code is not None:
                return row_idx, col_year, col_amount, col_code

        return None, None, None, None

    def _extract_rows_for_processing(self, table_idx: int, table: Any, fallback_cols: Optional[Tuple] = None) -> Tuple[List[Dict], Optional[Tuple]]:
        """
        Extract rows from table, cutting multi-level header if found
        Returns: (list of row dicts, column indices tuple)
        """
        _, rows = self._table_to_grid(table)
        index_row, col_year, col_amount, col_code = self._find_index_row_and_cols(rows)

        # If index row not found in this table, use fallback columns from table 1
        if index_row is None:
            if not fallback_cols:
                # No column info - return raw data
                out = []
                for row_idx in sorted(rows.keys()):
                    out.append({
                        "table_idx": table_idx,
                        "row_idx": row_idx,
                        "raw_row": rows[row_idx]
                    })
                return out, None

            col_year, col_amount, col_code = fallback_cols
            clean_row_ids = sorted(rows.keys())
        else:
            # Take only rows AFTER index row
            clean_row_ids = [r for r in sorted(rows.keys()) if r > index_row]

        # Extract data from rows
        out = []
        for row_idx in clean_row_ids:
            row = rows[row_idx]
            out.append({
                "table_idx": table_idx,
                "row_idx": row_idx,
                "year_cell": row.get(col_year, "") if col_year is not None else "",
                "amount_cell": row.get(col_amount, "") if col_amount is not None else "",
                "code_cell": row.get(col_code, "") if col_code is not None else "",
                "raw_row": row
            })

        return out, (col_year, col_amount, col_code)

    def parse(self, azure_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse income statement from Azure DI result

        New logic:
        1. Table 0 - skip (document header)
        2. Table 1 - find row where col[0]='1', cut everything above
        3. Tables 2+ - use column indices from table 1
        4. Filter out "Всього" rows (save for verification)
        5. Extract and group data

        Args:
            azure_result: Result from Azure Document Intelligence

        Returns:
            Parsed data structured by year and code
        """
        try:
            self.validate_result(azure_result)

            tables = azure_result["analyzeResult"].get("tables", [])
            self.logger.info(f"Parsing document with {len(tables)} tables")

            if len(tables) <= 1:
                self.logger.error("Not enough tables (need at least 2)")
                return {
                    "success": False,
                    "error": "Недостаточно таблиц в документе",
                    "data": {}
                }

            all_rows = []
            cols = None

            # Table 0 - skip (document header)
            self.logger.info("Table 0: Skipped (document header)")

            # Table 1 - find index row and cut header
            rows_1, cols = self._extract_rows_for_processing(1, tables[1], fallback_cols=None)

            if cols is None:
                raise RuntimeError("Cannot find index row in Table 1 (col[0]='1' + '4','7','13')")

            self.logger.info(f"Table 1: Processed, header cut. Columns: year={cols[0]}, amount={cols[1]}, code={cols[2]}")
            all_rows.extend(rows_1)

            # Tables 2+ - use columns from table 1
            for table_idx in range(2, len(tables)):
                rows_i, _ = self._extract_rows_for_processing(table_idx, tables[table_idx], fallback_cols=cols)
                all_rows.extend(rows_i)
                self.logger.info(f"Table {table_idx}: Added {len(rows_i)} rows")

            # Parse data from rows
            records, totals = self._parse_rows_data(all_rows)

            self.logger.info(f"Extracted {len(records)} records and {len(totals)} 'Всього' rows")

            # Group and sum
            grouped_data = self._group_and_sum(records)

            # Verify with totals
            verification = self._verify_with_totals(grouped_data, totals)

            result = {
                "success": True,
                "data": grouped_data,
                "summary": self._create_summary(grouped_data),
                "verification": verification
            }

            self.logger.info(f"Parsing completed. Found {len(grouped_data)} years")
            return result

        except Exception as e:
            self.logger.error(f"Parsing failed: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "data": {}
            }

    def _parse_rows_data(self, all_rows: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Parse rows data into records and totals
        Returns: (records list, totals list)
        """
        records = []
        totals = []

        for row_data in all_rows:
            year_cell = row_data.get("year_cell", "")
            amount_cell = row_data.get("amount_cell", "")
            code_cell = row_data.get("code_cell", "")
            raw_row = row_data.get("raw_row", {})

            # Check if this is a "Всього" row
            is_total = any("всього" in str(val).lower() for val in raw_row.values())

            if is_total:
                # Extract for verification
                year_match = re.search(r'\b(20\d{2})\b', year_cell)
                if not year_match:
                    continue
                year = year_match.group(1)

                amount_clean = amount_cell.replace(' ', '').replace(',', '.')
                amount_match = re.search(r'(\d+\.?\d*)', amount_clean)
                if not amount_match:
                    continue
                amount = float(amount_match.group(1))

                totals.append({
                    "year": year,
                    "amount": amount
                })
                continue

            # Extract regular data
            year_match = re.search(r'\b(20\d{2})\b', year_cell)
            if not year_match:
                continue
            year = year_match.group(1)

            amount_clean = amount_cell.replace(' ', '').replace(',', '.')
            amount_match = re.search(r'(\d+\.?\d*)', amount_clean)
            if not amount_match:
                continue
            amount = float(amount_match.group(1))

            code_match = re.search(r'\b(\d{3})\b', code_cell)
            if not code_match:
                continue
            code = code_match.group(1)

            name_match = re.search(r'\d{3}\s*-?\s*(.+)', code_cell)
            name = name_match.group(1).strip() if name_match else ""

            records.append({
                "year": year,
                "code": code,
                "code_name": name,
                "amount": amount
            })

        return records, totals

    def _verify_with_totals(self, grouped_data: Dict, totals: List[Dict]) -> Dict:
        """
        Verify grouped data with 'Всього' rows
        Returns verification results
        """
        verification = {
            "matches": [],
            "mismatches": [],
            "total_match": False
        }

        # Verify by year
        for year in sorted(grouped_data.keys()):
            our_total = grouped_data[year].get("_total", 0)

            year_totals = [t for t in totals if t["year"] == year]
            if year_totals:
                expected = year_totals[0]["amount"]
                diff = abs(our_total - expected)

                if diff < 1.0:
                    verification["matches"].append({
                        "year": year,
                        "our_total": our_total,
                        "expected": expected
                    })
                else:
                    verification["mismatches"].append({
                        "year": year,
                        "our_total": our_total,
                        "expected": expected,
                        "diff": diff
                    })

        # Verify grand total
        our_grand_total = sum(year_data.get("_total", 0) for year_data in grouped_data.values())
        expected_grand_total = sum(t["amount"] for t in totals)

        verification["our_grand_total"] = our_grand_total
        verification["expected_grand_total"] = expected_grand_total
        verification["total_diff"] = abs(our_grand_total - expected_grand_total)
        verification["total_match"] = verification["total_diff"] < 1.0

        return verification

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
            verification = parsed_data.get("verification", {})

            if not data:
                return "⚠️ Не знайдено даних про доходи у документі"

            # Build simple text output
            output = []
            output.append("📊 АНАЛІЗ СПРАВКИ ПРО ДОХОДИ")
            output.append("")
            output.append(f"💰 Загальна сума: {summary.get('total_amount', 0):.2f} грн")
            output.append(f"📅 Періоди: {', '.join(summary.get('years', []))}")

            # Verification status
            if verification.get("total_match"):
                output.append("✅ Сверка з 'Всього': СОВПАДАЕТ")
            else:
                output.append(f"⚠️ Сверка з 'Всього': РАСХОЖДЕНИЕ {verification.get('total_diff', 0):.2f} грн")

            output.append("")

            # Data for each year
            for year in sorted(data.keys()):
                year_data = data[year]
                year_total = year_data.get("_total", 0)

                output.append("─────────")
                output.append(f"📆 {year} рік • Всього: {year_total:.2f} грн")

                # Year verification
                year_match = [m for m in verification.get("matches", []) if m["year"] == year]
                year_mismatch = [m for m in verification.get("mismatches", []) if m["year"] == year]

                if year_match:
                    output.append(f"   ✅ Сверка: {year_match[0]['expected']:.2f} грн")
                elif year_mismatch:
                    output.append(f"   ⚠️ Сверка: {year_mismatch[0]['expected']:.2f} грн (різниця {year_mismatch[0]['diff']:.2f} грн)")

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
