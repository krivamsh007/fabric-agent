
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


__all__ = [
    "MeasureCategory",
    "DAXMeasure",
    "EnterpriseDAXGenerator",
    "EnterpriseDaxGenerator",   # backward-compat alias
]


class MeasureCategory(str, Enum):
    BASE = "Base Metrics"
    TIME_INTELLIGENCE = "Time Intelligence"
    ROLLING = "Rolling Calculations"
    STATISTICAL = "Statistical Analysis"
    CUSTOMER = "Customer Analytics"
    PRODUCT = "Product Analytics"
    FINANCIAL = "Financial Metrics"
    KPI = "KPI & Targets"
    ADVANCED = "Advanced Analytics"


@dataclass
class DAXMeasure:
    name: str
    expression: str
    description: str = ""
    category: MeasureCategory = MeasureCategory.BASE
    format_string: Optional[str] = None
    display_folder: Optional[str] = None
    dependencies: List[str] = field(default_factory=list)
    is_hidden: bool = False

    def to_bim(self) -> Dict[str, Any]:
        m: Dict[str, Any] = {
            "name": self.name,
            "expression": self.expression.strip(),
        }
        if self.description:
            m["description"] = self.description
        if self.format_string:
            m["formatString"] = self.format_string
        if self.display_folder:
            m["displayFolder"] = self.display_folder
        if self.is_hidden:
            m["isHidden"] = True
        return m


class EnterpriseDAXGenerator:
    """
    Enterprise-friendly DAX generator that provides:
      - MeasureCategory enum
      - DAXMeasure dataclass
      - generate_all() for tools/setup_enterprise_demo.py
      - generate_measure_graph() for bootstrap_refactor_enterprise_complex.py
    """

    def __init__(
        self,
        fact_table: str = "FactSales",
        date_table: str = "DimDate",
        product_table: str = "DimProduct",
        customer_table: str = "DimCustomer",
        store_table: str = "DimStore",
        seed: int = 42,
    ):
        self.fact = fact_table
        self.date = date_table
        self.product = product_table
        self.customer = customer_table
        self.store = store_table
        self.seed = seed

    def generate_all(self, target_count: int = 80) -> List[DAXMeasure]:
        """
        Return a reasonably complex measure set with clear dependencies.
        Uses only common DAX functions and classic star-schema dimensions.
        """
        measures: List[DAXMeasure] = []

        # --- Base measures (keep names stable; these become roots) ---
        measures += [
            DAXMeasure(
                "Total Sales",
                f"SUM({self.fact}[NetAmount])",
                "Sum of net sales amount",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Sales",
            ),
            DAXMeasure(
                "Total Quantity",
                f"SUM({self.fact}[Quantity])",
                "Total units sold",
                MeasureCategory.BASE,
                '#,##0',
                "Sales",
            ),
            DAXMeasure(
                "Total Orders",
                f"DISTINCTCOUNT({self.fact}[OrderID])",
                "Distinct order count",
                MeasureCategory.BASE,
                '#,##0',
                "Sales",
            ),
            DAXMeasure(
                "Gross Sales",
                f"SUM({self.fact}[GrossAmount])",
                "Gross sales before discounts",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Sales",
            ),
            DAXMeasure(
                "Total Discount",
                f"SUM({self.fact}[DiscountAmount])",
                "Discount amount",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Sales",
            ),
            DAXMeasure(
                "Total Cost",
                f"SUM({self.fact}[CostAmount])",
                "Cost of goods sold",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Finance",
            ),
            DAXMeasure(
                "Total Profit",
                f"SUM({self.fact}[Profit])",
                "Profit",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Finance",
            ),
            DAXMeasure(
                "Total Shipping",
                f"SUM({self.fact}[ShippingCost])",
                "Shipping cost",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Finance",
            ),
            DAXMeasure(
                "Total Tax",
                f"SUM({self.fact}[TaxAmount])",
                "Tax collected",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Finance",
            ),
            DAXMeasure(
                "Customer Count",
                f"DISTINCTCOUNT({self.fact}[CustomerKey])",
                "Distinct customers",
                MeasureCategory.BASE,
                '#,##0',
                "Customers",
            ),
            DAXMeasure(
                "Return Count",
                f"CALCULATE(COUNTROWS({self.fact}), {self.fact}[IsReturned] = TRUE())",
                "Returned transactions",
                MeasureCategory.BASE,
                '#,##0',
                "Returns",
            ),
            DAXMeasure(
                "Return Quantity",
                f"SUM({self.fact}[ReturnQuantity])",
                "Units returned",
                MeasureCategory.BASE,
                '#,##0',
                "Returns",
            ),
        ]

        # --- Derived (dependencies) ---
        measures += [
            DAXMeasure(
                "Average Order Value",
                "DIVIDE([Total Sales], [Total Orders], 0)",
                "Average sales per order",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Sales\\Averages",
                ["Total Sales", "Total Orders"],
            ),
            DAXMeasure(
                "Average Unit Price",
                "DIVIDE([Total Sales], [Total Quantity], 0)",
                "Average sales per unit",
                MeasureCategory.BASE,
                '"$"#,##0.00',
                "Sales\\Averages",
                ["Total Sales", "Total Quantity"],
            ),
            DAXMeasure(
                "Gross Margin %",
                "DIVIDE([Total Profit], [Total Sales], 0)",
                "Gross margin percent",
                MeasureCategory.FINANCIAL,
                "0.00%",
                "Finance\\Margins",
                ["Total Profit", "Total Sales"],
            ),
            DAXMeasure(
                "Net Margin %",
                "DIVIDE([Total Profit] - [Total Shipping] - [Total Discount], [Total Sales], 0)",
                "Net margin percent",
                MeasureCategory.FINANCIAL,
                "0.00%",
                "Finance\\Margins",
                ["Total Profit", "Total Shipping", "Total Discount", "Total Sales"],
            ),
        ]

        # --- Time intelligence ---
        measures += [
            DAXMeasure(
                "Sales SPLY",
                f"CALCULATE([Total Sales], SAMEPERIODLASTYEAR({self.date}[Date]))",
                "Sales same period last year",
                MeasureCategory.TIME_INTELLIGENCE,
                '"$"#,##0.00',
                "Time\\SPLY",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Sales YoY Growth",
                "DIVIDE([Total Sales] - [Sales SPLY], [Sales SPLY], 0)",
                "YoY sales growth",
                MeasureCategory.TIME_INTELLIGENCE,
                "0.00%",
                "Time\\YoY",
                ["Total Sales", "Sales SPLY"],
            ),
            DAXMeasure(
                "Sales YTD",
                f"CALCULATE([Total Sales], DATESYTD({self.date}[Date]))",
                "Year-to-date sales",
                MeasureCategory.TIME_INTELLIGENCE,
                '"$"#,##0.00',
                "Time\\YTD",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Sales MTD",
                f"CALCULATE([Total Sales], DATESMTD({self.date}[Date]))",
                "Month-to-date sales",
                MeasureCategory.TIME_INTELLIGENCE,
                '"$"#,##0.00',
                "Time\\MTD",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Sales QTD",
                f"CALCULATE([Total Sales], DATESQTD({self.date}[Date]))",
                "Quarter-to-date sales",
                MeasureCategory.TIME_INTELLIGENCE,
                '"$"#,##0.00',
                "Time\\QTD",
                ["Total Sales"],
            ),
        ]

        # --- Rolling windows ---
        measures += [
            DAXMeasure(
                "Sales Rolling 3M",
                f"CALCULATE([Total Sales], DATESINPERIOD({self.date}[Date], MAX({self.date}[Date]), -3, MONTH))",
                "Rolling 3-month sales",
                MeasureCategory.ROLLING,
                '"$"#,##0.00',
                "Rolling\\3M",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Sales Rolling 6M",
                f"CALCULATE([Total Sales], DATESINPERIOD({self.date}[Date], MAX({self.date}[Date]), -6, MONTH))",
                "Rolling 6-month sales",
                MeasureCategory.ROLLING,
                '"$"#,##0.00',
                "Rolling\\6M",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Sales Rolling 12M",
                f"CALCULATE([Total Sales], DATESINPERIOD({self.date}[Date], MAX({self.date}[Date]), -12, MONTH))",
                "Rolling 12-month sales",
                MeasureCategory.ROLLING,
                '"$"#,##0.00',
                "Rolling\\12M",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Sales Moving Avg 30D",
                f"AVERAGEX(DATESINPERIOD({self.date}[Date], MAX({self.date}[Date]), -30, DAY), CALCULATE([Total Sales]))",
                "30-day moving average sales",
                MeasureCategory.ROLLING,
                '"$"#,##0.00',
                "Rolling\\MovingAvg",
                ["Total Sales"],
            ),
        ]

        # --- Customer / Product segmentation (simple but realistic) ---
        measures += [
            DAXMeasure(
                "Enterprise Sales",
                f'CALCULATE([Total Sales], {self.customer}[Segment] = "Enterprise")',
                "Sales for Enterprise segment",
                MeasureCategory.CUSTOMER,
                '"$"#,##0.00',
                "Customer\\Segments",
                ["Total Sales"],
            ),
            DAXMeasure(
                "Enterprise Sales %",
                "DIVIDE([Enterprise Sales], [Total Sales], 0)",
                "Enterprise sales percent of total",
                MeasureCategory.CUSTOMER,
                "0.00%",
                "Customer\\Segments",
                ["Enterprise Sales", "Total Sales"],
            ),
            DAXMeasure(
                "Category Sales %",
                f"DIVIDE([Total Sales], CALCULATE([Total Sales], ALL({self.product}[Category])), 0)",
                "Sales % within category context",
                MeasureCategory.PRODUCT,
                "0.00%",
                "Product\\Category",
                ["Total Sales"],
            ),
        ]

        # --- KPI layer ---
        measures += [
            DAXMeasure(
                "Sales Target",
                "1000000",
                "Static sales target for demo",
                MeasureCategory.KPI,
                '"$"#,##0.00',
                "KPI\\Targets",
            ),
            DAXMeasure(
                "Sales vs Target",
                "[Total Sales] - [Sales Target]",
                "Variance to target",
                MeasureCategory.KPI,
                '"$"#,##0.00',
                "KPI\\Targets",
                ["Total Sales", "Sales Target"],
            ),
            DAXMeasure(
                "Sales Target Attainment",
                "DIVIDE([Total Sales], [Sales Target], 0)",
                "% of target achieved",
                MeasureCategory.KPI,
                "0.00%",
                "KPI\\Targets",
                ["Total Sales", "Sales Target"],
            ),
        ]

        # --- Fill to target_count with a dependency-heavy chain ---
        # This gives you stress-test depth for rename dependency mapping.
        i = 1
        while len(measures) < target_count:
            a = measures[-1].name
            b = measures[-2].name if len(measures) > 1 else "Total Sales"
            name = f"Derived Metric {i:03d}"
            expr = f"VAR x = [{a}] VAR y = [{b}] RETURN x + (0.1 * y)"
            measures.append(
                DAXMeasure(
                    name,
                    expr,
                    "Auto-generated derived metric (dependency stress test)",
                    MeasureCategory.ADVANCED,
                    None,
                    "Advanced\\Derived",
                    [a, b],
                )
            )
            i += 1

        return measures

    def generate_measure_graph(self, measure_count: int = 60, base_table: str = "FactSales") -> List[Dict[str, str]]:
        """
        Backward-compatible helper expected by your bootstrap script.
        Returns: [{"name": "...", "expression": "..."}, ...]
        """
        # Respect requested count
        measures = self.generate_all(target_count=max(30, measure_count))
        measures = measures[:measure_count]
        return [{"name": m.name, "expression": m.expression} for m in measures]


# Backward-compat alias (your bootstrap script imports EnterpriseDaxGenerator)
EnterpriseDaxGenerator = EnterpriseDAXGenerator
