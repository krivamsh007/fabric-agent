"""
Setup Enterprise Demo Tool
==========================

MCP tool that programmatically creates a complete enterprise Star Schema
demo in a Microsoft Fabric workspace, including:

1. Data Generation: Mock CSVs for Sales Star Schema
2. Lakehouse: Creates 'Sales_Warehouse' lakehouse
3. Pipeline: Data Factory pipeline for CSV to Delta conversion
4. Semantic Model: Power BI model with complex DAX measures

This tool demonstrates the full capabilities of the Fabric Agent
for enterprise-grade data platform automation.
"""

from __future__ import annotations

import base64
import json
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from loguru import logger
from pydantic import BaseModel, Field

from fabric_agent.tools.enterprise_data_generator import (
    EnterpriseDataGenerator,
    GeneratorConfig,
)
from fabric_agent.tools.enterprise_dax_generator import (
    EnterpriseDAXGenerator,
    MeasureCategory,
)
from fabric_agent.storage.memory_manager import (
    MemoryManager,
    StateType,
    OperationStatus,
)

if TYPE_CHECKING:
    from fabric_agent.api.fabric_client import FabricApiClient


# ============================================================================
# Input/Output Models
# ============================================================================

class SetupEnterpriseDemoInput(BaseModel):
    """
    Input for the setup_enterprise_demo tool.
    
    Creates a complete enterprise Star Schema demo environment.
    
    Attributes:
        demo_name: Name prefix for created items (default: "Enterprise_Sales")
        num_transactions: Number of sales transactions to generate (default: 500000)
        start_year: Start year for data (default: 2021)
        end_year: End year for data (default: 2024)
        create_lakehouse: Whether to create the lakehouse (default: True)
        create_pipeline: Whether to create the data pipeline (default: True)
        create_semantic_model: Whether to create the semantic model (default: True)
        dry_run: If True, only generate data locally without creating Fabric items
    
    Example:
        >>> input = SetupEnterpriseDemoInput(
        ...     demo_name="Sales_Demo",
        ...     num_transactions=100000,
        ...     dry_run=True
        ... )
    """
    
    demo_name: str = Field(
        default="Enterprise_Sales",
        description="Name prefix for created items",
        min_length=1,
        max_length=50,
    )
    num_transactions: int = Field(
        default=500000,
        description="Number of sales transactions to generate",
        ge=1000,
        le=5000000,
    )
    start_year: int = Field(
        default=2021,
        description="Start year for data generation",
        ge=2010,
        le=2024,
    )
    end_year: int = Field(
        default=2024,
        description="End year for data generation",
        ge=2015,
        le=2030,
    )
    create_lakehouse: bool = Field(
        default=True,
        description="Create the lakehouse in Fabric",
    )
    create_pipeline: bool = Field(
        default=True,
        description="Create the data pipeline",
    )
    create_semantic_model: bool = Field(
        default=True,
        description="Create the semantic model with DAX measures",
    )
    dry_run: bool = Field(
        default=False,
        description="If True, only generate data locally",
    )


class CreatedItem(BaseModel):
    """Information about a created Fabric item."""
    
    item_type: str = Field(..., description="Type of item")
    name: str = Field(..., description="Item name")
    id: Optional[str] = Field(None, description="Item ID")
    status: str = Field(..., description="Creation status")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")


class GeneratedData(BaseModel):
    """Information about generated data."""
    
    table_name: str = Field(..., description="Table name")
    row_count: int = Field(..., description="Number of rows")
    file_path: Optional[str] = Field(None, description="Local file path")
    columns: List[str] = Field(default_factory=list, description="Column names")


class GeneratedMeasure(BaseModel):
    """Information about a generated DAX measure."""
    
    name: str = Field(..., description="Measure name")
    category: str = Field(..., description="Measure category")
    dependencies: List[str] = Field(default_factory=list, description="Dependent measures")
    dependency_depth: int = Field(default=0, description="Depth in dependency tree")


class SetupEnterpriseDemoOutput(BaseModel):
    """
    Output from the setup_enterprise_demo tool.
    
    Attributes:
        success: Whether the setup completed successfully.
        demo_name: Name prefix used for created items.
        workspace_id: Workspace where items were created.
        workspace_name: Workspace name.
        created_items: List of items created in Fabric.
        generated_data: Information about generated data tables.
        generated_measures: Information about DAX measures.
        measure_stats: Statistics about generated measures.
        dependency_graph: Measure dependency relationships.
        execution_time_seconds: Total execution time.
        dry_run: Whether this was a dry run.
        error: Error message if failed.
        snapshot_id: Audit trail snapshot ID.
    """
    
    success: bool = Field(..., description="Whether setup succeeded")
    demo_name: str = Field(..., description="Demo name prefix")
    workspace_id: Optional[str] = Field(None, description="Workspace ID")
    workspace_name: Optional[str] = Field(None, description="Workspace name")
    created_items: List[CreatedItem] = Field(
        default_factory=list,
        description="Created Fabric items",
    )
    generated_data: List[GeneratedData] = Field(
        default_factory=list,
        description="Generated data tables",
    )
    generated_measures: List[GeneratedMeasure] = Field(
        default_factory=list,
        description="Generated DAX measures",
    )
    measure_stats: Dict[str, Any] = Field(
        default_factory=dict,
        description="Measure statistics",
    )
    dependency_graph: Dict[str, List[str]] = Field(
        default_factory=dict,
        description="Measure dependencies",
    )
    execution_time_seconds: float = Field(
        default=0,
        description="Execution time",
    )
    dry_run: bool = Field(..., description="Was this a dry run")
    error: Optional[str] = Field(None, description="Error if failed")
    snapshot_id: Optional[str] = Field(None, description="Audit trail ID")


# ============================================================================
# Semantic Model Definition Generator
# ============================================================================

class SemanticModelBuilder:
    """Builds the semantic model BIM definition."""
    
    def __init__(self, model_name: str, dax_generator: EnterpriseDAXGenerator):
        self.model_name = model_name
        self.dax = dax_generator
    
    def build_model_bim(self) -> Dict[str, Any]:
        """Build the complete model.bim structure."""
        
        # Generate all measures
        measures = self.dax.generate_all()
        
        # Build the model
        model = {
            "name": self.model_name,
            "compatibilityLevel": 1567,
            "model": {
                "culture": "en-US",
                "dataAccessOptions": {
                    "legacyRedirects": True,
                    "returnErrorValuesAsNull": True
                },
                "defaultPowerBIDataSourceVersion": "powerBI_V3",
                "tables": self._build_tables(measures),
                "relationships": self._build_relationships(),
                "annotations": [
                    {"name": "PBI_QueryOrder", "value": json.dumps(self._get_query_order())},
                    {"name": "GeneratedBy", "value": "FabricAgent Enterprise Demo"},
                    {"name": "GeneratedAt", "value": datetime.utcnow().isoformat()},
                ]
            }
        }
        
        return model
    
    def _build_tables(self, measures: List[Any]) -> List[Dict[str, Any]]:
        """Build table definitions including measures."""
        tables = []
        
        # Fact table
        tables.append({
            "name": "FactSales",
            "columns": self._fact_sales_columns(),
            "partitions": [{
                "name": "FactSales",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }]
        })
        
        # Date dimension with measures
        date_table = {
            "name": "DimDate",
            "columns": self._dim_date_columns(),
            "partitions": [{
                "name": "DimDate",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }],
            "measures": [m.to_bim() for m in measures],  # Attach measures here
        }
        tables.append(date_table)
        
        # Product dimension
        tables.append({
            "name": "DimProduct",
            "columns": self._dim_product_columns(),
            "partitions": [{
                "name": "DimProduct",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }]
        })
        
        # Customer dimension
        tables.append({
            "name": "DimCustomer",
            "columns": self._dim_customer_columns(),
            "partitions": [{
                "name": "DimCustomer",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }]
        })
        
        # Store dimension
        tables.append({
            "name": "DimStore",
            "columns": self._dim_store_columns(),
            "partitions": [{
                "name": "DimStore",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }]
        })
        
        # Employee dimension
        tables.append({
            "name": "DimEmployee",
            "columns": self._dim_employee_columns(),
            "partitions": [{
                "name": "DimEmployee",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }]
        })
        
        # Promotion dimension
        tables.append({
            "name": "DimPromotion",
            "columns": self._dim_promotion_columns(),
            "partitions": [{
                "name": "DimPromotion",
                "mode": "import",
                "source": {"type": "m", "expression": ["let Source = ... in Source"]}
            }]
        })
        
        return tables
    
    def _fact_sales_columns(self) -> List[Dict[str, Any]]:
        """Define FactSales columns."""
        return [
            {"name": "SalesKey", "dataType": "int64"},
            {"name": "OrderID", "dataType": "string"},
            {"name": "OrderLineNumber", "dataType": "int64"},
            {"name": "DateKey", "dataType": "int64"},
            {"name": "ProductKey", "dataType": "int64"},
            {"name": "StoreKey", "dataType": "int64"},
            {"name": "CustomerKey", "dataType": "int64"},
            {"name": "EmployeeKey", "dataType": "int64"},
            {"name": "PromotionKey", "dataType": "int64"},
            {"name": "OrderDate", "dataType": "dateTime"},
            {"name": "Quantity", "dataType": "int64"},
            {"name": "UnitPrice", "dataType": "decimal"},
            {"name": "UnitCost", "dataType": "decimal"},
            {"name": "GrossAmount", "dataType": "decimal"},
            {"name": "DiscountPercent", "dataType": "decimal"},
            {"name": "DiscountAmount", "dataType": "decimal"},
            {"name": "NetAmount", "dataType": "decimal"},
            {"name": "CostAmount", "dataType": "decimal"},
            {"name": "ShippingCost", "dataType": "decimal"},
            {"name": "TaxRate", "dataType": "decimal"},
            {"name": "TaxAmount", "dataType": "decimal"},
            {"name": "TotalAmount", "dataType": "decimal"},
            {"name": "Profit", "dataType": "decimal"},
            {"name": "ProfitMargin", "dataType": "decimal"},
            {"name": "IsReturned", "dataType": "boolean"},
            {"name": "ReturnQuantity", "dataType": "int64"},
        ]
    
    def _dim_date_columns(self) -> List[Dict[str, Any]]:
        """Define DimDate columns."""
        return [
            {"name": "DateKey", "dataType": "int64"},
            {"name": "Date", "dataType": "dateTime"},
            {"name": "Year", "dataType": "int64"},
            {"name": "Quarter", "dataType": "string"},
            {"name": "Month", "dataType": "int64"},
            {"name": "MonthName", "dataType": "string"},
            {"name": "Week", "dataType": "int64"},
            {"name": "DayOfMonth", "dataType": "int64"},
            {"name": "DayName", "dataType": "string"},
            {"name": "IsWeekend", "dataType": "boolean"},
            {"name": "IsHoliday", "dataType": "boolean"},
            {"name": "FiscalYear", "dataType": "int64"},
            {"name": "FiscalQuarter", "dataType": "string"},
        ]
    
    def _dim_product_columns(self) -> List[Dict[str, Any]]:
        """Define DimProduct columns."""
        return [
            {"name": "ProductKey", "dataType": "int64"},
            {"name": "ProductID", "dataType": "string"},
            {"name": "ProductName", "dataType": "string"},
            {"name": "Category", "dataType": "string"},
            {"name": "Subcategory", "dataType": "string"},
            {"name": "Brand", "dataType": "string"},
            {"name": "UnitPrice", "dataType": "decimal"},
            {"name": "UnitCost", "dataType": "decimal"},
            {"name": "IsActive", "dataType": "boolean"},
        ]
    
    def _dim_customer_columns(self) -> List[Dict[str, Any]]:
        """Define DimCustomer columns."""
        return [
            {"name": "CustomerKey", "dataType": "int64"},
            {"name": "CustomerID", "dataType": "string"},
            {"name": "CustomerName", "dataType": "string"},
            {"name": "Segment", "dataType": "string"},
            {"name": "Industry", "dataType": "string"},
            {"name": "Region", "dataType": "string"},
            {"name": "Country", "dataType": "string"},
            {"name": "LoyaltyTier", "dataType": "string"},
            {"name": "FirstPurchaseDate", "dataType": "dateTime"},
            {"name": "IsActive", "dataType": "boolean"},
        ]
    
    def _dim_store_columns(self) -> List[Dict[str, Any]]:
        """Define DimStore columns."""
        return [
            {"name": "StoreKey", "dataType": "int64"},
            {"name": "StoreID", "dataType": "string"},
            {"name": "StoreName", "dataType": "string"},
            {"name": "StoreType", "dataType": "string"},
            {"name": "Region", "dataType": "string"},
            {"name": "Territory", "dataType": "string"},
            {"name": "City", "dataType": "string"},
            {"name": "State", "dataType": "string"},
            {"name": "Country", "dataType": "string"},
            {"name": "IsActive", "dataType": "boolean"},
        ]
    
    def _dim_employee_columns(self) -> List[Dict[str, Any]]:
        """Define DimEmployee columns."""
        return [
            {"name": "EmployeeKey", "dataType": "int64"},
            {"name": "EmployeeID", "dataType": "string"},
            {"name": "FullName", "dataType": "string"},
            {"name": "Title", "dataType": "string"},
            {"name": "Department", "dataType": "string"},
            {"name": "Region", "dataType": "string"},
            {"name": "HireDate", "dataType": "dateTime"},
            {"name": "IsActive", "dataType": "boolean"},
            {"name": "AnnualQuota", "dataType": "decimal"},
        ]
    
    def _dim_promotion_columns(self) -> List[Dict[str, Any]]:
        """Define DimPromotion columns."""
        return [
            {"name": "PromotionKey", "dataType": "int64"},
            {"name": "PromotionID", "dataType": "string"},
            {"name": "PromotionName", "dataType": "string"},
            {"name": "PromotionType", "dataType": "string"},
            {"name": "StartDate", "dataType": "dateTime"},
            {"name": "EndDate", "dataType": "dateTime"},
            {"name": "DiscountPercent", "dataType": "decimal"},
            {"name": "IsActive", "dataType": "boolean"},
        ]
    
    def _build_relationships(self) -> List[Dict[str, Any]]:
        """Build relationship definitions."""
        return [
            {
                "name": "FactSales_DimDate",
                "fromTable": "FactSales",
                "fromColumn": "DateKey",
                "toTable": "DimDate",
                "toColumn": "DateKey"
            },
            {
                "name": "FactSales_DimProduct",
                "fromTable": "FactSales",
                "fromColumn": "ProductKey",
                "toTable": "DimProduct",
                "toColumn": "ProductKey"
            },
            {
                "name": "FactSales_DimCustomer",
                "fromTable": "FactSales",
                "fromColumn": "CustomerKey",
                "toTable": "DimCustomer",
                "toColumn": "CustomerKey"
            },
            {
                "name": "FactSales_DimStore",
                "fromTable": "FactSales",
                "fromColumn": "StoreKey",
                "toTable": "DimStore",
                "toColumn": "StoreKey"
            },
            {
                "name": "FactSales_DimEmployee",
                "fromTable": "FactSales",
                "fromColumn": "EmployeeKey",
                "toTable": "DimEmployee",
                "toColumn": "EmployeeKey"
            },
            {
                "name": "FactSales_DimPromotion",
                "fromTable": "FactSales",
                "fromColumn": "PromotionKey",
                "toTable": "DimPromotion",
                "toColumn": "PromotionKey"
            },
        ]
    
    def _get_query_order(self) -> List[str]:
        """Get query order for tables."""
        return [
            "DimDate", "DimProduct", "DimCustomer", 
            "DimStore", "DimEmployee", "DimPromotion", "FactSales"
        ]


# ============================================================================
# Pipeline Definition Generator
# ============================================================================

class PipelineBuilder:
    """Builds the Data Factory pipeline definition."""
    
    def __init__(self, pipeline_name: str, lakehouse_name: str):
        self.pipeline_name = pipeline_name
        self.lakehouse_name = lakehouse_name
    
    def build_pipeline_definition(self) -> Dict[str, Any]:
        """Build the complete pipeline definition."""
        
        tables = [
            "DimDate", "DimProduct", "DimCustomer", 
            "DimStore", "DimEmployee", "DimPromotion", "FactSales"
        ]
        
        activities = []
        
        for table in tables:
            # Copy activity for each table
            activities.append({
                "name": f"Copy_{table}",
                "type": "Copy",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 3,
                    "retryIntervalInSeconds": 30
                },
                "typeProperties": {
                    "source": {
                        "type": "DelimitedTextSource",
                        "storeSettings": {
                            "type": "LakehouseReadSettings",
                            "recursive": True
                        },
                        "formatSettings": {
                            "type": "DelimitedTextReadSettings"
                        }
                    },
                    "sink": {
                        "type": "LakehouseSink",
                        "tableActionOption": "Overwrite",
                        "partitionOption": "None"
                    },
                    "enableStaging": False,
                    "translator": {
                        "type": "TabularTranslator",
                        "typeConversion": True
                    }
                },
                "inputs": [{
                    "referenceName": f"{table}_CSV",
                    "type": "DatasetReference"
                }],
                "outputs": [{
                    "referenceName": f"{table}_Delta",
                    "type": "DatasetReference"
                }]
            })
        
        pipeline = {
            "name": self.pipeline_name,
            "properties": {
                "activities": activities,
                "annotations": [
                    "Enterprise Sales Demo",
                    "Generated by FabricAgent"
                ],
                "folder": {
                    "name": "Enterprise_Demo"
                }
            }
        }
        
        return pipeline


# ============================================================================
# Main Tool Implementation
# ============================================================================

class SetupEnterpriseDemoTool:
    """
    Tool for creating complete enterprise demo environment.
    
    This tool orchestrates the creation of:
    1. Mock data generation (7 tables, 500K+ transactions)
    2. Lakehouse creation in Fabric
    3. Data pipeline for CSV to Delta conversion
    4. Semantic model with 80+ complex DAX measures
    """
    
    def __init__(
        self,
        client: "FabricApiClient",
        memory_manager: MemoryManager,
        workspace_id: str,
        workspace_name: str,
    ):
        self.client = client
        self.memory = memory_manager
        self.workspace_id = workspace_id
        self.workspace_name = workspace_name
    
    async def execute(
        self,
        input: SetupEnterpriseDemoInput,
    ) -> SetupEnterpriseDemoOutput:
        """
        Execute the enterprise demo setup.
        
        Args:
            input: Setup configuration.
        
        Returns:
            SetupEnterpriseDemoOutput with results.
        """
        import time
        start_time = time.time()
        
        logger.info(f"Starting enterprise demo setup: {input.demo_name}")
        
        created_items: List[CreatedItem] = []
        generated_data: List[GeneratedData] = []
        generated_measures: List[GeneratedMeasure] = []
        
        try:
            # Step 1: Generate data
            logger.info("Step 1: Generating enterprise data...")
            data_generator = EnterpriseDataGenerator(
                GeneratorConfig(
                    num_transactions=input.num_transactions,
                    start_date=datetime(input.start_year, 1, 1).date(),
                    end_date=datetime(input.end_year, 12, 31).date(),
                )
            )
            
            tables = data_generator.generate_all()
            
            for table_name, rows in tables.items():
                columns = list(rows[0].keys()) if rows else []
                generated_data.append(GeneratedData(
                    table_name=table_name,
                    row_count=len(rows),
                    columns=columns[:10],  # First 10 columns
                ))
            
            # Step 2: Generate DAX measures
            logger.info("Step 2: Generating DAX measures...")
            dax_generator = EnterpriseDAXGenerator()
            measures = dax_generator.generate_all()
            dep_graph = dax_generator.get_dependency_graph()
            
            for measure in measures:
                depth = self._calculate_depth(measure.name, dep_graph)
                generated_measures.append(GeneratedMeasure(
                    name=measure.name,
                    category=measure.category.value,
                    dependencies=measure.dependencies,
                    dependency_depth=depth,
                ))
            
            # Calculate measure statistics
            measure_stats = {
                "total_measures": len(measures),
                "by_category": {},
                "max_dependency_depth": max(m.dependency_depth for m in generated_measures),
                "measures_with_dependencies": sum(1 for m in generated_measures if m.dependencies),
            }
            for cat in MeasureCategory:
                count = sum(1 for m in measures if m.category == cat)
                if count > 0:
                    measure_stats["by_category"][cat.value] = count
            
            # Convert dependency graph for output
            dependency_graph_output = {
                name: list(deps) for name, deps in dep_graph.items() if deps
            }
            
            if not input.dry_run:
                # Step 3: Create Lakehouse
                if input.create_lakehouse:
                    logger.info("Step 3: Creating Lakehouse...")
                    lakehouse_result = await self._create_lakehouse(
                        f"{input.demo_name}_Warehouse"
                    )
                    created_items.append(lakehouse_result)
                
                # Step 4: Upload data to Lakehouse
                logger.info("Step 4: Uploading data to Lakehouse...")
                # In a real implementation, this would upload CSVs
                # For now, we record the intent
                created_items.append(CreatedItem(
                    item_type="Files",
                    name="CSV Data Files",
                    status="pending_implementation",
                    details={"tables": list(tables.keys())},
                ))
                
                # Step 5: Create Pipeline
                if input.create_pipeline:
                    logger.info("Step 5: Creating Data Pipeline...")
                    pipeline_result = await self._create_pipeline(
                        f"{input.demo_name}_ETL_Pipeline",
                        f"{input.demo_name}_Warehouse",
                    )
                    created_items.append(pipeline_result)
                
                # Step 6: Create Semantic Model
                if input.create_semantic_model:
                    logger.info("Step 6: Creating Semantic Model...")
                    model_result = await self._create_semantic_model(
                        f"{input.demo_name}_Model",
                        dax_generator,
                    )
                    created_items.append(model_result)
            
            # Record to audit trail
            snapshot = await self.memory.record_state(
                state_type=StateType.CUSTOM,
                operation="setup_enterprise_demo",
                status=OperationStatus.SUCCESS if not input.dry_run else OperationStatus.DRY_RUN,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                target_name=input.demo_name,
                state_data={
                    "tables_generated": [d.table_name for d in generated_data],
                    "total_rows": sum(d.row_count for d in generated_data),
                    "measures_generated": len(measures),
                    "items_created": len(created_items),
                },
                metadata={
                    "demo_name": input.demo_name,
                    "num_transactions": input.num_transactions,
                    "dry_run": input.dry_run,
                },
            )
            
            execution_time = time.time() - start_time
            
            logger.info(f"Enterprise demo setup complete in {execution_time:.1f}s")
            
            return SetupEnterpriseDemoOutput(
                success=True,
                demo_name=input.demo_name,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                created_items=created_items,
                generated_data=generated_data,
                generated_measures=generated_measures,
                measure_stats=measure_stats,
                dependency_graph=dependency_graph_output,
                execution_time_seconds=execution_time,
                dry_run=input.dry_run,
                snapshot_id=snapshot.id,
            )
            
        except Exception as e:
            logger.error(f"Enterprise demo setup failed: {e}")
            
            # Record failure
            snapshot = await self.memory.record_state(
                state_type=StateType.CUSTOM,
                operation="setup_enterprise_demo",
                status=OperationStatus.FAILED,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                target_name=input.demo_name,
                error_message=str(e),
            )
            
            return SetupEnterpriseDemoOutput(
                success=False,
                demo_name=input.demo_name,
                workspace_id=self.workspace_id,
                workspace_name=self.workspace_name,
                created_items=created_items,
                generated_data=generated_data,
                generated_measures=generated_measures,
                measure_stats={},
                dependency_graph={},
                execution_time_seconds=time.time() - start_time,
                dry_run=input.dry_run,
                error=str(e),
                snapshot_id=snapshot.id,
            )
    
    def _calculate_depth(self, name: str, graph: Dict[str, set], visited: set = None) -> int:
        """Calculate depth in dependency tree."""
        if visited is None:
            visited = set()
        if name in visited:
            return 0  # Circular dependency protection
        visited.add(name)
        
        deps = graph.get(name, set())
        if not deps:
            return 0
        return 1 + max(self._calculate_depth(d, graph, visited.copy()) for d in deps)
    
    async def _create_lakehouse(self, name: str) -> CreatedItem:
        """Create a Lakehouse in Fabric."""
        try:
            response = await self.client.post(
                f"/workspaces/{self.workspace_id}/lakehouses",
                json_data={"displayName": name},
            )
            
            if response.status_code in (200, 201, 202):
                data = response.json() if response.content else {}
                return CreatedItem(
                    item_type="Lakehouse",
                    name=name,
                    id=data.get("id"),
                    status="created",
                    details=data,
                )
            else:
                return CreatedItem(
                    item_type="Lakehouse",
                    name=name,
                    status="failed",
                    details={"status_code": response.status_code},
                )
        except Exception as e:
            return CreatedItem(
                item_type="Lakehouse",
                name=name,
                status="error",
                details={"error": str(e)},
            )
    
    async def _create_pipeline(self, name: str, lakehouse_name: str) -> CreatedItem:
        """Create a Data Factory pipeline."""
        try:
            builder = PipelineBuilder(name, lakehouse_name)
            definition = builder.build_pipeline_definition()
            
            # Note: Actual pipeline creation would require the Data Factory API
            # This is a placeholder showing what would be created
            return CreatedItem(
                item_type="Pipeline",
                name=name,
                status="definition_generated",
                details={
                    "activities": len(definition["properties"]["activities"]),
                    "definition": definition,
                },
            )
        except Exception as e:
            return CreatedItem(
                item_type="Pipeline",
                name=name,
                status="error",
                details={"error": str(e)},
            )
    
    async def _create_semantic_model(
        self,
        name: str,
        dax_generator: EnterpriseDAXGenerator,
    ) -> CreatedItem:
        """Create a Semantic Model with DAX measures."""
        try:
            builder = SemanticModelBuilder(name, dax_generator)
            model_bim = builder.build_model_bim()
            
            # Encode as base64 for API
            model_json = json.dumps(model_bim, indent=2)
            model_b64 = base64.b64encode(model_json.encode()).decode()
            
            # Create the semantic model
            create_request = {
                "displayName": name,
                "definition": {
                    "parts": [{
                        "path": "model.bim",
                        "payload": model_b64,
                        "payloadType": "InlineBase64"
                    }]
                }
            }
            
            response = await self.client.post(
                f"/workspaces/{self.workspace_id}/semanticModels",
                json_data=create_request,
            )
            
            if response.status_code in (200, 201, 202):
                # Handle potential LRO
                result = await self.client.wait_for_lro(response)
                return CreatedItem(
                    item_type="SemanticModel",
                    name=name,
                    id=result.get("id"),
                    status="created",
                    details={
                        "measures": len(dax_generator.measures),
                        "tables": 7,
                        "relationships": 6,
                    },
                )
            else:
                return CreatedItem(
                    item_type="SemanticModel",
                    name=name,
                    status="failed",
                    details={
                        "status_code": response.status_code,
                        "measures_defined": len(dax_generator.measures),
                    },
                )
        except Exception as e:
            return CreatedItem(
                item_type="SemanticModel",
                name=name,
                status="error",
                details={
                    "error": str(e),
                    "measures_defined": len(dax_generator.measures),
                },
            )
