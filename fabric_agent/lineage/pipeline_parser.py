"""
Pipeline Parser
===============

Parses Microsoft Fabric Data Pipeline definitions to extract:
- Notebook activities (which notebooks does it execute?)
- Copy activities (source → destination)
- Lakehouse read/write operations
- Triggers and schedules
- Activity dependencies

This enables the lineage engine to show pipeline execution flow.

Example:
    parser = PipelineParser()
    deps = parser.parse(pipeline_definition)
    
    print(f"Notebooks: {deps.notebooks_executed}")
    print(f"Lakehouses read: {deps.lakehouses_read}")
"""

from __future__ import annotations

import base64
import json
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

from loguru import logger


@dataclass
class PipelineDependencies:
    """
    Dependencies extracted from a pipeline definition.
    
    Attributes:
        notebooks_executed: Notebooks run by this pipeline
        lakehouses_read: Lakehouses that are read from
        lakehouses_written: Lakehouses that are written to
        tables_read: Tables that are read
        tables_written: Tables that are written
        external_sources: External data sources referenced
        triggered_pipelines: Other pipelines triggered by this one
        triggers: Trigger configurations
        schedule: Schedule configuration (if any)
        activities: List of all activities with their types
    """
    
    notebooks_executed: List[str] = field(default_factory=list)
    lakehouses_read: List[str] = field(default_factory=list)
    lakehouses_written: List[str] = field(default_factory=list)
    tables_read: List[str] = field(default_factory=list)
    tables_written: List[str] = field(default_factory=list)
    external_sources: List[str] = field(default_factory=list)
    triggered_pipelines: List[str] = field(default_factory=list)
    triggers: List[Dict[str, Any]] = field(default_factory=list)
    schedule: Optional[Dict[str, Any]] = None
    activities: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "notebooks_executed": self.notebooks_executed,
            "lakehouses_read": self.lakehouses_read,
            "lakehouses_written": self.lakehouses_written,
            "tables_read": self.tables_read,
            "tables_written": self.tables_written,
            "external_sources": self.external_sources,
            "triggered_pipelines": self.triggered_pipelines,
            "triggers": self.triggers,
            "schedule": self.schedule,
            "activities": self.activities,
        }


class PipelineParser:
    """
    Parser for Fabric Data Pipeline definitions.
    
    Extracts dependency information from pipeline JSON definitions
    returned by the Fabric getDefinition API.
    """
    
    def parse(self, definition: Dict[str, Any]) -> PipelineDependencies:
        """
        Parse a pipeline definition and extract dependencies.
        
        Args:
            definition: Pipeline definition from getDefinition API
        
        Returns:
            PipelineDependencies with all extracted information
        """
        deps = PipelineDependencies()
        
        # Extract content from definition parts
        content = self._extract_content(definition)
        
        if not content:
            logger.warning("Could not extract pipeline content")
            return deps
        
        # Parse the pipeline JSON
        try:
            pipeline_json = json.loads(content) if isinstance(content, str) else content
        except json.JSONDecodeError:
            logger.warning("Could not parse pipeline JSON")
            return deps
        
        # Extract activities
        activities = pipeline_json.get("properties", {}).get("activities", [])
        
        for activity in activities:
            self._process_activity(activity, deps)
        
        # Extract triggers
        triggers = pipeline_json.get("properties", {}).get("triggers", [])
        deps.triggers = triggers
        
        # Look for schedule trigger
        for trigger in triggers:
            if trigger.get("type") == "ScheduleTrigger":
                deps.schedule = trigger.get("recurrence", {})
        
        return deps
    
    def _extract_content(self, definition: Dict[str, Any]) -> Optional[str]:
        """Extract the pipeline content from definition parts."""
        parts = definition.get("definition", {}).get("parts", [])
        
        for part in parts:
            path = part.get("path", "")
            payload = part.get("payload", "")
            
            # Look for the main pipeline definition
            if "pipeline" in path.lower() or not path:
                if payload:
                    try:
                        return base64.b64decode(payload).decode("utf-8", errors="replace")
                    except Exception:
                        pass
        
        # Fallback: return the whole definition if it's already JSON
        if "properties" in definition:
            return json.dumps(definition)
        
        return None
    
    def _process_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Process a single activity and extract its dependencies."""
        activity_type = activity.get("type", "")
        activity_name = activity.get("name", "")
        
        # Record the activity
        deps.activities.append({
            "name": activity_name,
            "type": activity_type,
        })
        
        # Process based on activity type
        if activity_type == "TridentNotebook":
            self._process_notebook_activity(activity, deps)
        elif activity_type == "Copy":
            self._process_copy_activity(activity, deps)
        elif activity_type == "ExecutePipeline":
            self._process_execute_pipeline_activity(activity, deps)
        elif activity_type == "Lookup":
            self._process_lookup_activity(activity, deps)
        elif activity_type == "GetMetadata":
            self._process_metadata_activity(activity, deps)
        elif activity_type in ("ForEach", "If", "Until", "Switch"):
            self._process_container_activity(activity, deps)
        elif activity_type == "SparkJob":
            self._process_spark_job_activity(activity, deps)
        elif activity_type == "Script":
            self._process_script_activity(activity, deps)
    
    def _process_notebook_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract notebook reference from a notebook activity."""
        type_props = activity.get("typeProperties", {})
        
        # Try different ways to find notebook reference
        notebook_ref = (
            type_props.get("notebook", {}).get("referenceName") or
            type_props.get("notebookPath") or
            type_props.get("artifact", {}).get("objectId")
        )
        
        if notebook_ref:
            deps.notebooks_executed.append(notebook_ref)
        
        # Also check for lakehouse references in notebook parameters
        parameters = type_props.get("parameters", {})
        self._extract_lakehouse_refs_from_params(parameters, deps)
    
    def _process_copy_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract source and sink from a copy activity."""
        type_props = activity.get("typeProperties", {})
        
        # Process source
        source = type_props.get("source", {})
        source_type = source.get("type", "")
        
        if "Lakehouse" in source_type or "Delta" in source_type:
            table_name = source.get("table") or source.get("tableName")
            if table_name:
                deps.tables_read.append(table_name)
            
            # Get lakehouse reference
            lakehouse = source.get("storeSettings", {}).get("lakehouse")
            if lakehouse:
                deps.lakehouses_read.append(lakehouse)
        
        elif source_type in ("AzureSqlSource", "SqlServerSource", "OracleSource"):
            deps.external_sources.append(f"{source_type}:{source.get('table', 'unknown')}")
        
        # Process sink
        sink = type_props.get("sink", {})
        sink_type = sink.get("type", "")
        
        if "Lakehouse" in sink_type or "Delta" in sink_type:
            table_name = sink.get("table") or sink.get("tableName")
            if table_name:
                deps.tables_written.append(table_name)
            
            lakehouse = sink.get("storeSettings", {}).get("lakehouse")
            if lakehouse:
                deps.lakehouses_written.append(lakehouse)
    
    def _process_execute_pipeline_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract triggered pipeline from an ExecutePipeline activity."""
        type_props = activity.get("typeProperties", {})
        
        pipeline_ref = (
            type_props.get("pipeline", {}).get("referenceName") or
            type_props.get("pipelineName")
        )
        
        if pipeline_ref:
            deps.triggered_pipelines.append(pipeline_ref)
    
    def _process_lookup_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract data source from a Lookup activity."""
        type_props = activity.get("typeProperties", {})
        source = type_props.get("source", {})
        
        if "Lakehouse" in source.get("type", "") or "Delta" in source.get("type", ""):
            table_name = source.get("table") or source.get("query", "").split()[-1]
            if table_name:
                deps.tables_read.append(table_name)
    
    def _process_metadata_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract dataset reference from GetMetadata activity."""
        type_props = activity.get("typeProperties", {})
        dataset = type_props.get("dataset", {}).get("referenceName")
        
        if dataset:
            deps.tables_read.append(dataset)
    
    def _process_container_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Process container activities (ForEach, If, etc.) and their nested activities."""
        type_props = activity.get("typeProperties", {})
        
        # Process nested activities
        nested_activities = (
            type_props.get("activities", []) or
            type_props.get("ifTrueActivities", []) or
            type_props.get("ifFalseActivities", []) or
            type_props.get("defaultActivities", [])
        )
        
        for nested in nested_activities:
            self._process_activity(nested, deps)
        
        # Process case activities for Switch
        cases = type_props.get("cases", [])
        for case in cases:
            for nested in case.get("activities", []):
                self._process_activity(nested, deps)
    
    def _process_spark_job_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract Spark job references."""
        type_props = activity.get("typeProperties", {})
        
        # Spark jobs might reference notebooks or JAR files
        main_file = type_props.get("file") or type_props.get("mainDefinitionFile")
        if main_file:
            deps.notebooks_executed.append(main_file)
    
    def _process_script_activity(self, activity: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract table references from script activities."""
        type_props = activity.get("typeProperties", {})
        scripts = type_props.get("scripts", [])
        
        for script in scripts:
            text = script.get("text", "")
            # Look for table references in SQL-like scripts
            tables = self._extract_tables_from_sql(text)
            deps.tables_read.extend(tables)
    
    def _extract_lakehouse_refs_from_params(self, parameters: Dict[str, Any], deps: PipelineDependencies) -> None:
        """Extract lakehouse references from activity parameters."""
        for key, value in parameters.items():
            if isinstance(value, dict):
                val = value.get("value", "")
            else:
                val = str(value)
            
            # Look for lakehouse-like references
            if "lakehouse" in key.lower() or "lakehouse" in val.lower():
                deps.lakehouses_read.append(val)
    
    def _extract_tables_from_sql(self, sql: str) -> List[str]:
        """Extract table names from SQL-like text."""
        tables = []
        
        # Simple pattern matching for FROM and JOIN clauses
        patterns = [
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            tables.extend(matches)
        
        return list(set(tables))


def parse_pipeline_from_file(filepath: str) -> PipelineDependencies:
    """
    Convenience function to parse a pipeline definition from a JSON file.
    
    Args:
        filepath: Path to the pipeline definition JSON file
    
    Returns:
        PipelineDependencies
    """
    with open(filepath, "r", encoding="utf-8") as f:
        definition = json.load(f)
    
    parser = PipelineParser()
    return parser.parse(definition)
