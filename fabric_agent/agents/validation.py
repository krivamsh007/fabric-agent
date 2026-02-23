"""
Validation Agent - Tests Refactoring Results
=============================================

This module validates that refactoring operations don't break
DAX calculations by running test queries before and after changes.

It provides:
- DAX query execution
- Before/after result comparison
- Automatic test generation
- Validation reporting
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

from loguru import logger


@dataclass
class QueryResult:
    """Result of a DAX query execution."""
    query: str
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    row_count: int = 0
    execution_time_ms: float = 0
    error_message: Optional[str] = None
    checksum: Optional[str] = None  # Hash of results for comparison
    
    def compute_checksum(self) -> str:
        """Compute a checksum of the result data."""
        if self.data is None:
            return ""
        data_str = json.dumps(self.data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()[:16]


@dataclass
class ValidationTest:
    """A single validation test case."""
    test_id: str
    name: str
    description: str
    dax_query: str
    expected_behavior: str = "results_match"  # results_match, row_count_match, no_error
    tolerance: float = 0.0001  # For numeric comparisons


@dataclass
class TestComparison:
    """Comparison between before and after results."""
    test: ValidationTest
    before_result: QueryResult
    after_result: QueryResult
    passed: bool
    difference_summary: str


@dataclass
class ValidationReport:
    """Complete validation report for a refactoring operation."""
    report_id: str
    operation_id: str
    model_name: str
    workspace_name: str
    timestamp: str
    tests_run: int
    tests_passed: int
    tests_failed: int
    overall_passed: bool
    comparisons: List[TestComparison]
    execution_time_ms: float
    recommendations: List[str]


def _try_import_sempy():
    """Try to import SemPy."""
    try:
        import sempy.fabric as fabric
        return fabric
    except ImportError:
        return None


class ValidationAgent:
    """
    Validates refactoring operations by testing DAX queries.
    
    This agent:
    1. Generates test queries based on affected measures
    2. Runs queries before refactoring
    3. Runs queries after refactoring
    4. Compares results and reports differences
    
    Usage:
        >>> validator = ValidationAgent(workspace_name)
        >>> 
        >>> # Capture state before refactoring
        >>> before = await validator.capture_state(model_name, tests)
        >>> 
        >>> # ... perform refactoring ...
        >>> 
        >>> # Validate after refactoring
        >>> report = await validator.validate(model_name, tests, before)
    """
    
    def __init__(self, workspace_name: str):
        """Initialize the validation agent."""
        self.workspace_name = workspace_name
        self._fabric = _try_import_sempy()
    
    def _ensure_sempy(self) -> Any:
        """Ensure SemPy is available."""
        if self._fabric is None:
            self._fabric = _try_import_sempy()
        if self._fabric is None:
            raise RuntimeError("SemPy required for validation")
        return self._fabric
    
    # =========================================================================
    # TEST GENERATION
    # =========================================================================
    
    def generate_tests_for_measure(
        self,
        measure_name: str,
        table_name: Optional[str] = None,
    ) -> List[ValidationTest]:
        """
        Generate standard test queries for a measure.
        
        Args:
            measure_name: The measure to test.
            table_name: Optional table context.
        
        Returns:
            List of validation tests.
        """
        tests = []
        
        # Test 1: Simple evaluation
        tests.append(ValidationTest(
            test_id=str(uuid4()),
            name=f"Evaluate {measure_name}",
            description=f"Simple evaluation of [{measure_name}]",
            dax_query=f'EVALUATE ROW("Value", [{measure_name}])',
            expected_behavior="results_match",
        ))
        
        # Test 2: Evaluate with table context (if provided)
        if table_name:
            tests.append(ValidationTest(
                test_id=str(uuid4()),
                name=f"{measure_name} by {table_name}",
                description=f"Evaluate [{measure_name}] grouped by {table_name}",
                dax_query=f'''
                    EVALUATE
                    SUMMARIZECOLUMNS(
                        {table_name}[Category],
                        "Value", [{measure_name}]
                    )
                ''',
                expected_behavior="results_match",
            ))
        
        # Test 3: Top N evaluation
        tests.append(ValidationTest(
            test_id=str(uuid4()),
            name=f"Top values for {measure_name}",
            description=f"Top 10 values of [{measure_name}]",
            dax_query=f'''
                EVALUATE
                TOPN(10,
                    SUMMARIZECOLUMNS("Value", [{measure_name}]),
                    [Value], DESC
                )
            ''',
            expected_behavior="results_match",
        ))
        
        return tests
    
    def generate_tests_for_refactor(
        self,
        old_name: str,
        new_name: str,
        affected_measures: List[str],
    ) -> List[ValidationTest]:
        """
        Generate comprehensive tests for a refactoring operation.
        
        Args:
            old_name: Original measure name.
            new_name: New measure name.
            affected_measures: List of measures that reference the target.
        
        Returns:
            List of validation tests.
        """
        tests = []
        
        # Test the renamed measure itself (using new name after refactor)
        tests.extend(self.generate_tests_for_measure(new_name))
        
        # Test each affected measure
        for measure in affected_measures[:10]:  # Limit to 10
            tests.extend(self.generate_tests_for_measure(measure))
        
        return tests
    
    # =========================================================================
    # QUERY EXECUTION
    # =========================================================================
    
    async def execute_query(
        self,
        model_name: str,
        query: str,
    ) -> QueryResult:
        """
        Execute a DAX query and return results.
        
        Args:
            model_name: Semantic model name.
            query: DAX query to execute.
        
        Returns:
            QueryResult with data or error.
        """
        import time
        start_time = time.time()
        
        try:
            fabric = self._ensure_sempy()
            
            # Execute using SemPy evaluate_dax
            df = fabric.evaluate_dax(
                dataset=model_name,
                dax_string=query,
                workspace=self.workspace_name,
            )
            
            execution_time = (time.time() - start_time) * 1000
            
            # Convert DataFrame to list of dicts
            data = df.to_dict(orient='records') if df is not None else []
            
            result = QueryResult(
                query=query,
                success=True,
                data=data,
                row_count=len(data),
                execution_time_ms=execution_time,
            )
            result.checksum = result.compute_checksum()
            
            return result
            
        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            
            return QueryResult(
                query=query,
                success=False,
                error_message=str(e),
                execution_time_ms=execution_time,
            )
    
    async def capture_state(
        self,
        model_name: str,
        tests: List[ValidationTest],
    ) -> Dict[str, QueryResult]:
        """
        Capture the current state by running all tests.
        
        Args:
            model_name: Semantic model name.
            tests: List of tests to run.
        
        Returns:
            Dictionary mapping test_id to QueryResult.
        """
        results = {}
        
        for test in tests:
            logger.debug(f"Capturing: {test.name}")
            result = await self.execute_query(model_name, test.dax_query)
            results[test.test_id] = result
        
        return results
    
    # =========================================================================
    # VALIDATION
    # =========================================================================
    
    async def validate(
        self,
        model_name: str,
        tests: List[ValidationTest],
        before_results: Dict[str, QueryResult],
        operation_id: str = "",
    ) -> ValidationReport:
        """
        Validate by comparing before and after results.
        
        Args:
            model_name: Semantic model name.
            tests: List of tests to run.
            before_results: Results captured before refactoring.
            operation_id: ID of the refactoring operation.
        
        Returns:
            ValidationReport with comparison details.
        """
        import time
        start_time = time.time()
        
        comparisons: List[TestComparison] = []
        tests_passed = 0
        tests_failed = 0
        
        for test in tests:
            before = before_results.get(test.test_id)
            if not before:
                logger.warning(f"No before result for test: {test.name}")
                continue
            
            # Run the test again (after refactoring)
            after = await self.execute_query(model_name, test.dax_query)
            
            # Compare results
            passed, diff_summary = self._compare_results(test, before, after)
            
            comparison = TestComparison(
                test=test,
                before_result=before,
                after_result=after,
                passed=passed,
                difference_summary=diff_summary,
            )
            comparisons.append(comparison)
            
            if passed:
                tests_passed += 1
            else:
                tests_failed += 1
        
        execution_time = (time.time() - start_time) * 1000
        
        # Generate recommendations
        recommendations = self._generate_recommendations(comparisons)
        
        return ValidationReport(
            report_id=str(uuid4()),
            operation_id=operation_id,
            model_name=model_name,
            workspace_name=self.workspace_name,
            timestamp=datetime.now(timezone.utc).isoformat(),
            tests_run=len(comparisons),
            tests_passed=tests_passed,
            tests_failed=tests_failed,
            overall_passed=tests_failed == 0,
            comparisons=comparisons,
            execution_time_ms=execution_time,
            recommendations=recommendations,
        )
    
    def _compare_results(
        self,
        test: ValidationTest,
        before: QueryResult,
        after: QueryResult,
    ) -> Tuple[bool, str]:
        """
        Compare before and after results.
        
        Returns:
            Tuple of (passed, difference_summary).
        """
        # If either query failed, compare error states
        if not before.success and not after.success:
            return True, "Both queries failed (expected if measure was removed)"
        
        if before.success != after.success:
            if not after.success:
                return False, f"Query failed after refactor: {after.error_message}"
            else:
                return False, f"Query started working (was failing before)"
        
        # Both succeeded - compare based on expected behavior
        if test.expected_behavior == "no_error":
            return True, "Query executed without error"
        
        elif test.expected_behavior == "row_count_match":
            if before.row_count == after.row_count:
                return True, f"Row count matches: {before.row_count}"
            else:
                return False, f"Row count changed: {before.row_count} -> {after.row_count}"
        
        elif test.expected_behavior == "results_match":
            # Compare checksums for quick check
            if before.checksum == after.checksum:
                return True, "Results identical (checksum match)"
            
            # Detailed comparison
            diff = self._detailed_comparison(before.data, after.data, test.tolerance)
            if diff:
                return False, diff
            else:
                return True, "Results match within tolerance"
        
        return False, "Unknown comparison type"
    
    def _detailed_comparison(
        self,
        before_data: Optional[List[Dict]],
        after_data: Optional[List[Dict]],
        tolerance: float,
    ) -> Optional[str]:
        """
        Perform detailed comparison of result data.
        
        Returns:
            Difference description or None if equal.
        """
        if before_data is None and after_data is None:
            return None
        
        if before_data is None or after_data is None:
            return "One result is None"
        
        if len(before_data) != len(after_data):
            return f"Row count differs: {len(before_data)} vs {len(after_data)}"
        
        # Compare row by row
        for i, (before_row, after_row) in enumerate(zip(before_data, after_data)):
            if set(before_row.keys()) != set(after_row.keys()):
                return f"Row {i}: Column mismatch"
            
            for key in before_row:
                before_val = before_row[key]
                after_val = after_row[key]
                
                # Numeric comparison with tolerance
                if isinstance(before_val, (int, float)) and isinstance(after_val, (int, float)):
                    if abs(before_val - after_val) > tolerance:
                        return f"Row {i}, column '{key}': {before_val} vs {after_val}"
                else:
                    if before_val != after_val:
                        return f"Row {i}, column '{key}': '{before_val}' vs '{after_val}'"
        
        return None  # All equal
    
    def _generate_recommendations(
        self,
        comparisons: List[TestComparison],
    ) -> List[str]:
        """Generate recommendations based on test results."""
        recommendations = []
        
        failed = [c for c in comparisons if not c.passed]
        
        if not failed:
            recommendations.append("✅ All validation tests passed. Safe to proceed.")
        else:
            recommendations.append(f"⚠️ {len(failed)} test(s) failed. Review before proceeding.")
            
            for comp in failed[:3]:  # Show first 3
                recommendations.append(f"  - {comp.test.name}: {comp.difference_summary}")
            
            if len(failed) > 3:
                recommendations.append(f"  ... and {len(failed) - 3} more failures")
            
            recommendations.append("")
            recommendations.append("Recommended actions:")
            recommendations.append("1. Review the DAX expressions that changed")
            recommendations.append("2. Verify the logic is still correct")
            recommendations.append("3. Consider rolling back if results are unexpected")
        
        return recommendations


# =============================================================================
# Convenience functions
# =============================================================================

async def validate_refactor(
    workspace_name: str,
    model_name: str,
    measure_name: str,
    before_state: Dict[str, QueryResult],
) -> ValidationReport:
    """
    Quick validation after a refactor.
    
    Example:
        >>> # Before refactor
        >>> validator = ValidationAgent(workspace_name)
        >>> tests = validator.generate_tests_for_measure("Revenue")
        >>> before = await validator.capture_state(model_name, tests)
        >>> 
        >>> # ... do refactor ...
        >>> 
        >>> # Validate
        >>> report = await validate_refactor(
        ...     workspace_name, model_name, "Revenue", before
        ... )
        >>> print(f"Passed: {report.overall_passed}")
    """
    validator = ValidationAgent(workspace_name)
    tests = validator.generate_tests_for_measure(measure_name)
    return await validator.validate(model_name, tests, before_state)
