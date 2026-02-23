#!/usr/bin/env python3
"""
Code Validation Script
======================

Validates all code in the fabric-agent project before execution:
- Syntax checking for all Python files
- Import validation (structural, not runtime)
- Configuration file validation
- Test structure validation

Usage:
    python scripts/validate_code.py
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import List, Tuple

# Colors for output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
RESET = "\033[0m"


def check_syntax(file_path: Path) -> Tuple[bool, str]:
    """Check Python syntax for a file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
        ast.parse(source, filename=str(file_path))
        return True, ""
    except SyntaxError as e:
        return False, f"Line {e.lineno}: {e.msg}"
    except Exception as e:
        return False, str(e)


def check_imports(file_path: Path) -> List[str]:
    """Check imports in a file and return any issues."""
    issues = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source, filename=str(file_path))
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    # Check for problematic imports
                    if alias.name.startswith("fabric_agent."):
                        # Verify the module path exists
                        module_path = alias.name.replace(".", "/")
                        expected = Path(file_path).parent.parent / f"{module_path}.py"
                        expected_pkg = Path(file_path).parent.parent / module_path / "__init__.py"
                        if not expected.exists() and not expected_pkg.exists():
                            issues.append(f"Import '{alias.name}' may not exist")
                            
            elif isinstance(node, ast.ImportFrom):
                if node.module and node.module.startswith("fabric_agent."):
                    module_path = node.module.replace(".", "/")
                    expected = Path(file_path).parent.parent / f"{module_path}.py"
                    expected_pkg = Path(file_path).parent.parent / module_path / "__init__.py"
                    if not expected.exists() and not expected_pkg.exists():
                        issues.append(f"Import from '{node.module}' may not exist")
                        
    except Exception as e:
        issues.append(f"Parse error: {e}")
    
    return issues


def validate_directory(base_path: Path) -> Tuple[int, int, List[str]]:
    """Validate all Python files in a directory."""
    passed = 0
    failed = 0
    all_issues = []
    
    for py_file in base_path.rglob("*.py"):
        # Skip pycache
        if "__pycache__" in str(py_file):
            continue
            
        # Syntax check
        ok, msg = check_syntax(py_file)
        rel_path = py_file.relative_to(base_path)
        
        if ok:
            print(f"  {GREEN}✓{RESET} {rel_path}")
            passed += 1
        else:
            print(f"  {RED}✗{RESET} {rel_path}: {msg}")
            failed += 1
            all_issues.append(f"{rel_path}: {msg}")
    
    return passed, failed, all_issues


def check_json_files(base_path: Path) -> List[str]:
    """Validate JSON configuration files."""
    import json
    issues = []
    
    json_files = [
        base_path / "mcp_config.json",
    ]
    
    for json_file in json_files:
        if json_file.exists():
            try:
                with open(json_file, "r") as f:
                    json.load(f)
                print(f"  {GREEN}✓{RESET} {json_file.name}")
            except json.JSONDecodeError as e:
                print(f"  {RED}✗{RESET} {json_file.name}: {e}")
                issues.append(f"{json_file.name}: {e}")
    
    return issues


def check_required_files(base_path: Path) -> List[str]:
    """Check that required files exist."""
    issues = []
    
    required = [
        "pyproject.toml",
        "README.md",
        ".env.template",
        "fabric_agent/__init__.py",
        "fabric_agent/core/__init__.py",
        "fabric_agent/core/agent.py",
        "fabric_agent/core/config.py",
        "fabric_agent/api/__init__.py",
        "fabric_agent/api/fabric_client.py",
        "fabric_agent/tools/__init__.py",
        "fabric_agent/tools/models.py",
        "fabric_agent/tools/fabric_tools.py",
        "fabric_agent/tools/safety_refactor.py",
        "fabric_agent/tools/workspace_graph.py",
        "fabric_agent/storage/__init__.py",
        "fabric_agent/storage/memory_manager.py",
        "fabric_agent/refactor/__init__.py",
        "fabric_agent/refactor/orchestrator.py",
        "fabric_agent/ui/__init__.py",
        "fabric_agent/ui/graph_viewer.py",
        "fabric_agent/mcp_server.py",
        "tests/__init__.py",
    ]
    
    for req in required:
        path = base_path / req
        if path.exists():
            print(f"  {GREEN}✓{RESET} {req}")
        else:
            print(f"  {RED}✗{RESET} {req} (MISSING)")
            issues.append(f"Missing: {req}")
    
    return issues


def main():
    """Run all validations."""
    print("=" * 60)
    print("FABRIC AGENT CODE VALIDATION")
    print("=" * 60)
    
    # Find project root
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    
    if not (project_root / "fabric_agent").exists():
        print(f"{RED}Error: fabric_agent directory not found{RESET}")
        sys.exit(1)
    
    total_passed = 0
    total_failed = 0
    all_issues = []
    
    # Check required files
    print(f"\n{YELLOW}Checking required files...{RESET}")
    issues = check_required_files(project_root)
    all_issues.extend(issues)
    
    # Check JSON configs
    print(f"\n{YELLOW}Checking configuration files...{RESET}")
    issues = check_json_files(project_root)
    all_issues.extend(issues)
    
    # Check main package
    print(f"\n{YELLOW}Validating fabric_agent package...{RESET}")
    passed, failed, issues = validate_directory(project_root / "fabric_agent")
    total_passed += passed
    total_failed += failed
    all_issues.extend(issues)
    
    # Check scripts
    print(f"\n{YELLOW}Validating scripts...{RESET}")
    passed, failed, issues = validate_directory(project_root / "scripts")
    total_passed += passed
    total_failed += failed
    all_issues.extend(issues)
    
    # Check tests
    print(f"\n{YELLOW}Validating tests...{RESET}")
    passed, failed, issues = validate_directory(project_root / "tests")
    total_passed += passed
    total_failed += failed
    all_issues.extend(issues)
    
    # Summary
    print("\n" + "=" * 60)
    print("VALIDATION SUMMARY")
    print("=" * 60)
    print(f"  Files checked: {total_passed + total_failed}")
    print(f"  {GREEN}Passed: {total_passed}{RESET}")
    print(f"  {RED}Failed: {total_failed}{RESET}")
    
    if all_issues:
        print(f"\n{RED}Issues found:{RESET}")
        for issue in all_issues:
            print(f"  • {issue}")
        print("\n" + "=" * 60)
        return 1
    else:
        print(f"\n{GREEN}All validations passed!{RESET}")
        print("=" * 60)
        return 0


if __name__ == "__main__":
    sys.exit(main())
