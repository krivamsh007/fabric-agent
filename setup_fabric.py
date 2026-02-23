#!/usr/bin/env python3
"""
Fabric Refactor - Master Setup Script
======================================

This script orchestrates the complete setup of your Fabric environment.

Usage:
    python setup_fabric.py                    # Full interactive setup
    python setup_fabric.py --step tables      # Just upload tables
    python setup_fabric.py --print-notebook   # Print notebook code
    python setup_fabric.py --print-measures   # Print DAX measures
    
The setup process:
1. Authenticate with Fabric
2. Create/verify workspace  
3. Create Lakehouse
4. Upload all data tables
5. Guide semantic model creation
6. Guide report creation
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))


def print_banner():
    """Print welcome banner"""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                                                               ║
║   🔧 FABRIC REFACTOR - AUTOMATED SETUP                        ║
║                                                               ║
║   This will create your complete Fabric environment           ║
║   for the AI-powered refactoring project.                     ║
║                                                               ║
╚═══════════════════════════════════════════════════════════════╝
""")


def check_dependencies():
    """Check required packages"""
    print("📦 Checking dependencies...")
    
    required = {
        "pandas": "pandas",
        "sempy": "semantic-link-sempy",
        "azure.identity": "azure-identity",
    }
    
    missing = []
    for module, package in required.items():
        try:
            __import__(module.split('.')[0])
            print(f"   ✅ {package}")
        except ImportError:
            print(f"   ❌ {package}")
            missing.append(package)
    
    if missing:
        print(f"\n⚠️  Missing packages. Install with:")
        print(f"   pip install {' '.join(missing)}")
        return False
    
    return True


def run_full_setup():
    """Run the complete setup process"""
    print_banner()
    
    if not check_dependencies():
        print("\nPlease install missing packages and try again.")
        return
    
    from fabric_setup import FabricSetup
    
    # Get workspace name
    print("\n" + "-" * 60)
    workspace = input("Enter workspace name [fabric-refactor-demo]: ").strip()
    if not workspace:
        workspace = "fabric-refactor-demo"
    
    # Run setup
    setup = FabricSetup(workspace_name=workspace)
    results = setup.run_full_setup()
    
    # Check results
    if all(results.values()):
        print("\n✅ Setup complete!")
    else:
        print("\n⚠️  Some steps need manual completion.")
        print("   See instructions above.")


def print_notebook():
    """Print the notebook code for copy/paste"""
    notebook_path = Path(__file__).parent / "notebooks" / "01_setup_data.py"
    
    if notebook_path.exists():
        print("\n" + "=" * 60)
        print("📓 FABRIC NOTEBOOK CODE")
        print("=" * 60)
        print("\nCopy everything below into a new Fabric notebook:\n")
        print("-" * 60)
        
        with open(notebook_path, 'r') as f:
            print(f.read())
            
        print("-" * 60)
        print("\n✅ Copy complete!")
    else:
        print(f"❌ Notebook not found at {notebook_path}")


def print_measures():
    """Print all DAX measures"""
    from semantic_model_setup import SemanticModelManager
    
    manager = SemanticModelManager("", "")
    manager._print_manual_measures()


def print_quick_start():
    """Print quick start guide"""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║                    QUICK START GUIDE                          ║
╚═══════════════════════════════════════════════════════════════╝

OPTION 1: Automated (requires Fabric SDK)
─────────────────────────────────────────
pip install semantic-link-sempy azure-identity pandas
python setup_fabric.py

OPTION 2: Manual with Notebook (recommended for free tier)
──────────────────────────────────────────────────────────
1. Go to https://app.fabric.microsoft.com
2. Create a new Workspace: "fabric-refactor-demo"
3. Create a new Lakehouse: "sales_lakehouse"
4. Create a new Notebook, attach to lakehouse
5. Run: python setup_fabric.py --print-notebook
6. Copy the output into your notebook and run it

OPTION 3: Upload CSVs Manually
──────────────────────────────
1. Create Lakehouse in Fabric portal
2. Upload these files from data/bronze/:
   - dim_products.csv
   - dim_customers.csv
   - dim_stores.csv
   - dim_date.csv
   - sales_transactions_202401.csv
3. Upload from data/silver/:
   - fact_sales_curated.csv
4. Create semantic model from lakehouse
5. Run: python setup_fabric.py --print-measures
6. Add the DAX measures to your model

After setup, test the impact analyzer:
─────────────────────────────────────
python -c "from src.impact_analyzer import demo; demo()"
""")


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Set up Fabric environment for Fabric Refactor project",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python setup_fabric.py                 # Full interactive setup
  python setup_fabric.py --quick-start   # Show setup options
  python setup_fabric.py --print-notebook # Print notebook code
  python setup_fabric.py --print-measures # Print DAX measures
        """
    )
    
    parser.add_argument("--quick-start", "-q", action="store_true",
                        help="Show quick start guide")
    parser.add_argument("--print-notebook", "-n", action="store_true",
                        help="Print Fabric notebook code")
    parser.add_argument("--print-measures", "-m", action="store_true",
                        help="Print DAX measures for manual creation")
    parser.add_argument("--step", choices=["all", "tables", "model"],
                        default="all", help="Run specific setup step")
    parser.add_argument("--workspace", "-w", default="fabric-refactor-demo",
                        help="Workspace name")
    
    args = parser.parse_args()
    
    if args.quick_start:
        print_quick_start()
    elif args.print_notebook:
        print_notebook()
    elif args.print_measures:
        print_measures()
    else:
        run_full_setup()


if __name__ == "__main__":
    main()
