#!/usr/bin/env python3
"""
Fabric Refactor - First Time Setup Script
==========================================
Run this after cloning to verify everything is working.
"""

import subprocess
import sys
from pathlib import Path


def print_header(text):
    print("\n" + "=" * 50)
    print(text)
    print("=" * 50)


def check_python_version():
    """Ensure Python 3.10+"""
    print_header("Checking Python Version")
    version = sys.version_info
    print(f"Python {version.major}.{version.minor}.{version.micro}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 10):
        print("⚠️  Warning: Python 3.10+ recommended")
        return False
    print("✓ Python version OK")
    return True


def check_dependencies():
    """Check if required packages are installed"""
    print_header("Checking Dependencies")
    
    required = [
        ("azure.identity", "azure-identity"),
        ("requests", "requests"),
        ("dotenv", "python-dotenv"),
    ]

    optional = [
        ("sempy", "semantic-link-sempy (optional for local)"),
        ("pandas", "pandas (optional)"),
        ("rich", "rich (optional)"),
    ]
    
    missing = []
    for module, package in required:
        try:
            __import__(module.split('.')[0])
            print(f"✓ {package}")
        except ImportError:
            print(f"✗ {package} (missing)")
            missing.append(package)

    for module, package in optional:
        try:
            __import__(module.split('.')[0])
            print(f"⚠ {package} - installed")
        except ImportError:
            print(f"⚠ {package} - not installed")
    
    return missing


def check_env_file():
    """Check if .env file exists"""
    print_header("Checking Environment Configuration")

    # Resolve relative to this script, not the current working directory
    root = Path(__file__).resolve().parent
    env_path = root / ".env"
    template_path = root / ".env.template"
    
    if env_path.exists():
        print("✓ .env file found")
        return True
    elif template_path.exists():
        print("ℹ .env file not found")
        print("  Creating from template...")
        import shutil
        shutil.copyfile(template_path, env_path)
        print("✓ Created .env from template")
        print("  → Edit .env with your Azure credentials")
        return True
    else:
        print("✗ No .env or .env.template found")
        return False


def test_fabric_connection():
    """Try to connect to Fabric"""
    print_header("Testing Fabric Connection")
    
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=Path(__file__).resolve().parent / ".env")

        from src.fabric_api import build_local_fabric_client

        print("✓ Fabric REST client ready")

        print("\nAttempting to list workspaces...")
        print("(This may open a browser for login)")

        client = build_local_fabric_client()
        data = client.get_json("/workspaces")
        workspaces = data.get("value", []) if isinstance(data, dict) else []
        print(f"✓ Connected! Found {len(workspaces)} workspace(s)")

        if workspaces:
            print("\nYour workspaces:")
            for ws in workspaces[:10]:
                print(f"  • {ws.get('displayName', ws.get('name', 'Unknown'))}")
        
        return True
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Log in at https://app.fabric.microsoft.com first")
        print("  2. Ensure your account has Fabric access")
        print("  3. Check firewall/proxy settings")
        return False


def main():
    print("\n" + "🔧 FABRIC REFACTOR SETUP 🔧".center(50))
    
    # Check Python
    check_python_version()
    
    # Check dependencies
    missing = check_dependencies()
    
    if missing:
        print(f"\n⚠️  Missing packages: {', '.join(missing)}")
        print("Install with: pip install -r requirements.txt")
        response = input("\nInstall now? [y/N]: ")
        if response.lower() == 'y':
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
            print("\n✓ Dependencies installed. Run this script again.")
            return
    
    # Check env
    check_env_file()
    
    # Test connection (optional)
    print_header("Ready to Test Fabric Connection?")
    response = input("Test connection now? (opens browser for login) [y/N]: ")
    
    if response.lower() == 'y':
        test_fabric_connection()
    
    # Summary
    print_header("Setup Complete!")
    print("""
Next Steps:
-----------
1. If you haven't already, create a workspace in Fabric:
   → https://app.fabric.microsoft.com
   → Workspaces → + New workspace → "fabric-refactor-demo"

2. Upload sample data:
   → Open your workspace
   → New item → Lakehouse → "demo_lakehouse"
   → Upload data/sample/sample_sales.csv

3. Create a Semantic Model and Report:
   → From Lakehouse → New semantic model
   → From Semantic Model → Create report

4. Run the test script:
   → python src/fabric_client.py

5. Tomorrow (Day 2): We'll build the PBIR parser!
""")


if __name__ == "__main__":
    main()
