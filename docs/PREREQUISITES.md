# Fabric Refactor Agent - Complete Setup Guide

## 🎯 Project Goal

Build an **AI Agent** that:
1. Connects to Microsoft Fabric via MCP (Model Context Protocol)
2. Understands your Fabric workspace structure
3. Analyzes impact before making changes
4. Safely refactors measures/columns across reports
5. Maintains persistent memory of all changes

---

## 📋 Prerequisites for Local Fabric Connection

### 1. Microsoft Fabric Account

**Option A: Free Trial (60 days)**
```
1. Go to: https://app.fabric.microsoft.com
2. Sign in with Microsoft account (personal or work)
3. Click "Start free trial" or "Try free"
4. You get a trial capacity for 60 days
```

**Option B: Existing Microsoft 365**
```
If your organization has Power BI Premium or Fabric capacity,
you can use that. Check with your IT admin.
```

### 2. Azure AD App Registration (Required for API Access)

You need an Azure AD app to authenticate programmatically:

```bash
# Step 1: Go to Azure Portal
https://portal.azure.com

# Step 2: Navigate to
Azure Active Directory → App registrations → New registration

# Step 3: Configure the app
Name: fabric-refactor-agent
Supported account types: Single tenant (or multi if needed)
Redirect URI: http://localhost (for local dev)

# Step 4: After creation, note these values:
- Application (client) ID  → AZURE_CLIENT_ID
- Directory (tenant) ID    → AZURE_TENANT_ID

# Step 5: Create a client secret
Certificates & secrets → New client secret
Note the Value → AZURE_CLIENT_SECRET (save immediately, shown only once!)

# Step 6: Add API Permissions
API permissions → Add a permission → 
  → Microsoft APIs → Power BI Service
  → Delegated permissions:
    ✓ Dataset.ReadWrite.All
    ✓ Report.ReadWrite.All
    ✓ Workspace.ReadWrite.All
    ✓ Content.Create
  → Application permissions (for service principal):
    ✓ Tenant.ReadWrite.All (if needed)

# Step 7: Grant admin consent (may need admin)
Click "Grant admin consent for [your org]"
```

### 3. Enable Service Principal Access in Fabric

```bash
# In Fabric Admin Portal:
1. Go to: https://app.fabric.microsoft.com
2. Settings (gear icon) → Admin portal
3. Tenant settings → Developer settings
4. Enable: "Service principals can use Fabric APIs"
5. Enable: "Service principals can access read-only admin APIs"
6. Apply to: Entire organization (or specific security group)
```

### 4. Local Environment Setup

```bash
# Python 3.10+ required
python --version  # Should be 3.10 or higher

# Create project directory
mkdir fabric-refactor-agent
cd fabric-refactor-agent

# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 5. Environment Variables

Create a `.env` file in your project root:

```bash
# Azure AD / Entra ID Authentication
AZURE_TENANT_ID=your-tenant-id-here
AZURE_CLIENT_ID=your-client-id-here
AZURE_CLIENT_SECRET=your-client-secret-here

# Fabric Configuration
FABRIC_WORKSPACE_NAME=fabric-refactor-demo

# Optional: For interactive login (no client secret needed)
# Set USE_INTERACTIVE_AUTH=true to use browser login
USE_INTERACTIVE_AUTH=false

# MCP Server Configuration
MCP_SERVER_PORT=8080
MCP_MEMORY_PATH=./memory/refactor_log.json
```

---

## 🔧 Authentication Methods

### Method 1: Interactive Browser Login (Easiest for Dev)

```python
from azure.identity import InteractiveBrowserCredential

credential = InteractiveBrowserCredential()
# Opens browser for login, caches token locally
```

### Method 2: Device Code Flow (For Headless/SSH)

```python
from azure.identity import DeviceCodeCredential

credential = DeviceCodeCredential()
# Prints a code, you enter it at https://microsoft.com/devicelogin
```

### Method 3: Client Secret (For Production/Agents)

```python
from azure.identity import ClientSecretCredential

credential = ClientSecretCredential(
    tenant_id=os.environ["AZURE_TENANT_ID"],
    client_id=os.environ["AZURE_CLIENT_ID"],
    client_secret=os.environ["AZURE_CLIENT_SECRET"]
)
```

### Method 4: Default Credential Chain (Tries All)

```python
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
# Tries: Environment vars → Managed Identity → CLI → Interactive
```

---

## 🧪 Verify Your Setup

Run this script to verify everything is configured correctly:

```python
# save as verify_setup.py
import os
from dotenv import load_dotenv

load_dotenv()

print("🔍 Checking Fabric Connection Prerequisites...\n")

# Check environment variables
required_vars = [
    ("AZURE_TENANT_ID", "Azure tenant ID"),
    ("AZURE_CLIENT_ID", "Azure client/app ID"),
]

optional_vars = [
    ("AZURE_CLIENT_SECRET", "Client secret (for service principal)"),
    ("FABRIC_WORKSPACE_NAME", "Target workspace name"),
]

all_good = True

print("Required variables:")
for var, desc in required_vars:
    value = os.environ.get(var)
    if value:
        print(f"  ✅ {var}: {value[:8]}...")
    else:
        print(f"  ❌ {var}: NOT SET - {desc}")
        all_good = False

print("\nOptional variables:")
for var, desc in optional_vars:
    value = os.environ.get(var)
    if value:
        print(f"  ✅ {var}: {'*' * 8}...")
    else:
        print(f"  ⚠️  {var}: Not set - {desc}")

# Test imports
print("\nChecking packages:")
packages = [
    ("azure.identity", "Azure authentication"),
    ("sempy", "Fabric SDK (semantic-link)"),
    ("mcp", "Model Context Protocol"),
    ("pandas", "Data processing"),
]

for pkg, desc in packages:
    try:
        __import__(pkg.split('.')[0])
        print(f"  ✅ {pkg}")
    except ImportError:
        print(f"  ❌ {pkg} - {desc}")
        all_good = False

if all_good:
    print("\n✅ All prerequisites met! Ready to connect.")
else:
    print("\n⚠️  Some prerequisites missing. See above.")
```

---

## 🚀 Quick Connection Test

```python
# save as test_connection.py
import os
from dotenv import load_dotenv
from azure.identity import InteractiveBrowserCredential, ClientSecretCredential

load_dotenv()

def get_credential():
    """Get appropriate credential based on config"""
    if os.environ.get("USE_INTERACTIVE_AUTH", "").lower() == "true":
        return InteractiveBrowserCredential()
    elif os.environ.get("AZURE_CLIENT_SECRET"):
        return ClientSecretCredential(
            tenant_id=os.environ["AZURE_TENANT_ID"],
            client_id=os.environ["AZURE_CLIENT_ID"],
            client_secret=os.environ["AZURE_CLIENT_SECRET"]
        )
    else:
        return InteractiveBrowserCredential()

def test_fabric_connection():
    print("🔌 Testing Fabric Connection...\n")
    
    try:
        import sempy.fabric as fabric
        
        # This triggers authentication
        print("⏳ Authenticating (may open browser)...")
        workspaces = fabric.list_workspaces()
        
        print(f"✅ Connected! Found {len(workspaces)} workspace(s)\n")
        
        for _, ws in workspaces.iterrows():
            print(f"  📁 {ws['Name']}")
            print(f"     ID: {ws['Id']}")
            
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    test_fabric_connection()
```

---

## 📁 Project Structure

```
fabric-refactor-agent/
├── .env                      # Your credentials (DO NOT COMMIT)
├── .env.template             # Template for others
├── requirements.txt          # Python dependencies
│
├── src/
│   ├── agent/                # AI Agent core
│   │   ├── __init__.py
│   │   ├── fabric_agent.py   # Main agent logic
│   │   └── tools.py          # Agent tools/functions
│   │
│   ├── mcp/                  # MCP Server
│   │   ├── __init__.py
│   │   ├── server.py         # MCP server implementation
│   │   └── handlers.py       # Tool handlers
│   │
│   ├── fabric/               # Fabric SDK wrapper
│   │   ├── __init__.py
│   │   ├── client.py         # Fabric API client
│   │   ├── workspace.py      # Workspace operations
│   │   └── semantic_model.py # Semantic model operations
│   │
│   ├── parser/               # PBIR parsing
│   │   ├── __init__.py
│   │   ├── pbir_parser.py    # PBIR format parser
│   │   └── impact_analyzer.py # Dependency analysis
│   │
│   └── memory/               # Persistent memory
│       ├── __init__.py
│       └── refactor_store.py # Change history
│
├── data/                     # Sample data for testing
│   ├── bronze/
│   ├── silver/
│   └── reports/
│
├── memory/                   # Persistent storage
│   └── refactor_log.json
│
├── tests/
│   └── test_agent.py
│
├── setup_fabric.py           # Setup script
├── run_agent.py              # Start the agent
└── README.md
```

---

## 🤖 Agent Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      USER / AI ASSISTANT                     │
│                   (Claude, GPT, etc.)                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                     MCP SERVER (localhost:8080)              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                    TOOLS                             │   │
│  │  • list_workspaces      • analyze_impact            │   │
│  │  • list_items           • rename_measure            │   │
│  │  • get_semantic_model   • rename_column             │   │
│  │  • get_report_pbir      • get_refactor_history      │   │
│  │  • export_report        • rollback_change           │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    FABRIC API CLIENT                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Workspace   │  │   Lakehouse  │  │   Semantic   │      │
│  │  Management  │  │   & Tables   │  │    Model     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │   Reports    │  │     PBIR     │  │   Memory     │      │
│  │   (PBIR)     │  │    Parser    │  │    Store     │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   MICROSOFT FABRIC                           │
│         (Workspaces, Lakehouses, Semantic Models, Reports)   │
└─────────────────────────────────────────────────────────────┘
```

---

## Next Steps

1. **Set up Azure AD app** (see Section 2)
2. **Configure .env file** (see Section 5)
3. **Run verify_setup.py** to check everything
4. **Run test_connection.py** to verify Fabric access
5. **Start building the MCP server** (next phase)
