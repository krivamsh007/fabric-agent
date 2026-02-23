"""
Enterprise Demo Data Generator
==============================

Generates production-grade mock data for a Sales Star Schema including:
- FactSales: 500K+ transaction records with realistic patterns
- DimProduct: Products with categories, subcategories, brands, costs
- DimDate: Full date dimension with fiscal calendars
- DimStore: Stores with regions, territories, managers
- DimCustomer: Customers with segments, tiers, demographics
- DimPromotion: Promotions with types, costs, effectiveness
- DimEmployee: Sales reps with hierarchy, tenure, quotas

Data includes:
- Seasonal patterns (holiday spikes, summer dips)
- Regional variations
- Product lifecycle patterns
- Customer behavior patterns
- Realistic NULL rates and data quality issues
"""

from __future__ import annotations

import csv
import io
import random
import hashlib
from datetime import datetime, date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

from loguru import logger


class Region(str, Enum):
    NORTHEAST = "Northeast"
    SOUTHEAST = "Southeast"
    MIDWEST = "Midwest"
    SOUTHWEST = "Southwest"
    WEST = "West"
    INTERNATIONAL = "International"


class CustomerSegment(str, Enum):
    ENTERPRISE = "Enterprise"
    MID_MARKET = "Mid-Market"
    SMB = "Small Business"
    CONSUMER = "Consumer"
    GOVERNMENT = "Government"


class ProductCategory(str, Enum):
    ELECTRONICS = "Electronics"
    FURNITURE = "Furniture"
    OFFICE_SUPPLIES = "Office Supplies"
    SOFTWARE = "Software"
    SERVICES = "Services"


@dataclass
class GeneratorConfig:
    """Configuration for demo data generation."""
    
    # Volume settings
    num_products: int = 500
    num_stores: int = 150
    num_customers: int = 10000
    num_employees: int = 500
    num_promotions: int = 100
    num_transactions: int = 500000
    
    # Date range
    start_date: date = field(default_factory=lambda: date(2021, 1, 1))
    end_date: date = field(default_factory=lambda: date(2024, 12, 31))
    
    # Fiscal calendar
    fiscal_year_start_month: int = 7  # July
    
    # Data quality settings (realistic enterprise issues)
    null_rate_low: float = 0.01
    null_rate_medium: float = 0.05
    null_rate_high: float = 0.10
    
    # Seed for reproducibility
    random_seed: int = 42


class EnterpriseDataGenerator:
    """
    Generates enterprise-grade mock data for a Sales Star Schema.
    
    Features:
    - Realistic seasonal patterns
    - Regional and product variations
    - Customer behavior modeling
    - Data quality issues (NULLs, outliers)
    - Referential integrity
    """
    
    def __init__(self, config: Optional[GeneratorConfig] = None):
        """Initialize the generator with configuration."""
        self.config = config or GeneratorConfig()
        random.seed(self.config.random_seed)
        
        # Reference data (generated in order due to dependencies)
        self.dates: List[Dict[str, Any]] = []
        self.products: List[Dict[str, Any]] = []
        self.stores: List[Dict[str, Any]] = []
        self.customers: List[Dict[str, Any]] = []
        self.employees: List[Dict[str, Any]] = []
        self.promotions: List[Dict[str, Any]] = []
        self.sales: List[Dict[str, Any]] = []
        
        logger.info(f"EnterpriseDataGenerator initialized with {self.config.num_transactions} target transactions")
    
    def generate_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Generate all dimension and fact tables.
        
        Returns:
            Dictionary mapping table names to lists of records.
        """
        logger.info("Starting enterprise data generation...")
        
        # Generate dimensions first (order matters for FK references)
        self._generate_dim_date()
        self._generate_dim_product()
        self._generate_dim_store()
        self._generate_dim_customer()
        self._generate_dim_employee()
        self._generate_dim_promotion()
        
        # Generate fact table last
        self._generate_fact_sales()
        
        logger.info("Enterprise data generation complete!")
        
        return {
            "DimDate": self.dates,
            "DimProduct": self.products,
            "DimStore": self.stores,
            "DimCustomer": self.customers,
            "DimEmployee": self.employees,
            "DimPromotion": self.promotions,
            "FactSales": self.sales,
        }
    
    def _generate_dim_date(self) -> None:
        """Generate comprehensive date dimension with fiscal calendar."""
        logger.info("Generating DimDate...")
        
        current = self.config.start_date
        date_key = 1
        
        while current <= self.config.end_date:
            # Fiscal year calculation
            if current.month >= self.config.fiscal_year_start_month:
                fiscal_year = current.year + 1
                fiscal_quarter = ((current.month - self.config.fiscal_year_start_month) // 3) + 1
                fiscal_month = current.month - self.config.fiscal_year_start_month + 1
            else:
                fiscal_year = current.year
                fiscal_quarter = ((current.month + 12 - self.config.fiscal_year_start_month) // 3) + 1
                fiscal_month = current.month + 12 - self.config.fiscal_year_start_month + 1
            
            # Week calculations
            week_of_year = current.isocalendar()[1]
            week_of_month = (current.day - 1) // 7 + 1
            
            # Holiday flags
            is_holiday = self._is_holiday(current)
            is_business_day = current.weekday() < 5 and not is_holiday
            
            # Season
            month = current.month
            if month in [12, 1, 2]:
                season = "Winter"
            elif month in [3, 4, 5]:
                season = "Spring"
            elif month in [6, 7, 8]:
                season = "Summer"
            else:
                season = "Fall"
            
            self.dates.append({
                "DateKey": date_key,
                "Date": current.isoformat(),
                "Year": current.year,
                "Quarter": f"Q{(current.month - 1) // 3 + 1}",
                "QuarterNumber": (current.month - 1) // 3 + 1,
                "Month": current.month,
                "MonthName": current.strftime("%B"),
                "MonthNameShort": current.strftime("%b"),
                "Week": week_of_year,
                "WeekOfMonth": week_of_month,
                "DayOfMonth": current.day,
                "DayOfWeek": current.weekday() + 1,
                "DayName": current.strftime("%A"),
                "DayNameShort": current.strftime("%a"),
                "IsWeekend": current.weekday() >= 5,
                "IsHoliday": is_holiday,
                "IsBusinessDay": is_business_day,
                "FiscalYear": fiscal_year,
                "FiscalQuarter": f"FQ{fiscal_quarter}",
                "FiscalQuarterNumber": fiscal_quarter,
                "FiscalMonth": fiscal_month,
                "FiscalYearQuarter": f"FY{fiscal_year}-Q{fiscal_quarter}",
                "YearMonth": current.strftime("%Y-%m"),
                "YearQuarter": f"{current.year}-Q{(current.month - 1) // 3 + 1}",
                "Season": season,
                "DayOfYear": current.timetuple().tm_yday,
                "IsLeapYear": current.year % 4 == 0 and (current.year % 100 != 0 or current.year % 400 == 0),
                "IsLastDayOfMonth": (current + timedelta(days=1)).month != current.month,
                "IsLastDayOfQuarter": (current + timedelta(days=1)).month in [1, 4, 7, 10] and (current + timedelta(days=1)).day == 1,
                "IsLastDayOfYear": current.month == 12 and current.day == 31,
            })
            
            current += timedelta(days=1)
            date_key += 1
        
        logger.info(f"Generated {len(self.dates)} date records")
    
    def _is_holiday(self, d: date) -> bool:
        """Check if date is a US federal holiday (simplified)."""
        holidays = [
            (1, 1),    # New Year
            (7, 4),    # Independence Day
            (12, 25),  # Christmas
            (12, 31),  # New Year's Eve
        ]
        
        # Thanksgiving (4th Thursday of November)
        if d.month == 11 and d.weekday() == 3:
            if 22 <= d.day <= 28:
                return True
        
        # Black Friday
        if d.month == 11 and d.weekday() == 4:
            if 23 <= d.day <= 29:
                return True
        
        return (d.month, d.day) in holidays
    
    def _generate_dim_product(self) -> None:
        """Generate product dimension with categories, subcategories, brands."""
        logger.info("Generating DimProduct...")
        
        # Product hierarchy
        categories = {
            ProductCategory.ELECTRONICS: {
                "subcategories": ["Computers", "Phones", "Audio", "Accessories", "Wearables"],
                "brands": ["TechPro", "DigiMax", "ElectroCore", "SmartGear", "NovaTech"],
                "base_price_range": (50, 2500),
                "margin_range": (0.15, 0.35),
            },
            ProductCategory.FURNITURE: {
                "subcategories": ["Desks", "Chairs", "Storage", "Tables", "Lighting"],
                "brands": ["OfficePro", "ComfortMax", "WorkSpace", "ErgoDesign", "ModernOffice"],
                "base_price_range": (75, 1500),
                "margin_range": (0.25, 0.50),
            },
            ProductCategory.OFFICE_SUPPLIES: {
                "subcategories": ["Paper", "Writing", "Filing", "Desk Accessories", "Breakroom"],
                "brands": ["SupplyMax", "OfficeMate", "PaperPro", "DeskEssentials", "WorkTools"],
                "base_price_range": (5, 150),
                "margin_range": (0.30, 0.60),
            },
            ProductCategory.SOFTWARE: {
                "subcategories": ["Productivity", "Security", "Design", "Analytics", "Collaboration"],
                "brands": ["SoftPro", "CloudSuite", "DataMax", "SecureIT", "TeamWork"],
                "base_price_range": (25, 500),
                "margin_range": (0.60, 0.85),
            },
            ProductCategory.SERVICES: {
                "subcategories": ["Consulting", "Training", "Support", "Installation", "Maintenance"],
                "brands": ["ServicePro", "ExpertHelp", "TechSupport", "ProServices", "CarePlus"],
                "base_price_range": (100, 5000),
                "margin_range": (0.40, 0.70),
            },
        }
        
        product_key = 1
        products_per_category = self.config.num_products // len(categories)
        
        for category, details in categories.items():
            for _ in range(products_per_category):
                subcategory = random.choice(details["subcategories"])
                brand = random.choice(details["brands"])
                
                base_price = random.uniform(*details["base_price_range"])
                margin = random.uniform(*details["margin_range"])
                cost = base_price * (1 - margin)
                
                # Product lifecycle
                intro_date = self.config.start_date + timedelta(
                    days=random.randint(0, (self.config.end_date - self.config.start_date).days // 2)
                )
                
                # Some products get discontinued
                is_discontinued = random.random() < 0.15
                discontinue_date = None
                if is_discontinued:
                    discontinue_date = intro_date + timedelta(days=random.randint(180, 730))
                    if discontinue_date > self.config.end_date:
                        discontinue_date = None
                        is_discontinued = False
                
                # Weight for shipping calculations
                weight = random.uniform(0.1, 50) if category != ProductCategory.SOFTWARE else 0
                
                self.products.append({
                    "ProductKey": product_key,
                    "ProductID": f"PRD-{product_key:06d}",
                    "ProductName": f"{brand} {subcategory} {random.randint(100, 999)}",
                    "ProductDescription": f"High-quality {subcategory.lower()} from {brand}",
                    "Category": category.value,
                    "Subcategory": subcategory,
                    "Brand": brand,
                    "SKU": f"{category.value[:3].upper()}-{brand[:3].upper()}-{product_key:05d}",
                    "UnitPrice": round(base_price, 2),
                    "UnitCost": round(cost, 2),
                    "StandardMargin": round(margin * 100, 2),
                    "Weight": round(weight, 2) if weight > 0 else None,
                    "WeightUnit": "lbs" if weight > 0 else None,
                    "ReorderPoint": random.randint(10, 100),
                    "SafetyStockLevel": random.randint(5, 50),
                    "LeadTimeDays": random.randint(3, 30),
                    "IntroductionDate": intro_date.isoformat(),
                    "DiscontinuedDate": discontinue_date.isoformat() if discontinue_date else None,
                    "IsActive": not is_discontinued or (discontinue_date and discontinue_date > date.today()),
                    "Color": random.choice(["Black", "White", "Silver", "Blue", "Red", None]),
                    "Size": random.choice(["Small", "Medium", "Large", "XL", None]),
                    "ProductTier": random.choice(["Basic", "Standard", "Premium", "Enterprise"]),
                    "WarrantyMonths": random.choice([12, 24, 36, 60, None]),
                    "CountryOfOrigin": random.choice(["USA", "China", "Germany", "Japan", "Mexico"]),
                })
                
                product_key += 1
        
        logger.info(f"Generated {len(self.products)} product records")
    
    def _generate_dim_store(self) -> None:
        """Generate store dimension with regions, territories, managers."""
        logger.info("Generating DimStore...")
        
        regions_data = {
            Region.NORTHEAST: {
                "states": ["NY", "MA", "PA", "NJ", "CT", "NH", "VT", "ME", "RI"],
                "cities": ["New York", "Boston", "Philadelphia", "Newark", "Hartford"],
                "territory_count": 5,
            },
            Region.SOUTHEAST: {
                "states": ["FL", "GA", "NC", "SC", "VA", "TN", "AL", "MS", "LA"],
                "cities": ["Miami", "Atlanta", "Charlotte", "Orlando", "Nashville"],
                "territory_count": 4,
            },
            Region.MIDWEST: {
                "states": ["IL", "OH", "MI", "IN", "WI", "MN", "IA", "MO", "KS"],
                "cities": ["Chicago", "Detroit", "Cleveland", "Indianapolis", "Milwaukee"],
                "territory_count": 5,
            },
            Region.SOUTHWEST: {
                "states": ["TX", "AZ", "NM", "OK", "AR"],
                "cities": ["Houston", "Dallas", "Phoenix", "San Antonio", "Austin"],
                "territory_count": 4,
            },
            Region.WEST: {
                "states": ["CA", "WA", "OR", "NV", "CO", "UT", "ID", "MT", "WY"],
                "cities": ["Los Angeles", "San Francisco", "Seattle", "Denver", "Portland"],
                "territory_count": 5,
            },
            Region.INTERNATIONAL: {
                "states": ["ON", "BC", "QC", "AB"],  # Canadian provinces
                "cities": ["Toronto", "Vancouver", "Montreal", "Calgary"],
                "territory_count": 2,
            },
        }
        
        store_types = ["Flagship", "Standard", "Express", "Warehouse", "Online"]
        
        store_key = 1
        stores_per_region = self.config.num_stores // len(regions_data)
        
        for region, details in regions_data.items():
            for i in range(stores_per_region):
                state = random.choice(details["states"])
                city = random.choice(details["cities"])
                territory = f"{region.value} Territory {(i % details['territory_count']) + 1}"
                store_type = random.choice(store_types)
                
                # Store metrics
                sqft = random.randint(5000, 50000) if store_type != "Online" else 0
                open_date = self.config.start_date - timedelta(days=random.randint(365, 3650))
                
                # Some stores closed
                is_closed = random.random() < 0.05
                close_date = None
                if is_closed:
                    close_date = self.config.start_date + timedelta(
                        days=random.randint(180, (self.config.end_date - self.config.start_date).days)
                    )
                
                self.stores.append({
                    "StoreKey": store_key,
                    "StoreID": f"STR-{store_key:04d}",
                    "StoreName": f"{city} {store_type} #{i+1}",
                    "StoreType": store_type,
                    "Region": region.value,
                    "Territory": territory,
                    "District": f"{region.value} District {(i % 3) + 1}",
                    "City": city,
                    "State": state,
                    "Country": "Canada" if region == Region.INTERNATIONAL else "USA",
                    "PostalCode": f"{random.randint(10000, 99999)}",
                    "Latitude": round(random.uniform(25, 50), 6),
                    "Longitude": round(random.uniform(-125, -70), 6),
                    "SquareFootage": sqft if sqft > 0 else None,
                    "OpenDate": open_date.isoformat(),
                    "CloseDate": close_date.isoformat() if close_date else None,
                    "IsActive": not is_closed,
                    "StoreManager": f"Manager_{random.randint(1000, 9999)}",
                    "RegionalManager": f"RM_{region.value[:3]}_{random.randint(1, 5)}",
                    "EmployeeCount": random.randint(10, 200) if store_type != "Online" else random.randint(50, 500),
                    "ParkingSpaces": random.randint(50, 500) if store_type != "Online" else None,
                    "HasServiceCenter": random.random() > 0.6,
                    "HasCafe": random.random() > 0.7,
                    "AnnualRent": round(random.uniform(50000, 500000), 2) if store_type != "Online" else None,
                    "TargetRevenue": round(random.uniform(1000000, 50000000), 2),
                })
                
                store_key += 1
        
        logger.info(f"Generated {len(self.stores)} store records")
    
    def _generate_dim_customer(self) -> None:
        """Generate customer dimension with segments, tiers, demographics."""
        logger.info("Generating DimCustomer...")
        
        first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
                       "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                      "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"]
        
        industries = ["Technology", "Healthcare", "Finance", "Manufacturing", "Retail", 
                      "Education", "Government", "Non-Profit", "Energy", "Transportation"]
        
        company_suffixes = ["Inc.", "Corp.", "LLC", "Ltd.", "Group", "Solutions", "Systems", "Services"]
        
        for customer_key in range(1, self.config.num_customers + 1):
            segment = random.choice(list(CustomerSegment))
            
            # Company vs Individual
            is_business = segment in [CustomerSegment.ENTERPRISE, CustomerSegment.MID_MARKET, 
                                       CustomerSegment.SMB, CustomerSegment.GOVERNMENT]
            
            if is_business:
                company_name = f"{random.choice(last_names)} {random.choice(company_suffixes)}"
                contact_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            else:
                company_name = None
                contact_name = f"{random.choice(first_names)} {random.choice(last_names)}"
            
            # Customer value metrics
            if segment == CustomerSegment.ENTERPRISE:
                annual_revenue = random.uniform(10000000, 500000000)
                employee_count = random.randint(1000, 100000)
                credit_limit = random.uniform(500000, 5000000)
            elif segment == CustomerSegment.MID_MARKET:
                annual_revenue = random.uniform(1000000, 50000000)
                employee_count = random.randint(100, 5000)
                credit_limit = random.uniform(100000, 1000000)
            elif segment == CustomerSegment.SMB:
                annual_revenue = random.uniform(100000, 5000000)
                employee_count = random.randint(10, 500)
                credit_limit = random.uniform(10000, 200000)
            elif segment == CustomerSegment.GOVERNMENT:
                annual_revenue = None
                employee_count = random.randint(50, 10000)
                credit_limit = random.uniform(100000, 2000000)
            else:  # Consumer
                annual_revenue = random.uniform(30000, 500000)
                employee_count = None
                credit_limit = random.uniform(1000, 50000)
            
            # Customer lifecycle
            first_purchase = self.config.start_date + timedelta(
                days=random.randint(0, (self.config.end_date - self.config.start_date).days // 2)
            )
            
            # Loyalty tier based on tenure and spend
            tenure_days = (self.config.end_date - first_purchase).days
            if tenure_days > 1000 and random.random() > 0.5:
                loyalty_tier = "Platinum"
            elif tenure_days > 500 and random.random() > 0.4:
                loyalty_tier = "Gold"
            elif tenure_days > 180 and random.random() > 0.3:
                loyalty_tier = "Silver"
            else:
                loyalty_tier = "Bronze"
            
            # Churn
            is_churned = random.random() < 0.08
            churn_date = None
            if is_churned:
                churn_date = first_purchase + timedelta(days=random.randint(90, tenure_days))
            
            region = random.choice(list(Region))
            
            self.customers.append({
                "CustomerKey": customer_key,
                "CustomerID": f"CUS-{customer_key:07d}",
                "CustomerName": company_name or contact_name,
                "ContactName": contact_name,
                "Segment": segment.value,
                "Industry": random.choice(industries) if is_business else None,
                "CompanySize": self._get_company_size(employee_count) if employee_count else None,
                "AnnualRevenue": round(annual_revenue, 2) if annual_revenue else None,
                "EmployeeCount": employee_count,
                "Region": region.value,
                "Country": "Canada" if region == Region.INTERNATIONAL else "USA",
                "State": random.choice(["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]),
                "City": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]),
                "PostalCode": f"{random.randint(10000, 99999)}",
                "Email": f"{contact_name.lower().replace(' ', '.')}@{(company_name or 'email').lower().replace(' ', '').replace('.', '')[:10]}.com",
                "Phone": f"+1-{random.randint(200, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                "FirstPurchaseDate": first_purchase.isoformat(),
                "LastPurchaseDate": (churn_date - timedelta(days=random.randint(1, 30))).isoformat() if churn_date else None,
                "LoyaltyTier": loyalty_tier,
                "LoyaltyPoints": random.randint(0, 100000) if loyalty_tier != "Bronze" else random.randint(0, 5000),
                "CreditLimit": round(credit_limit, 2),
                "PaymentTerms": random.choice(["Net 15", "Net 30", "Net 45", "Net 60", "Due on Receipt"]),
                "PreferredContactMethod": random.choice(["Email", "Phone", "Mail"]),
                "IsActive": not is_churned,
                "ChurnDate": churn_date.isoformat() if churn_date else None,
                "ChurnReason": random.choice(["Price", "Service", "Competition", "No Longer Needed", None]) if is_churned else None,
                "ReferralSource": random.choice(["Web Search", "Referral", "Trade Show", "Social Media", "Direct Mail", "Cold Call"]),
                "AccountManager": f"AM_{random.randint(1, 50)}" if is_business else None,
            })
        
        logger.info(f"Generated {len(self.customers)} customer records")
    
    def _get_company_size(self, employee_count: Optional[int]) -> Optional[str]:
        """Categorize company size based on employee count."""
        if employee_count is None:
            return None
        if employee_count < 50:
            return "Micro"
        if employee_count < 250:
            return "Small"
        if employee_count < 1000:
            return "Medium"
        if employee_count < 5000:
            return "Large"
        return "Enterprise"
    
    def _generate_dim_employee(self) -> None:
        """Generate employee dimension with hierarchy, tenure, quotas."""
        logger.info("Generating DimEmployee...")
        
        first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
        
        titles = {
            "Sales Rep": {"level": 1, "quota_range": (500000, 1500000)},
            "Senior Sales Rep": {"level": 2, "quota_range": (1000000, 3000000)},
            "Sales Manager": {"level": 3, "quota_range": (3000000, 8000000)},
            "Regional Sales Director": {"level": 4, "quota_range": (8000000, 20000000)},
            "VP Sales": {"level": 5, "quota_range": (20000000, 100000000)},
        }
        
        departments = ["Inside Sales", "Field Sales", "Enterprise Sales", "Channel Sales", "Government Sales"]
        
        for employee_key in range(1, self.config.num_employees + 1):
            title = random.choice(list(titles.keys()))
            title_info = titles[title]
            
            hire_date = self.config.start_date - timedelta(days=random.randint(0, 3650))
            
            # Termination
            is_terminated = random.random() < 0.12
            termination_date = None
            if is_terminated:
                termination_date = hire_date + timedelta(days=random.randint(180, 1825))
                if termination_date > self.config.end_date:
                    termination_date = None
                    is_terminated = False
            
            # Quota based on title
            annual_quota = random.uniform(*title_info["quota_range"])
            
            # Manager hierarchy
            manager_key = None
            if title_info["level"] < 5:
                # Assign a manager from higher level
                potential_managers = [
                    e["EmployeeKey"] for e in self.employees
                    if titles.get(e["Title"], {}).get("level", 0) > title_info["level"]
                ]
                if potential_managers:
                    manager_key = random.choice(potential_managers)
            
            region = random.choice(list(Region))
            
            self.employees.append({
                "EmployeeKey": employee_key,
                "EmployeeID": f"EMP-{employee_key:05d}",
                "FirstName": random.choice(first_names),
                "LastName": random.choice(last_names),
                "FullName": f"{random.choice(first_names)} {random.choice(last_names)}",
                "Email": f"emp{employee_key}@company.com",
                "Title": title,
                "TitleLevel": title_info["level"],
                "Department": random.choice(departments),
                "Region": region.value,
                "Territory": f"{region.value} Territory {random.randint(1, 5)}",
                "HireDate": hire_date.isoformat(),
                "TerminationDate": termination_date.isoformat() if termination_date else None,
                "IsActive": not is_terminated,
                "ManagerKey": manager_key,
                "AnnualQuota": round(annual_quota, 2),
                "QuotaQ1": round(annual_quota * 0.22, 2),  # Slightly lower Q1
                "QuotaQ2": round(annual_quota * 0.25, 2),
                "QuotaQ3": round(annual_quota * 0.25, 2),
                "QuotaQ4": round(annual_quota * 0.28, 2),  # Higher Q4
                "CommissionRate": round(random.uniform(0.02, 0.10), 4),
                "BaseSalary": round(random.uniform(40000, 200000), 2),
                "YearsExperience": random.randint(0, 30),
                "Education": random.choice(["High School", "Associate", "Bachelor", "Master", "MBA", "PhD"]),
                "Certifications": random.randint(0, 5),
            })
        
        logger.info(f"Generated {len(self.employees)} employee records")
    
    def _generate_dim_promotion(self) -> None:
        """Generate promotion dimension with types, costs, effectiveness."""
        logger.info("Generating DimPromotion...")
        
        promo_types = ["Percentage Off", "Dollar Off", "BOGO", "Bundle", "Free Shipping", 
                       "Loyalty Bonus", "Seasonal", "Clearance", "Flash Sale", "Member Exclusive"]
        
        channels = ["Online", "In-Store", "Email", "Mobile App", "Social Media", "All Channels"]
        
        for promo_key in range(1, self.config.num_promotions + 1):
            promo_type = random.choice(promo_types)
            
            # Promotion dates
            start_date = self.config.start_date + timedelta(
                days=random.randint(0, (self.config.end_date - self.config.start_date).days - 30)
            )
            duration = random.randint(1, 90)
            end_date = start_date + timedelta(days=duration)
            
            # Discount
            if promo_type == "Percentage Off":
                discount_pct = random.choice([5, 10, 15, 20, 25, 30, 40, 50])
                discount_amt = None
            elif promo_type == "Dollar Off":
                discount_pct = None
                discount_amt = random.choice([5, 10, 15, 20, 25, 50, 100])
            else:
                discount_pct = random.choice([10, 15, 20, 25])
                discount_amt = None
            
            # Requirements
            min_purchase = random.choice([0, 25, 50, 100, 250, 500]) if random.random() > 0.3 else None
            min_quantity = random.choice([1, 2, 3, 5]) if random.random() > 0.5 else None
            
            self.promotions.append({
                "PromotionKey": promo_key,
                "PromotionID": f"PROMO-{promo_key:04d}",
                "PromotionName": f"{promo_type} - {start_date.strftime('%b %Y')}",
                "PromotionType": promo_type,
                "Description": f"Special {promo_type.lower()} promotion",
                "StartDate": start_date.isoformat(),
                "EndDate": end_date.isoformat(),
                "DiscountPercent": discount_pct,
                "DiscountAmount": discount_amt,
                "MinPurchaseAmount": min_purchase,
                "MinQuantity": min_quantity,
                "MaxUsesPerCustomer": random.choice([1, 2, 5, 10, None]),
                "TotalBudget": round(random.uniform(10000, 500000), 2),
                "ActualCost": round(random.uniform(5000, 400000), 2),
                "Channel": random.choice(channels),
                "TargetSegment": random.choice([s.value for s in CustomerSegment] + [None]),
                "TargetCategory": random.choice([c.value for c in ProductCategory] + [None]),
                "PromoCode": f"{''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=8))}",
                "IsStackable": random.random() > 0.7,
                "IsActive": start_date <= date.today() <= end_date,
                "RedemptionCount": random.randint(100, 50000),
                "ExpectedLift": round(random.uniform(0.05, 0.30), 4),
                "ActualLift": round(random.uniform(0.02, 0.35), 4),
            })
        
        logger.info(f"Generated {len(self.promotions)} promotion records")
    
    def _generate_fact_sales(self) -> None:
        """Generate fact sales with realistic patterns and distributions."""
        logger.info(f"Generating {self.config.num_transactions} FactSales records...")
        
        # Build lookup indices for performance
        date_lookup = {d["Date"]: d["DateKey"] for d in self.dates}
        active_products = [p for p in self.products if p["IsActive"]]
        active_stores = [s for s in self.stores if s["IsActive"]]
        active_customers = [c for c in self.customers if c["IsActive"]]
        active_employees = [e for e in self.employees if e["IsActive"]]
        
        # Seasonal multipliers (index 0 = January)
        seasonal_weights = [0.85, 0.80, 0.90, 0.95, 1.00, 0.90, 0.85, 0.88, 0.95, 1.05, 1.20, 1.40]
        
        # Day of week weights (0 = Monday)
        dow_weights = [1.0, 1.05, 1.05, 1.10, 1.15, 0.70, 0.50]
        
        sale_key = 1
        
        # Distribute transactions across date range
        date_range_days = (self.config.end_date - self.config.start_date).days
        
        for _ in range(self.config.num_transactions):
            # Pick a date with seasonal/dow weighting
            for _ in range(10):  # Try to find a valid date
                days_offset = random.randint(0, date_range_days)
                sale_date = self.config.start_date + timedelta(days=days_offset)
                
                # Apply seasonal weight
                month_weight = seasonal_weights[sale_date.month - 1]
                dow_weight = dow_weights[sale_date.weekday()]
                
                if random.random() < (month_weight * dow_weight) / 1.5:
                    break
            
            date_key = date_lookup.get(sale_date.isoformat())
            if not date_key:
                continue
            
            # Select entities
            product = random.choice(active_products)
            store = random.choice(active_stores)
            customer = random.choice(active_customers)
            employee = random.choice(active_employees)
            
            # Check if promotion applies
            promotion = None
            promo_key = None
            for p in self.promotions:
                if p["StartDate"] <= sale_date.isoformat() <= p["EndDate"]:
                    if random.random() < 0.25:  # 25% of sales use a promotion
                        promotion = p
                        promo_key = p["PromotionKey"]
                        break
            
            # Calculate quantities and amounts
            quantity = self._get_realistic_quantity(product, customer)
            unit_price = product["UnitPrice"]
            unit_cost = product["UnitCost"]
            
            # Apply promotion discount
            discount_pct = 0
            discount_amt = 0
            if promotion:
                if promotion["DiscountPercent"]:
                    discount_pct = promotion["DiscountPercent"] / 100
                elif promotion["DiscountAmount"]:
                    discount_amt = promotion["DiscountAmount"]
            
            gross_amount = quantity * unit_price
            discount_amount = (gross_amount * discount_pct) + discount_amt
            net_amount = gross_amount - discount_amount
            cost_amount = quantity * unit_cost
            profit = net_amount - cost_amount
            margin = profit / net_amount if net_amount > 0 else 0
            
            # Shipping and tax
            shipping_cost = 0
            if store["StoreType"] == "Online":
                if net_amount < 50:
                    shipping_cost = random.uniform(5, 15)
                elif net_amount < 100:
                    shipping_cost = random.uniform(0, 10)
            
            tax_rate = random.uniform(0.05, 0.10)
            tax_amount = net_amount * tax_rate
            
            total_amount = net_amount + shipping_cost + tax_amount
            
            # Order and fulfillment
            order_id = f"ORD-{sale_date.strftime('%Y%m%d')}-{sale_key:07d}"
            
            # Returns (about 5% of orders)
            is_returned = random.random() < 0.05
            return_date = None
            return_reason = None
            return_quantity = 0
            if is_returned:
                return_date = sale_date + timedelta(days=random.randint(1, 30))
                return_reason = random.choice(["Defective", "Wrong Item", "Changed Mind", "Not as Described", "Better Price Found"])
                return_quantity = random.randint(1, quantity)
            
            self.sales.append({
                "SalesKey": sale_key,
                "OrderID": order_id,
                "OrderLineNumber": random.randint(1, 5),
                "DateKey": date_key,
                "ProductKey": product["ProductKey"],
                "StoreKey": store["StoreKey"],
                "CustomerKey": customer["CustomerKey"],
                "EmployeeKey": employee["EmployeeKey"],
                "PromotionKey": promo_key,
                "OrderDate": sale_date.isoformat(),
                "ShipDate": (sale_date + timedelta(days=random.randint(1, 7))).isoformat(),
                "DeliveryDate": (sale_date + timedelta(days=random.randint(3, 14))).isoformat() if store["StoreType"] == "Online" else sale_date.isoformat(),
                "Quantity": quantity,
                "UnitPrice": round(unit_price, 2),
                "UnitCost": round(unit_cost, 2),
                "GrossAmount": round(gross_amount, 2),
                "DiscountPercent": round(discount_pct * 100, 2),
                "DiscountAmount": round(discount_amount, 2),
                "NetAmount": round(net_amount, 2),
                "CostAmount": round(cost_amount, 2),
                "ShippingCost": round(shipping_cost, 2),
                "TaxRate": round(tax_rate * 100, 2),
                "TaxAmount": round(tax_amount, 2),
                "TotalAmount": round(total_amount, 2),
                "Profit": round(profit, 2),
                "ProfitMargin": round(margin * 100, 2),
                "PaymentMethod": random.choice(["Credit Card", "Debit Card", "ACH", "Wire", "Check", "PO"]),
                "SalesChannel": "Online" if store["StoreType"] == "Online" else random.choice(["In-Store", "Phone", "Fax"]),
                "OrderStatus": "Returned" if is_returned else random.choice(["Completed", "Completed", "Completed", "Pending", "Shipped"]),
                "IsReturned": is_returned,
                "ReturnDate": return_date.isoformat() if return_date else None,
                "ReturnReason": return_reason,
                "ReturnQuantity": return_quantity,
                "CustomerSatisfactionScore": random.choice([1, 2, 3, 4, 4, 5, 5, 5, None]) if random.random() > 0.7 else None,
            })
            
            sale_key += 1
            
            if sale_key % 50000 == 0:
                logger.info(f"Generated {sale_key} sales records...")
        
        logger.info(f"Generated {len(self.sales)} FactSales records")
    
    def _get_realistic_quantity(self, product: Dict, customer: Dict) -> int:
        """Get realistic quantity based on product and customer."""
        # Enterprise customers buy more
        if customer["Segment"] == CustomerSegment.ENTERPRISE.value:
            base_qty = random.randint(5, 100)
        elif customer["Segment"] == CustomerSegment.MID_MARKET.value:
            base_qty = random.randint(2, 50)
        elif customer["Segment"] == CustomerSegment.SMB.value:
            base_qty = random.randint(1, 20)
        else:
            base_qty = random.randint(1, 5)
        
        # Office supplies bought in larger quantities
        if product["Category"] == ProductCategory.OFFICE_SUPPLIES.value:
            base_qty *= random.randint(1, 3)
        
        # Software typically 1 license at a time (unless enterprise)
        if product["Category"] == ProductCategory.SOFTWARE.value:
            if customer["Segment"] != CustomerSegment.ENTERPRISE.value:
                base_qty = min(base_qty, random.randint(1, 10))
        
        return max(1, base_qty)
    
    def to_csv(self, table_name: str, data: List[Dict[str, Any]]) -> str:
        """Convert table data to CSV string."""
        if not data:
            return ""
        
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()
    
    def save_all_csv(self, output_dir: Path) -> Dict[str, Path]:
        """
        Save all tables as CSV files.
        
        Returns:
            Dictionary mapping table names to file paths.
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        tables = self.generate_all()
        paths = {}
        
        for table_name, data in tables.items():
            file_path = output_dir / f"{table_name}.csv"
            csv_content = self.to_csv(table_name, data)
            
            with open(file_path, "w", newline="", encoding="utf-8") as f:
                f.write(csv_content)
            
            paths[table_name] = file_path
            logger.info(f"Saved {table_name} to {file_path} ({len(data)} rows)")
        
        return paths
