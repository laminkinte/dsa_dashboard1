import streamlit as st
import pandas as pd
import numpy as np
import warnings
from datetime import datetime, timedelta
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="DSA Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS with GMD currency
st.markdown("""
    <style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A8A;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1E40AF;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #F8FAFC;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #3B82F6;
        margin-bottom: 0.5rem;
    }
    .dataframe {
        font-size: 0.9rem;
    }
    .stDownloadButton button {
        width: 100%;
        background-color: #10B981;
        color: white;
    }
    .currency-gmd {
        font-size: 1.2rem;
        font-weight: bold;
        color: #059669;
    }
    .column-display {
        font-size: 0.8rem;
        background-color: #f1f5f9;
        padding: 0.5rem;
        border-radius: 0.25rem;
        margin-bottom: 0.5rem;
        max-height: 200px;
        overflow-y: auto;
    }
    </style>
""", unsafe_allow_html=True)

# Main header
st.markdown('<div class="main-header">📊 DSA Performance Analysis Dashboard (GMD)</div>', unsafe_allow_html=True)

# Initialize session state for file storage
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = {}
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = {}
if 'report_1_data' not in st.session_state:
    st.session_state.report_1_data = {}
if 'report_2_data' not in st.session_state:
    st.session_state.report_2_data = {}
if 'filtered_report_1' not in st.session_state:
    st.session_state.filtered_report_1 = {}
if 'filtered_report_2' not in st.session_state:
    st.session_state.filtered_report_2 = {}
if 'payment_report_data' not in st.session_state:
    st.session_state.payment_report_data = {}
if 'filtered_payment_data' not in st.session_state:
    st.session_state.filtered_payment_data = {}
if 'show_columns' not in st.session_state:
    st.session_state.show_columns = True
if 'master_report_data' not in st.session_state:
    st.session_state.master_report_data = {}

def clean_mobile_number(mobile):
    """Clean mobile numbers to ensure consistency"""
    if pd.isna(mobile):
        return None
    mobile_str = str(mobile)
    mobile_clean = ''.join(filter(str.isdigit, mobile_str))
    if len(mobile_clean) == 7:
        return mobile_clean
    elif len(mobile_clean) > 7:
        return mobile_clean[-7:]
    else:
        return mobile_clean

def safe_str_access(series):
    """Safely apply string operations to a series"""
    if series.dtype == 'object':
        return series.astype(str).str.strip()
    else:
        return series.astype(str).str.strip()

def clean_currency_amount(amount):
    """Clean currency amounts, handling GMD specifically"""
    if pd.isna(amount):
        return 0
    amount_str = str(amount)
    # Remove GMD symbol, commas, and spaces
    amount_clean = amount_str.replace('GMD', '').replace(',', '').replace(' ', '').strip()
    try:
        return float(amount_clean)
    except:
        return 0

def find_column(df, possible_names):
    """Find a column in dataframe from list of possible names"""
    for name in possible_names:
        if name in df.columns:
            return name
    return None

def parse_date(date_str, date_formats=None):
    """Parse date string with multiple formats"""
    if pd.isna(date_str):
        return None
    
    if date_formats is None:
        date_formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%dT%H:%M:%S.%f'
        ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except (ValueError, TypeError):
            continue
    
    # Try pandas to_datetime as fallback
    try:
        return pd.to_datetime(date_str)
    except:
        return None

def filter_by_date(df, date_col, start_date, end_date):
    """Filter dataframe by date range"""
    if df.empty or date_col not in df.columns:
        return df
    
    df_filtered = df.copy()
    
    # Try to parse the date column
    try:
        # Create a new column with parsed dates
        df_filtered['_parsed_date'] = df_filtered[date_col].apply(lambda x: parse_date(x))
        
        # Filter by date range
        if start_date:
            start_date_dt = datetime.combine(start_date, datetime.min.time())
            df_filtered = df_filtered[df_filtered['_parsed_date'] >= start_date_dt]
        
        if end_date:
            end_date_dt = datetime.combine(end_date, datetime.max.time())
            df_filtered = df_filtered[df_filtered['_parsed_date'] <= end_date_dt]
        
        # Drop the temporary column
        df_filtered = df_filtered.drop(columns=['_parsed_date'], errors='ignore')
        
    except Exception as e:
        st.sidebar.warning(f"Could not filter by date: {str(e)}")
        return df
    
    return df_filtered

def find_date_column(df):
    """Find date column in dataframe"""
    date_cols = ['created_at', 'Created At', 'createdAt', 'CreatedAt', 
                 'date', 'Date', 'timestamp', 'Timestamp', 'time', 'Time',
                 'Registration Date', 'Updated At', 'Created At']
    
    for col in date_cols:
        if col in df.columns:
            return col
    
    # Look for columns with 'date' or 'time' in name
    for col in df.columns:
        col_lower = str(col).lower()
        if 'date' in col_lower or 'time' in col_lower or 'created' in col_lower:
            return col
    
    return None

def process_report_1(onboarding_df, ticket_df, conversion_df, deposit_df, scan_df, start_date=None, end_date=None):
    """Process data for Report 1 with date filtering - EXACT FORMAT as sample"""
    try:
        # Clean column names
        for df in [onboarding_df, ticket_df, conversion_df, deposit_df, scan_df]:
            df.columns = [str(col).strip() for col in df.columns]
        
        # Apply date filtering if dates are provided
        if start_date or end_date:
            # Find date columns in each dataframe
            ticket_date_col = find_date_column(ticket_df)
            deposit_date_col = find_date_column(deposit_df)
            scan_date_col = find_date_column(scan_df)
            onboarding_date_col = find_date_column(onboarding_df)
            
            # Apply date filters
            if ticket_date_col:
                ticket_df = filter_by_date(ticket_df, ticket_date_col, start_date, end_date)
            
            if deposit_date_col:
                deposit_df = filter_by_date(deposit_df, deposit_date_col, start_date, end_date)
            
            if scan_date_col:
                scan_df = filter_by_date(scan_df, scan_date_col, start_date, end_date)
            
            if onboarding_date_col:
                onboarding_df = filter_by_date(onboarding_df, onboarding_date_col, start_date, end_date)
        
        # Fix ticket data if needed
        if ticket_df.shape[1] == 29:
            ticket_df.columns = [
                "user_id", "transaction_id", "sub_transaction_id", "entity_name",
                "full_name", "created_by", "status", "internal_status", "service_name",
                "product_name", "transaction_type", "amount", "before_balance", "after_balance",
                "ucp_name", "wallet_name", "pouch_name", "reference", "error_code", "error_message",
                "vendor_transaction_id", "vendor_response_code", "vendor_message", "slug", "remarks",
                "created_at", "business_hierarchy", "parent_user_id", "parent_full_name"
            ]
        
        # Rename columns for consistency
        name_cols = ["full_name", "Full Name", "Name"]
        name_col = find_column(onboarding_df, name_cols)
        
        if name_col is None:
            onboarding_df["full_name"] = "Unknown"
        else:
            onboarding_df = onboarding_df.rename(columns={name_col: "full_name"})
        
        # Safely rename columns
        dsa_mobile_col = find_column(onboarding_df, ["Customer Referrer Mobile", "dsa_mobile", "Agent Mobile", "Referrer Mobile"])
        if dsa_mobile_col:
            onboarding_df = onboarding_df.rename(columns={dsa_mobile_col: "dsa_mobile"})
        else:
            onboarding_df["dsa_mobile"] = "Unknown"
        
        customer_mobile_col = find_column(onboarding_df, ["Mobile", "customer_mobile", "Customer Mobile", "User Mobile"])
        if customer_mobile_col:
            onboarding_df = onboarding_df.rename(columns={customer_mobile_col: "customer_mobile"})
        else:
            onboarding_df["customer_mobile"] = "Unknown"
        
        # Clean deposit customer column
        deposit_customer_col = find_column(deposit_df, ['customer_mobile', 'Customer Mobile', 'Mobile', 'User Identifier', 'user_id', 'User ID'])
        
        if deposit_customer_col is None:
            st.error("No suitable customer/mobile column found in Deposit data")
            return None
        
        deposit_df = deposit_df.rename(columns={deposit_customer_col: "customer_mobile"})
        
        # Handle conversion data if provided
        if not conversion_df.empty:
            conversion_dsa_col = find_column(conversion_df, ["Agent Mobile", "dsa_mobile", "DSA Mobile", "Referrer Mobile"])
            if conversion_dsa_col:
                conversion_df = conversion_df.rename(columns={conversion_dsa_col: "dsa_mobile"})
            
            deposit_count_col = find_column(conversion_df, ["Deposit Count", "deposit_count", "Deposits"])
            if deposit_count_col:
                conversion_df = conversion_df.rename(columns={deposit_count_col: "deposit_count"})
        
        # CRITICAL: Clean ticket customer column - Look for customer identifier in ticket data
        ticket_customer_col = None
        
        # First, check if we have the standard column names from your data
        if "User Identifier" in ticket_df.columns:
            ticket_df = ticket_df.rename(columns={"User Identifier": "customer_mobile"})
            ticket_customer_col = "customer_mobile"
        elif "user_id" in ticket_df.columns:
            ticket_df = ticket_df.rename(columns={"user_id": "customer_mobile"})
            ticket_customer_col = "customer_mobile"
        elif "Created By" in ticket_df.columns:
            ticket_df = ticket_df.rename(columns={"Created By": "customer_mobile"})
            ticket_customer_col = "customer_mobile"
        elif "created_by" in ticket_df.columns:
            ticket_df = ticket_df.rename(columns={"created_by": "customer_mobile"})
            ticket_customer_col = "customer_mobile"
        else:
            # Try to find any customer column
            ticket_customer_col = find_column(ticket_df, ["User Identifier", "user_id", "Created By", "created_by", "customer_mobile", "Customer Mobile", "Mobile"])
            if ticket_customer_col:
                ticket_df = ticket_df.rename(columns={ticket_customer_col: "customer_mobile"})
                ticket_customer_col = "customer_mobile"
            else:
                st.error(f"No suitable customer column found in Ticket data. Available columns: {list(ticket_df.columns)}")
                return None
        
        # CRITICAL: Clean scan customer column
        scan_customer_col = find_column(scan_df, ['Created By', 'Customer Mobile', 'Mobile', 'User Identifier', 'user_id', 'customer_mobile'])
        
        if scan_customer_col is None:
            st.error(f"No suitable customer column found in Scan data. Available columns: {list(scan_df.columns)}")
            return None
        
        scan_df = scan_df.rename(columns={scan_customer_col: "customer_mobile"})
        
        # Clean mobile numbers
        for df, col in [(onboarding_df, "customer_mobile"), (onboarding_df, "dsa_mobile"),
                        (deposit_df, "customer_mobile"), (ticket_df, "customer_mobile"),
                        (scan_df, "customer_mobile"), (conversion_df, "dsa_mobile")]:
            if col in df.columns:
                df[col] = safe_str_access(df[col])
        
        # CRITICAL: Identify deposit transactions (CR - Credit/Customer Deposits)
        deposit_tx_type_col = find_column(deposit_df, ["transaction_type", "Transaction Type", "TransactionType", "Type"])
        if deposit_tx_type_col and "transaction_type" not in deposit_df.columns:
            deposit_df = deposit_df.rename(columns={deposit_tx_type_col: "transaction_type"})
        
        # Filter for only deposit transactions (CR)
        if "transaction_type" in deposit_df.columns:
            deposit_df["transaction_type"] = safe_str_access(deposit_df["transaction_type"])
            # Filter for CR (Credit/Deposit) transactions only
            original_deposit_count = len(deposit_df)
            deposit_df = deposit_df[deposit_df["transaction_type"].str.upper().isin(["CR", "DEPOSIT", "C"])]
            if original_deposit_count > 0:
                st.info(f"Filtered deposit data: {original_deposit_count} → {len(deposit_df)} CR transactions")
        
        # CRITICAL: Clean and filter ticket data
        # 1. Filter for Customer entity only (not Merchant)
        if "Entity Name" in ticket_df.columns:
            ticket_df["Entity Name"] = safe_str_access(ticket_df["Entity Name"])
            original_ticket_count = len(ticket_df)
            ticket_df = ticket_df[ticket_df["Entity Name"].str.lower() == "customer"]
            st.info(f"Filtered ticket data (Customer only): {original_ticket_count} → {len(ticket_df)}")
        
        # 2. Filter for DR transactions only (ticket purchases)
        if "Transaction Type" in ticket_df.columns:
            ticket_df["Transaction Type"] = safe_str_access(ticket_df["Transaction Type"])
            original_ticket_count = len(ticket_df)
            ticket_df = ticket_df[ticket_df["Transaction Type"].str.upper().isin(["DR", "DEBIT", "D"])]
            st.info(f"Filtered ticket data (DR only): {original_ticket_count} → {len(ticket_df)}")
        elif "transaction_type" in ticket_df.columns:
            ticket_df["transaction_type"] = safe_str_access(ticket_df["transaction_type"])
            original_ticket_count = len(ticket_df)
            ticket_df = ticket_df[ticket_df["transaction_type"].str.upper().isin(["DR", "DEBIT", "D"])]
            st.info(f"Filtered ticket data (DR only): {original_ticket_count} → {len(ticket_df)}")
        
        # CRITICAL: Clean numeric columns for ticket data
        if "Amount" in ticket_df.columns:
            ticket_df["ticket_amount"] = ticket_df["Amount"].apply(clean_currency_amount)
        elif "amount" in ticket_df.columns:
            ticket_df["ticket_amount"] = ticket_df["amount"].apply(clean_currency_amount)
        else:
            ticket_df["ticket_amount"] = 0
        
        # CRITICAL: Clean scan data
        if "Transaction Type" in scan_df.columns:
            scan_df["Transaction Type"] = safe_str_access(scan_df["Transaction Type"])
            original_scan_count = len(scan_df)
            # Filter for DR transactions only (scan to send)
            scan_df = scan_df[scan_df["Transaction Type"].str.upper().isin(["DR", "DEBIT", "D"])]
            st.info(f"Filtered scan data (DR only): {original_scan_count} → {len(scan_df)}")
        elif "transaction_type" in scan_df.columns:
            scan_df["transaction_type"] = safe_str_access(scan_df["transaction_type"])
            original_scan_count = len(scan_df)
            scan_df = scan_df[scan_df["transaction_type"].str.upper().isin(["DR", "DEBIT", "D"])]
            st.info(f"Filtered scan data (DR only): {original_scan_count} → {len(scan_df)}")
        
        # Clean numeric columns for scan data
        if "Amount" in scan_df.columns:
            scan_df["scan_amount"] = scan_df["Amount"].apply(clean_currency_amount)
        elif "amount" in scan_df.columns:
            scan_df["scan_amount"] = scan_df["amount"].apply(clean_currency_amount)
        else:
            scan_df["scan_amount"] = 0
        
        # CRITICAL: Validate we have required columns before proceeding
        required_onboarding_cols = ["dsa_mobile", "customer_mobile"]
        if not all(col in onboarding_df.columns for col in required_onboarding_cols):
            st.error(f"Missing required columns in onboarding data: {required_onboarding_cols}")
            st.error(f"Available columns: {list(onboarding_df.columns)}")
            return None
        
        # CRITICAL: Ensure no null DSA mobile numbers (customers must be onboarded by a DSA)
        original_onboarded_count = len(onboarding_df)
        onboarding_df = onboarding_df.dropna(subset=["dsa_mobile"])
        onboarding_df = onboarding_df[onboarding_df["dsa_mobile"].astype(str).str.strip() != ""]
        if original_onboarded_count > 0:
            st.info(f"Valid onboarded customers with DSA: {original_onboarded_count} → {len(onboarding_df)}")
        
        # CRITICAL: Aggregate ticket data - only count customers with POSITIVE ticket amounts
        if not ticket_df.empty:
            # Group by customer and sum ticket amounts
            ticket_agg = ticket_df.groupby("customer_mobile").agg(
                ticket_amount=("ticket_amount", "sum"),
                ticket_count=("ticket_amount", lambda x: (x > 0).sum())
            ).reset_index()
            # Only mark as bought_ticket if they have a positive ticket amount
            ticket_agg["bought_ticket"] = (ticket_agg["ticket_amount"] > 0).astype(int)
            
        else:
            ticket_agg = pd.DataFrame(columns=["customer_mobile", "ticket_amount", "ticket_count", "bought_ticket"])
        
        # CRITICAL: Aggregate scan data - only count customers with POSITIVE scan amounts
        if not scan_df.empty:
            scan_summary = scan_df.groupby("customer_mobile").agg(
                scan_amount=("scan_amount", "sum"),
                scan_count=("scan_amount", "count")
            ).reset_index()
            # Only mark as did_scan if they have a positive scan amount
            scan_summary["did_scan"] = (scan_summary["scan_amount"] > 0).astype(int)
            
        else:
            scan_summary = pd.DataFrame(columns=["customer_mobile", "scan_amount", "scan_count", "did_scan"])
        
        # Get unique depositors from CR transactions
        unique_depositors = deposit_df[["customer_mobile"]].drop_duplicates().assign(deposited=1)
        
        # Create onboarded customers table
        onboarded_customers = onboarding_df[["dsa_mobile", "customer_mobile", "full_name"]].copy()
        onboarded_customers = onboarded_customers.drop_duplicates(subset=["customer_mobile"])
        
        # Merge all data
        onboarded_customers = onboarded_customers.merge(
            ticket_agg[["customer_mobile", "bought_ticket", "ticket_amount"]],
            on="customer_mobile", 
            how="left"
        ).merge(
            scan_summary[["customer_mobile", "did_scan", "scan_amount"]],
            on="customer_mobile", 
            how="left"
        ).merge(
            unique_depositors,
            on="customer_mobile", 
            how="left"
        )
        
        # Fill NaN values
        onboarded_customers["bought_ticket"] = onboarded_customers["bought_ticket"].fillna(0).astype(int)
        onboarded_customers["did_scan"] = onboarded_customers["did_scan"].fillna(0).astype(int)
        onboarded_customers["deposited"] = onboarded_customers["deposited"].fillna(0).astype(int)
        onboarded_customers["ticket_amount"] = onboarded_customers["ticket_amount"].fillna(0)
        onboarded_customers["scan_amount"] = onboarded_customers["scan_amount"].fillna(0)
        
        # CRITICAL: Create qualified customers table - EXACTLY as in sample
        # A customer qualifies if:
        # 1. They were onboarded by a DSA (dsa_mobile exists)
        # 2. They deposited (deposited == 1)
        # 3. They either bought a ticket (bought_ticket == 1 AND ticket_amount > 0) 
        #    OR did a scan (did_scan == 1 AND scan_amount > 0)
        
        # First filter: deposited = 1
        deposited_customers = onboarded_customers[onboarded_customers["deposited"] == 1].copy()
        
        # Second filter: either bought ticket OR did scan
        qualified_customers = deposited_customers[
            (deposited_customers["bought_ticket"] == 1) | 
            (deposited_customers["did_scan"] == 1)
        ].copy()
        
        # Third filter: ensure positive amounts
        qualified_customers = qualified_customers[
            (qualified_customers["ticket_amount"] > 0) | 
            (qualified_customers["scan_amount"] > 0)
        ]
        
        # CRITICAL: Additional validation - ensure DSA mobile is not empty
        qualified_customers = qualified_customers[qualified_customers["dsa_mobile"].astype(str).str.strip() != ""]
        
        # Sort and add running counts - EXACT FORMAT as sample
        if not qualified_customers.empty:
            qualified_customers = qualified_customers.sort_values(["dsa_mobile", "customer_mobile"])
            
            # Create a clean table with the exact format from sample
            result_rows = []
            
            for dsa_mobile in qualified_customers["dsa_mobile"].unique():
                dsa_customers = qualified_customers[qualified_customers["dsa_mobile"] == dsa_mobile].copy()
                dsa_customers = dsa_customers.sort_values("customer_mobile")
                
                # Add summary columns only for first customer of each DSA
                customer_count = len(dsa_customers)
                deposit_count = dsa_customers["deposited"].sum()
                ticket_count = dsa_customers["bought_ticket"].sum()
                scan_count = dsa_customers["did_scan"].sum()
                payment = customer_count * 40  # GMD 40 per customer
                
                # First customer row with summary
                first_customer = dsa_customers.iloc[0].copy()
                first_row = {
                    'dsa_mobile': dsa_mobile,
                    'customer_mobile': first_customer['customer_mobile'],
                    'full_name': first_customer['full_name'],
                    'bought_ticket': first_customer['bought_ticket'],
                    'ticket_amount': first_customer['ticket_amount'],
                    'did_scan': first_customer['did_scan'],
                    'scan_amount': first_customer['scan_amount'],
                    'deposited': first_customer['deposited'],
                    'Customer Count': customer_count,
                    'Deposit Count': deposit_count,
                    'Ticket Count': ticket_count,
                    'Scan To Send Count': scan_count,
                    'Payment (Customer Count *40)': payment
                }
                result_rows.append(first_row)
                
                # Remaining customer rows without summary
                for idx in range(1, len(dsa_customers)):
                    customer = dsa_customers.iloc[idx]
                    row = {
                        'dsa_mobile': dsa_mobile,
                        'customer_mobile': customer['customer_mobile'],
                        'full_name': customer['full_name'],
                        'bought_ticket': customer['bought_ticket'],
                        'ticket_amount': customer['ticket_amount'],
                        'did_scan': customer['did_scan'],
                        'scan_amount': customer['scan_amount'],
                        'deposited': customer['deposited'],
                        'Customer Count': '',
                        'Deposit Count': '',
                        'Ticket Count': '',
                        'Scan To Send Count': '',
                        'Payment (Customer Count *40)': ''
                    }
                    result_rows.append(row)
            
            # Create the final qualified customers dataframe
            qualified_customers_final = pd.DataFrame(result_rows)
            
            # Ensure proper column order
            columns_order = [
                'dsa_mobile', 'customer_mobile', 'full_name', 'bought_ticket',
                'ticket_amount', 'did_scan', 'scan_amount', 'deposited',
                'Customer Count', 'Deposit Count', 'Ticket Count', 
                'Scan To Send Count', 'Payment (Customer Count *40)'
            ]
            qualified_customers_final = qualified_customers_final[columns_order]
        else:
            qualified_customers_final = pd.DataFrame(columns=[
                'dsa_mobile', 'customer_mobile', 'full_name', 'bought_ticket',
                'ticket_amount', 'did_scan', 'scan_amount', 'deposited',
                'Customer Count', 'Deposit Count', 'Ticket Count', 
                'Scan To Send Count', 'Payment (Customer Count *40)'
            ])
        
        # Create DSA summary
        dsa_summary_all = onboarded_customers.groupby("dsa_mobile").agg(
            Customer_Count=("customer_mobile", "count"),
            Customers_who_deposited=("deposited", "sum"),
            Customers_who_bought_ticket=("bought_ticket", "sum"),
            Customers_who_did_scan=("did_scan", "sum"),
            Total_Ticket_Amount=("ticket_amount", "sum"),
            Total_Scan_Amount=("scan_amount", "sum")
        ).reset_index()
        
        if not conversion_df.empty and "dsa_mobile" in conversion_df.columns:
            dsa_summary_all = dsa_summary_all.merge(
                conversion_df[["dsa_mobile", "deposit_count"]].drop_duplicates(),
                on="dsa_mobile",
                how="left"
            )
        
        # Calculate conversion rates
        dsa_summary_all["Ticket_Conversion_Rate"] = (dsa_summary_all["Customers_who_bought_ticket"] / 
                                                     dsa_summary_all["Customer_Count"].replace(0, 1) * 100).round(2)
        dsa_summary_all["Scan_Conversion_Rate"] = (dsa_summary_all["Customers_who_did_scan"] / 
                                                   dsa_summary_all["Customer_Count"].replace(0, 1) * 100).round(2)
        dsa_summary_all["Deposit_Conversion_Rate"] = (dsa_summary_all["Customers_who_deposited"] / 
                                                      dsa_summary_all["Customer_Count"].replace(0, 1) * 100).round(2)
        
        return {
            "qualified_customers": qualified_customers_final,
            "dsa_summary": dsa_summary_all,
            "onboarded_customers": onboarded_customers,
            "ticket_details": ticket_df,
            "scan_details": scan_df,
            "deposit_details": deposit_df,
            "filtered_dates": {"start_date": start_date, "end_date": end_date}
        }
        
    except Exception as e:
        st.error(f"Error processing Report 1: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

def process_report_2(onboarding_df, deposit_df, ticket_df, scan_df, start_date=None, end_date=None):
    """Process data for Report 2 - ONLY NO ONBOARDING customers with exact sample format"""
    try:
        st.info("Processing Report 2: Analyzing NO ONBOARDING customers...")
        
        # Create copies to avoid modifying original data
        onboarding_df = onboarding_df.copy()
        deposit_df = deposit_df.copy()
        ticket_df = ticket_df.copy()
        scan_df = scan_df.copy()
        
        # Store original dataframes for debugging
        original_deposit_df = deposit_df.copy()
        original_ticket_df = ticket_df.copy()
        original_scan_df = scan_df.copy()
        
        # Clean column names consistently
        for df in [onboarding_df, deposit_df, ticket_df, scan_df]:
            df.columns = [str(col).strip() for col in df.columns]
        
        # Apply date filtering if dates are provided
        if start_date or end_date:
            # Find date columns in each dataframe
            deposit_date_col = find_date_column(deposit_df)
            ticket_date_col = find_date_column(ticket_df)
            scan_date_col = find_date_column(scan_df)
            onboarding_date_col = find_date_column(onboarding_df)
            
            # Apply date filters
            if deposit_date_col:
                deposit_df = filter_by_date(deposit_df, deposit_date_col, start_date, end_date)
                st.info(f"Deposit data filtered to {len(deposit_df)} rows")
            
            if ticket_date_col:
                ticket_df = filter_by_date(ticket_df, ticket_date_col, start_date, end_date)
                st.info(f"Ticket data filtered to {len(ticket_df)} rows")
            
            if scan_date_col:
                scan_df = filter_by_date(scan_df, scan_date_col, start_date, end_date)
                st.info(f"Scan data filtered to {len(scan_df)} rows")
            
            if onboarding_date_col:
                onboarding_df = filter_by_date(onboarding_df, onboarding_date_col, start_date, end_date)
                st.info(f"Onboarding data filtered to {len(onboarding_df)} rows")
        
        # DEBUG: Show data shapes
        st.info(f"Data shapes - Onboarding: {onboarding_df.shape}, Deposit: {deposit_df.shape}, Ticket: {ticket_df.shape}, Scan: {scan_df.shape}")
        
        # 1. IDENTIFY ALL DEPOSITORS FROM DEPOSIT DATA
        # Find deposit customer and DSA columns with more flexible matching
        deposit_customer_col = None
        deposit_dsa_col = None
        deposit_tx_type_col = None
        
        # Look for customer columns in deposit data
        deposit_customer_options = [
            'User Identifier', 'user_id', 'UserIdentifier', 'Customer Mobile', 
            'customer_mobile', 'Mobile', 'USER_ID', 'User ID', 'UserID',
            'customer_id', 'Customer_ID', 'User', 'Created By', 'created_by'
        ]
        
        deposit_dsa_options = [
            'Created By', 'created_by', 'CreatedBy', 'DSA Mobile', 'dsa_mobile', 
            'Agent Mobile', 'agent_mobile', 'Referrer Mobile', 'Referrer_ID',
            'Agent_ID', 'agent_id', 'dsa_id', 'DSA_ID'
        ]
        
        deposit_tx_type_options = [
            'Transaction Type', 'transaction_type', 'TransactionType', 'Type', 
            'transaction type', 'txn_type', 'TXN_TYPE', 'transactionType', 'trx_type'
        ]
        
        # Find the actual column names
        for col in deposit_df.columns:
            col_lower = str(col).lower()
            if any(opt.lower() in col_lower for opt in deposit_customer_options):
                if deposit_customer_col is None:
                    deposit_customer_col = col
                    st.info(f"Found deposit customer column: {col}")
            
            if any(opt.lower() in col_lower for opt in deposit_dsa_options):
                if deposit_dsa_col is None:
                    deposit_dsa_col = col
                    st.info(f"Found deposit DSA column: {col}")
            
            if any(opt.lower() in col_lower for opt in deposit_tx_type_options):
                if deposit_tx_type_col is None:
                    deposit_tx_type_col = col
                    st.info(f"Found deposit transaction type column: {col}")
        
        # If not found by pattern, check for exact matches
        if deposit_customer_col is None:
            for col in deposit_customer_options:
                if col in deposit_df.columns:
                    deposit_customer_col = col
                    break
        
        if deposit_dsa_col is None:
            for col in deposit_dsa_options:
                if col in deposit_df.columns:
                    deposit_dsa_col = col
                    break
        
        if deposit_tx_type_col is None:
            for col in deposit_tx_type_options:
                if col in deposit_df.columns:
                    deposit_tx_type_col = col
                    break
        
        # DEBUG: Show what columns were found
        st.info(f"Deposit - Customer col: {deposit_customer_col}, DSA col: {deposit_dsa_col}, Tx Type col: {deposit_tx_type_col}")
        
        # Validate required columns
        if deposit_customer_col is None or deposit_dsa_col is None:
            st.error(f"Cannot find required columns in deposit data. Available columns: {list(deposit_df.columns)}")
            return None
        
        # 2. CLEAN AND PREPARE DEPOSIT DATA
        # Clean mobile numbers in deposit data
        def clean_mobile_for_report2(mobile):
            """Enhanced mobile number cleaning for Report 2"""
            if pd.isna(mobile):
                return None
            mobile_str = str(mobile)
            # Remove all non-digit characters
            mobile_clean = ''.join(filter(str.isdigit, mobile_str))
            # Handle various formats
            if mobile_clean.startswith('220'):
                mobile_clean = mobile_clean[3:]  # Remove country code
            if len(mobile_clean) >= 7:
                return mobile_clean[-7:]  # Take last 7 digits
            return mobile_clean if mobile_clean else None
        
        # Apply cleaning to deposit data
        deposit_df['customer_mobile_clean'] = deposit_df[deposit_customer_col].apply(clean_mobile_for_report2)
        deposit_df['dsa_mobile_clean'] = deposit_df[deposit_dsa_col].apply(clean_mobile_for_report2)
        
        # Filter out rows where customer or DSA mobile is missing/empty
        original_deposit_count = len(deposit_df)
        deposit_df = deposit_df.dropna(subset=['customer_mobile_clean', 'dsa_mobile_clean'])
        deposit_df = deposit_df[(deposit_df['customer_mobile_clean'] != '') & (deposit_df['dsa_mobile_clean'] != '')]
        st.info(f"Deposit data after cleaning: {original_deposit_count} → {len(deposit_df)} rows")
        
        # Filter for CR (deposit) transactions only if transaction type column exists
        if deposit_tx_type_col and deposit_tx_type_col in deposit_df.columns:
            deposit_df[deposit_tx_type_col] = deposit_df[deposit_tx_type_col].astype(str).str.strip().str.upper()
            original_count = len(deposit_df)
            # Include more variations of deposit transactions
            deposit_df = deposit_df[deposit_df[deposit_tx_type_col].isin(['CR', 'DEPOSIT', 'C', 'CREDIT', 'D'])]
            st.info(f"Deposit data filtered to CR transactions: {original_count} → {len(deposit_df)} rows")
        
        # 3. GET ONBOARDING MAPPING
        # Find columns in onboarding data
        onboarding_customer_col = None
        onboarding_dsa_col = None
        
        # Look for columns in onboarding data
        for col in onboarding_df.columns:
            col_lower = str(col).lower()
            if 'mobile' in col_lower and ('customer' in col_lower or 'user' in col_lower):
                onboarding_customer_col = col
            elif 'referrer' in col_lower or 'dsa' in col_lower or 'agent' in col_lower:
                onboarding_dsa_col = col
        
        # If not found by pattern, check common names
        if onboarding_customer_col is None:
            for col in ['Mobile', 'Customer Mobile', 'customer_mobile', 'User Mobile']:
                if col in onboarding_df.columns:
                    onboarding_customer_col = col
                    break
        
        if onboarding_dsa_col is None:
            for col in ['Customer Referrer Mobile', 'dsa_mobile', 'Referrer Mobile', 'Agent Mobile']:
                if col in onboarding_df.columns:
                    onboarding_dsa_col = col
                    break
        
        # Create onboarding mapping
        onboarding_map = {}
        if onboarding_customer_col and onboarding_dsa_col:
            # Clean mobile numbers in onboarding data
            onboarding_df['customer_mobile_clean'] = onboarding_df[onboarding_customer_col].apply(clean_mobile_for_report2)
            onboarding_df['dsa_mobile_clean'] = onboarding_df[onboarding_dsa_col].apply(clean_mobile_for_report2)
            
            # Create mapping of customer → DSA who onboarded them
            valid_onboarding = onboarding_df.dropna(subset=['customer_mobile_clean', 'dsa_mobile_clean'])
            valid_onboarding = valid_onboarding[(valid_onboarding['customer_mobile_clean'] != '') & 
                                                (valid_onboarding['dsa_mobile_clean'] != '')]
            
            for _, row in valid_onboarding.iterrows():
                customer_mobile = row['customer_mobile_clean']
                dsa_mobile = row['dsa_mobile_clean']
                if customer_mobile and dsa_mobile:
                    onboarding_map[customer_mobile] = dsa_mobile
            
            st.info(f"Found {len(onboarding_map)} customer-DSA mappings in onboarding data")
        else:
            st.warning(f"Cannot find required columns in onboarding data. Columns: {list(onboarding_df.columns)}")
        
        # 4. GET CUSTOMER NAMES FROM ALL SOURCES
        customer_names = {}
        
        # Get names from deposit data
        deposit_name_col = None
        name_options = ['Full Name', 'full_name', 'Name', 'Customer Name', 'customer_name']
        for col in name_options:
            if col in deposit_df.columns:
                deposit_name_col = col
                break
        
        if deposit_name_col:
            for _, row in deposit_df.iterrows():
                customer_mobile = row['customer_mobile_clean']
                name = row.get(deposit_name_col)
                if customer_mobile and pd.notna(name) and str(name).strip():
                    customer_names[customer_mobile] = str(name).strip()
        
        # 5. IDENTIFY CUSTOMERS WITH DEPOSIT + TICKET/SCAN ACTIVITY
        # Find ticket and scan customer columns
        ticket_customer_col = None
        scan_customer_col = None
        
        # Find ticket customer column
        for col in ticket_df.columns:
            col_lower = str(col).lower()
            if any(opt in col_lower for opt in ['user', 'customer', 'mobile', 'created by']):
                ticket_customer_col = col
                break
        
        # Find scan customer column
        for col in scan_df.columns:
            col_lower = str(col).lower()
            if any(opt in col_lower for opt in ['user', 'customer', 'mobile', 'created by']):
                scan_customer_col = col
                break
        
        # If not found, use common names
        if ticket_customer_col is None:
            for col in ['user_id', 'User Identifier', 'customer_mobile', 'Mobile', 'Created By']:
                if col in ticket_df.columns:
                    ticket_customer_col = col
                    break
        
        if scan_customer_col is None:
            for col in ['user_id', 'User Identifier', 'customer_mobile', 'Mobile', 'Created By']:
                if col in scan_df.columns:
                    scan_customer_col = col
                    break
        
        # Clean mobile numbers in ticket and scan data
        if ticket_customer_col:
            ticket_df['customer_mobile_clean'] = ticket_df[ticket_customer_col].apply(clean_mobile_for_report2)
            # Filter ticket data for DR transactions
            ticket_tx_col = find_column(ticket_df, ['transaction_type', 'Transaction Type'])
            if ticket_tx_col:
                ticket_df[ticket_tx_col] = ticket_df[ticket_tx_col].astype(str).str.strip().str.upper()
                # Include more variations of debit transactions
                ticket_df = ticket_df[ticket_df[ticket_tx_col].isin(['DR', 'DEBIT', 'D', 'CR'])]
                st.info(f"Ticket data filtered to {len(ticket_df)} DR transactions")
        
        if scan_customer_col:
            scan_df['customer_mobile_clean'] = scan_df[scan_customer_col].apply(clean_mobile_for_report2)
            # Filter scan data for DR transactions
            scan_tx_col = find_column(scan_df, ['transaction_type', 'Transaction Type'])
            if scan_tx_col:
                scan_df[scan_tx_col] = scan_df[scan_tx_col].astype(str).str.strip().str.upper()
                # Include more variations of debit transactions
                scan_df = scan_df[scan_df[scan_tx_col].isin(['DR', 'DEBIT', 'D', 'CR'])]
                st.info(f"Scan data filtered to {len(scan_df)} DR transactions")
        
        # 6. CREATE DSA-CUSTOMER ANALYSIS
        dsa_customers = {}
        
        # Process each deposit to identify DSA-customer relationships
        for _, row in deposit_df.iterrows():
            customer_mobile = row['customer_mobile_clean']
            dsa_mobile = row['dsa_mobile_clean']
            
            # Skip if customer or DSA is missing, or if they're the same
            if not customer_mobile or not dsa_mobile or customer_mobile == dsa_mobile:
                continue
            
            # Initialize DSA entry if not exists
            if dsa_mobile not in dsa_customers:
                dsa_customers[dsa_mobile] = {}
            
            # Initialize customer entry if not exists
            if customer_mobile not in dsa_customers[dsa_mobile]:
                dsa_customers[dsa_mobile][customer_mobile] = {
                    'full_name': customer_names.get(customer_mobile, 'Unknown'),
                    'deposit_count': 0,
                    'bought_ticket': 0,
                    'did_scan': 0,
                    'onboarded_by': onboarding_map.get(customer_mobile, 'NOT ONBOARDED'),
                    'match_status': 'NO ONBOARDING' if customer_mobile not in onboarding_map else 'ONBOARDED'
                }
            
            # Increment deposit count
            dsa_customers[dsa_mobile][customer_mobile]['deposit_count'] += 1
        
        st.info(f"Found {len(dsa_customers)} DSAs with {sum(len(customers) for customers in dsa_customers.values())} unique customers in deposit data")
        
        # 7. CHECK TICKET PURCHASES
        if ticket_customer_col and not ticket_df.empty:
            ticket_df = ticket_df.dropna(subset=['customer_mobile_clean'])
            ticket_customers = set(ticket_df['customer_mobile_clean'].unique())
            
            for dsa_mobile, customers in dsa_customers.items():
                for customer_mobile in customers:
                    if customer_mobile in ticket_customers:
                        # Count ticket purchases for this customer
                        customer_tickets = ticket_df[ticket_df['customer_mobile_clean'] == customer_mobile]
                        dsa_customers[dsa_mobile][customer_mobile]['bought_ticket'] = len(customer_tickets)
        
        # 8. CHECK SCAN TRANSACTIONS
        if scan_customer_col and not scan_df.empty:
            scan_df = scan_df.dropna(subset=['customer_mobile_clean'])
            scan_customers = set(scan_df['customer_mobile_clean'].unique())
            
            for dsa_mobile, customers in dsa_customers.items():
                for customer_mobile in customers:
                    if customer_mobile in scan_customers:
                        # Count scan transactions for this customer
                        customer_scans = scan_df[scan_df['customer_mobile_clean'] == customer_mobile]
                        dsa_customers[dsa_mobile][customer_mobile]['did_scan'] = len(customer_scans)
        
        # 9. UPDATE MATCH STATUS
        for dsa_mobile, customers in dsa_customers.items():
            for customer_mobile, customer_data in customers.items():
                onboarded_by = customer_data['onboarded_by']
                if onboarded_by == 'NOT ONBOARDED':
                    customer_data['match_status'] = 'NO ONBOARDING'
                elif onboarded_by == dsa_mobile:
                    customer_data['match_status'] = 'MATCH'
                else:
                    customer_data['match_status'] = 'MISMATCH'
        
        # 10. CREATE FORMATTED OUTPUT - ONLY NO ONBOARDING CUSTOMERS
        all_rows = []
        
        for dsa_mobile, customers in dsa_customers.items():
            # Filter for NO ONBOARDING customers who have deposit AND (ticket OR scan)
            no_onboarding_customers = []
            for customer_mobile, customer_data in customers.items():
                if (customer_data['match_status'] == 'NO ONBOARDING' and 
                    customer_data['deposit_count'] > 0 and
                    (customer_data['bought_ticket'] > 0 or customer_data['did_scan'] > 0)):
                    no_onboarding_customers.append((customer_mobile, customer_data))
            
            if not no_onboarding_customers:
                continue
            
            # Sort customers by mobile number
            no_onboarding_customers.sort(key=lambda x: x[0])
            
            # Calculate summary for this DSA
            customer_count = len(no_onboarding_customers)
            deposit_count = sum(data['deposit_count'] for _, data in no_onboarding_customers)
            ticket_count = sum(data['bought_ticket'] for _, data in no_onboarding_customers)
            scan_count = sum(data['did_scan'] for _, data in no_onboarding_customers)
            payment = customer_count * 25  # GMD 25 per customer
            
            # Add summary row for first customer
            first_customer_mobile, first_customer_data = no_onboarding_customers[0]
            
            summary_row = {
                'dsa_mobile': dsa_mobile,
                'customer_mobile': first_customer_mobile,
                'full_name': first_customer_data['full_name'],
                'bought_ticket': first_customer_data['bought_ticket'],
                'did_scan': first_customer_data['did_scan'],
                'deposited': first_customer_data['deposit_count'],
                'onboarded_by': first_customer_data['onboarded_by'],
                'match_status': first_customer_data['match_status'],
                'Customer Count': customer_count,
                'Deposit Count': deposit_count,
                'Ticket Count': ticket_count,
                'Scan To Send Count': scan_count,
                'Payment': payment
            }
            all_rows.append(summary_row)
            
            # Add remaining customer rows
            for customer_mobile, customer_data in no_onboarding_customers[1:]:
                customer_row = {
                    'dsa_mobile': dsa_mobile,
                    'customer_mobile': customer_mobile,
                    'full_name': customer_data['full_name'],
                    'bought_ticket': customer_data['bought_ticket'],
                    'did_scan': customer_data['did_scan'],
                    'deposited': customer_data['deposit_count'],
                    'onboarded_by': customer_data['onboarded_by'],
                    'match_status': customer_data['match_status'],
                    'Customer Count': '',
                    'Deposit Count': '',
                    'Ticket Count': '',
                    'Scan To Send Count': '',
                    'Payment': ''
                }
                all_rows.append(customer_row)
            
            # Add empty separator row after each DSA (optional)
            all_rows.append({
                'dsa_mobile': '',
                'customer_mobile': '',
                'full_name': '',
                'bought_ticket': '',
                'did_scan': '',
                'deposited': '',
                'onboarded_by': '',
                'match_status': '',
                'Customer Count': '',
                'Deposit Count': '',
                'Ticket Count': '',
                'Scan To Send Count': '',
                'Payment': ''
            })
        
        # Create DataFrame
        if all_rows:
            results_df = pd.DataFrame(all_rows)
            
            # Define column order
            columns = [
                'dsa_mobile', 'customer_mobile', 'full_name', 'bought_ticket', 
                'did_scan', 'deposited', 'onboarded_by', 'match_status',
                'Customer Count', 'Deposit Count', 'Ticket Count', 
                'Scan To Send Count', 'Payment'
            ]
            
            # Ensure all columns exist
            for col in columns:
                if col not in results_df.columns:
                    results_df[col] = ''
            
            results_df = results_df[columns]
            
            st.success(f"Report 2 generated successfully! Found {len(results_df[results_df['Customer Count'] != ''])} DSAs with NO ONBOARDING customers.")
            
            # DEBUG: Show some statistics
            total_no_onboarding = results_df[results_df['match_status'] == 'NO ONBOARDING'].shape[0]
            total_customers = results_df[results_df['Customer Count'] != '']['Customer Count'].astype(float).sum()
            st.info(f"Total NO ONBOARDING customers: {total_no_onboarding}")
            st.info(f"Total customers in summary: {int(total_customers)}")
            
        else:
            results_df = pd.DataFrame(columns=[
                'dsa_mobile', 'customer_mobile', 'full_name', 'bought_ticket', 
                'did_scan', 'deposited', 'onboarded_by', 'match_status',
                'Customer Count', 'Deposit Count', 'Ticket Count', 
                'Scan To Send Count', 'Payment'
            ])
            st.info("No NO ONBOARDING customers found meeting the criteria.")
        
        # Return all cleaned dataframes for debugging
        return {
            "report_2_results": results_df,
            "customer_names": customer_names,
            "onboarding_map": onboarding_map,
            "dsa_customers": dsa_customers,
            "filtered_dates": {"start_date": start_date, "end_date": end_date},
            "deposit_df": deposit_df,  # Add cleaned deposit dataframe
            "ticket_df": ticket_df,    # Add cleaned ticket dataframe  
            "scan_df": scan_df,        # Add cleaned scan dataframe
            "onboarding_df": onboarding_df  # Add cleaned onboarding dataframe
        }
        
    except Exception as e:
        st.error(f"Error processing Report 2: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

def debug_report_2_missing_customers(report_2_data, original_deposit_df, original_ticket_df, original_scan_df):
    """Debug function to identify why customers are being missed in Report 2"""
    if not report_2_data:
        st.warning("No Report 2 data available for debugging")
        return
    
    if "report_2_results" not in report_2_data:
        st.warning("Report 2 results not available")
        return
    
    results_df = report_2_data["report_2_results"]
    
    # Check if we have cleaned deposit data
    if "deposit_df" in report_2_data and not report_2_data["deposit_df"].empty:
        # Use cleaned deposit data if available
        all_deposit_customers = set(report_2_data["deposit_df"]['customer_mobile_clean'].dropna().unique())
        st.info(f"Using cleaned deposit data with {len(all_deposit_customers)} unique customers")
    else:
        # Try to use original deposit data and clean it
        st.warning("Using original deposit data for debugging - may not match cleaned data")
        # Find customer column in original data
        customer_cols = ['User Identifier', 'user_id', 'Customer Mobile', 'customer_mobile', 'Mobile', 'Created By']
        deposit_customer_col = None
        for col in customer_cols:
            if col in original_deposit_df.columns:
                deposit_customer_col = col
                break
        
        if deposit_customer_col:
            def clean_debug_mobile(mobile):
                if pd.isna(mobile):
                    return None
                mobile_str = str(mobile)
                mobile_clean = ''.join(filter(str.isdigit, mobile_str))
                if len(mobile_clean) >= 7:
                    return mobile_clean[-7:]
                return mobile_clean
            
            cleaned_mobiles = original_deposit_df[deposit_customer_col].apply(clean_debug_mobile)
            all_deposit_customers = set(cleaned_mobiles.dropna().unique())
        else:
            st.error("Cannot find customer column in deposit data for debugging")
            return
    
    # Get customers in report
    report_customers = set(results_df['customer_mobile'].dropna().unique())
    
    # Find missing customers
    missing_customers = all_deposit_customers - report_customers
    
    if missing_customers:
        st.warning(f"Found {len(missing_customers)} customers in deposit data but not in Report 2")
        
        # Sample some missing customers to debug
        sample_missing = list(missing_customers)[:10]  # Show first 10
        st.write(f"Sample missing customers (first 10): {sample_missing}")
        
        # Check why they're missing - check if they have ticket or scan activity
        ticket_customers = set()
        scan_customers = set()
        
        if "ticket_df" in report_2_data and not report_2_data["ticket_df"].empty:
            ticket_customers = set(report_2_data["ticket_df"]['customer_mobile_clean'].dropna().unique())
        elif original_ticket_df is not None and not original_ticket_df.empty:
            # Try to clean original ticket data
            ticket_cols = ['user_id', 'User Identifier', 'customer_mobile', 'Mobile', 'Created By']
            for col in ticket_cols:
                if col in original_ticket_df.columns:
                    def clean_ticket_mobile(mobile):
                        if pd.isna(mobile):
                            return None
                        mobile_str = str(mobile)
                        mobile_clean = ''.join(filter(str.isdigit, mobile_str))
                        if len(mobile_clean) >= 7:
                            return mobile_clean[-7:]
                        return mobile_clean
                    
                    cleaned = original_ticket_df[col].apply(clean_ticket_mobile)
                    ticket_customers = set(cleaned.dropna().unique())
                    break
        
        if "scan_df" in report_2_data and not report_2_data["scan_df"].empty:
            scan_customers = set(report_2_data["scan_df"]['customer_mobile_clean'].dropna().unique())
        elif original_scan_df is not None and not original_scan_df.empty:
            # Try to clean original scan data
            scan_cols = ['user_id', 'User Identifier', 'customer_mobile', 'Mobile', 'Created By']
            for col in scan_cols:
                if col in original_scan_df.columns:
                    def clean_scan_mobile(mobile):
                        if pd.isna(mobile):
                            return None
                        mobile_str = str(mobile)
                        mobile_clean = ''.join(filter(str.isdigit, mobile_str))
                        if len(mobile_clean) >= 7:
                            return mobile_clean[-7:]
                        return mobile_clean
                    
                    cleaned = original_scan_df[col].apply(clean_scan_mobile)
                    scan_customers = set(cleaned.dropna().unique())
                    break
        
        # Check onboarding status for missing customers
        onboarding_map = report_2_data.get("onboarding_map", {})
        
        st.write("### Debug Analysis for Missing Customers")
        for customer in sample_missing[:5]:  # Show first 5 in detail
            has_ticket = customer in ticket_customers
            has_scan = customer in scan_customers
            is_onboarded = customer in onboarding_map
            
            st.write(f"**Customer {customer}:**")
            st.write(f"  - Has ticket activity: {has_ticket}")
            st.write(f"  - Has scan activity: {has_scan}")
            st.write(f"  - Is onboarded: {is_onboarded}")
            if is_onboarded:
                st.write(f"  - Onboarded by: {onboarding_map[customer]}")
            st.write(f"  - Would be included if: Deposit + (Ticket OR Scan) AND NOT Onboarded")
            st.write(f"  - Status: {'MISSING - No ticket/scan' if not (has_ticket or has_scan) else 'MISSING - Is onboarded' if is_onboarded else 'SHOULD BE INCLUDED'}")
            st.write("---")
        
        # Summary statistics
        missing_with_ticket = len([c for c in missing_customers if c in ticket_customers])
        missing_with_scan = len([c for c in missing_customers if c in scan_customers])
        missing_with_activity = len([c for c in missing_customers if c in ticket_customers or c in scan_customers])
        missing_onboarded = len([c for c in missing_customers if c in onboarding_map])
        
        st.write("### Summary of Missing Customers")
        st.write(f"Total missing customers: {len(missing_customers)}")
        st.write(f"Missing customers with ticket activity: {missing_with_ticket}")
        st.write(f"Missing customers with scan activity: {missing_with_scan}")
        st.write(f"Missing customers with any activity: {missing_with_activity}")
        st.write(f"Missing customers that are onboarded: {missing_onboarded}")
        st.write(f"Missing customers that should be in report (activity + not onboarded): {missing_with_activity - missing_onboarded}")
    else:
        st.success("No missing customers found! All deposit customers are accounted for in Report 2.")

def generate_payment_report(report_1_data, report_2_data):
    """Generate Payment report combining earnings from Report 1 and Report 2"""
    try:
        payment_records = []
        
        # Process Report 1 payments (Qualified Customers)
        report1_payments = {}
        if (report_1_data and "qualified_customers" in report_1_data and 
            not report_1_data["qualified_customers"].empty):
            
            # Get rows with payment data (first rows per DSA where Customer Count is filled)
            payment_rows = report_1_data["qualified_customers"][
                report_1_data["qualified_customers"]['Customer Count'] != ''
            ].copy()
            
            if not payment_rows.empty:
                for _, row in payment_rows.iterrows():
                    dsa_mobile = row['dsa_mobile']
                    # Clean the payment value
                    payment_value = row['Payment (Customer Count *40)']
                    if pd.isna(payment_value) or payment_value == '':
                        payment_amount = 0
                    else:
                        # Try to convert to numeric
                        try:
                            payment_amount = float(str(payment_value).replace(',', '').strip())
                        except:
                            payment_amount = 0
                    
                    report1_payments[dsa_mobile] = payment_amount
        
        # Process Report 2 payments (Not Onboarded Customers)
        report2_payments = {}
        if (report_2_data and "report_2_results" in report_2_data and 
            not report_2_data["report_2_results"].empty):
            
            # Get rows with payment data (rows where Customer Count is filled)
            payment_rows = report_2_data["report_2_results"][
                (report_2_data["report_2_results"]['Customer Count'] != '') &
                (report_2_data["report_2_results"]['Payment'] != '')
            ].copy()
            
            if not payment_rows.empty:
                for _, row in payment_rows.iterrows():
                    dsa_mobile = row['dsa_mobile']
                    if dsa_mobile:  # Skip empty DSA rows
                        # Clean the payment value
                        payment_value = row['Payment']
                        if pd.isna(payment_value) or payment_value == '':
                            payment_amount = 0
                        else:
                            # Try to convert to numeric
                            try:
                                payment_amount = float(str(payment_value).replace(',', '').strip())
                            except:
                                payment_amount = 0
                        
                        report2_payments[dsa_mobile] = payment_amount
        
        # Combine all DSAs from both reports
        all_dsas = set(list(report1_payments.keys()) + list(report2_payments.keys()))
        
        # Create payment records
        for dsa_mobile in sorted(all_dsas):
            payment_qualified = report1_payments.get(dsa_mobile, 0)
            payment_not_onboarded = report2_payments.get(dsa_mobile, 0)
            total_payable = payment_qualified + payment_not_onboarded
            
            payment_records.append({
                'DSA_Mobile': dsa_mobile,
                'Payment for Qualified Customers': payment_qualified,
                'Payment for not onboarded Customers': payment_not_onboarded,
                'Total Amount Payable': total_payable
            })
        
        # Calculate totals row
        if payment_records:
            total_qualified = sum(record['Payment for Qualified Customers'] for record in payment_records)
            total_not_onboarded = sum(record['Payment for not onboarded Customers'] for record in payment_records)
            total_overall = sum(record['Total Amount Payable'] for record in payment_records)
            
            # Add totals row
            payment_records.append({
                'DSA_Mobile': 'Total',
                'Payment for Qualified Customers': total_qualified,
                'Payment for not onboarded Customers': total_not_onboarded,
                'Total Amount Payable': total_overall
            })
        
        # Create DataFrame
        payment_df = pd.DataFrame(payment_records)
        
        return payment_df
        
    except Exception as e:
        st.error(f"Error generating Payment report: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return pd.DataFrame(columns=['DSA_Mobile', 'Payment for Qualified Customers', 
                                     'Payment for not onboarded Customers', 'Total Amount Payable'])

def apply_filters_to_data(data, filters, report_type):
    """Apply DSA and other filters to the data"""
    if report_type == "report_1":
        if "qualified_customers" not in data or data["qualified_customers"].empty:
            return data
        
        filtered_data = data.copy()
        qualified_df = data["qualified_customers"].copy()
        
        # Apply DSA filter
        if filters["dsa_option"] == "Single DSA" and filters["selected_dsa"]:
            qualified_df = qualified_df[qualified_df["dsa_mobile"] == filters["selected_dsa"]]
        elif filters["dsa_option"] == "Multiple DSAs" and filters["selected_dsas"]:
            qualified_df = qualified_df[qualified_df["dsa_mobile"].isin(filters["selected_dsas"])]
        
        # Apply minimum customers filter
        if filters["min_customers"] > 0:
            # Get DSAs with at least min_customers
            dsa_counts = qualified_df[qualified_df['Customer Count'] != '']
            if not dsa_counts.empty:
                dsa_counts['Customer Count'] = pd.to_numeric(dsa_counts['Customer Count'], errors='coerce').fillna(0)
                valid_dsas = dsa_counts[dsa_counts['Customer Count'] >= filters["min_customers"]]['dsa_mobile']
                qualified_df = qualified_df[qualified_df['dsa_mobile'].isin(valid_dsas)]
        
        # Apply minimum payment filter
        if filters["min_payment"] > 0:
            # Get DSAs with at least min_payment
            dsa_payments = qualified_df[qualified_df['Payment (Customer Count *40)'] != '']
            if not dsa_payments.empty:
                dsa_payments['Payment (Customer Count *40)'] = pd.to_numeric(dsa_payments['Payment (Customer Count *40)'], errors='coerce').fillna(0)
                valid_dsas = dsa_payments[dsa_payments['Payment (Customer Count *40)'] >= filters["min_payment"]]['dsa_mobile']
                qualified_df = qualified_df[qualified_df['dsa_mobile'].isin(valid_dsas)]
        
        filtered_data["qualified_customers"] = qualified_df
        return filtered_data
    
    elif report_type == "report_2":
        if "report_2_results" not in data or data["report_2_results"].empty:
            return data
        
        filtered_data = data.copy()
        report2_df = data["report_2_results"].copy()
        
        # Apply DSA filter
        if filters["dsa_option"] == "Single DSA" and filters["selected_dsa"]:
            report2_df = report2_df[report2_df["dsa_mobile"] == filters["selected_dsa"]]
        elif filters["dsa_option"] == "Multiple DSAs" and filters["selected_dsas"]:
            report2_df = report2_df[report2_df["dsa_mobile"].isin(filters["selected_dsas"])]
        
        # Apply minimum customers filter
        if filters["min_customers"] > 0:
            # Get DSAs with at least min_customers
            dsa_counts = report2_df[report2_df['Customer Count'] != '']
            if not dsa_counts.empty:
                dsa_counts['Customer Count'] = pd.to_numeric(dsa_counts['Customer Count'], errors='coerce').fillna(0)
                valid_dsas = dsa_counts[dsa_counts['Customer Count'] >= filters["min_customers"]]['dsa_mobile']
                report2_df = report2_df[report2_df['dsa_mobile'].isin(valid_dsas)]
        
        # Apply minimum payment filter
        if filters["min_payment"] > 0:
            # Get DSAs with at least min_payment
            dsa_payments = report2_df[report2_df['Payment'] != '']
            if not dsa_payments.empty:
                dsa_payments['Payment'] = pd.to_numeric(dsa_payments['Payment'], errors='coerce').fillna(0)
                valid_dsas = dsa_payments[dsa_payments['Payment'] >= filters["min_payment"]]['dsa_mobile']
                report2_df = report2_df[report2_df['dsa_mobile'].isin(valid_dsas)]
        
        filtered_data["report_2_results"] = report2_df
        return filtered_data
    
    return data

def create_master_excel_report(filtered_report_1, filtered_report_2, filtered_payment_report):
    """Create master Excel report with all reports in separate sheets"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Report 1 sheets
            if filtered_report_1 and "qualified_customers" in filtered_report_1 and not filtered_report_1["qualified_customers"].empty:
                filtered_report_1["qualified_customers"].to_excel(writer, index=False, sheet_name="Report1_Qualified_Customers")
            
            if filtered_report_1 and "dsa_summary" in filtered_report_1 and not filtered_report_1["dsa_summary"].empty:
                filtered_report_1["dsa_summary"].to_excel(writer, index=False, sheet_name="Report1_DSA_Summary")
            
            if filtered_report_1 and "onboarded_customers" in filtered_report_1 and not filtered_report_1["onboarded_customers"].empty:
                filtered_report_1["onboarded_customers"].to_excel(writer, index=False, sheet_name="Report1_All_Customers")
            
            if filtered_report_1 and "ticket_details" in filtered_report_1 and not filtered_report_1["ticket_details"].empty:
                filtered_report_1["ticket_details"].to_excel(writer, index=False, sheet_name="Report1_Ticket_Details")
            
            if filtered_report_1 and "scan_details" in filtered_report_1 and not filtered_report_1["scan_details"].empty:
                filtered_report_1["scan_details"].to_excel(writer, index=False, sheet_name="Report1_Scan_Details")
            
            if filtered_report_1 and "deposit_details" in filtered_report_1 and not filtered_report_1["deposit_details"].empty:
                filtered_report_1["deposit_details"].to_excel(writer, index=False, sheet_name="Report1_Deposit_Details")
            
            # Report 2 sheets
            if filtered_report_2 and "report_2_results" in filtered_report_2 and not filtered_report_2["report_2_results"].empty:
                filtered_report_2["report_2_results"].to_excel(writer, index=False, sheet_name="Report2_NO_ONBOARDING")
            
            # Payment Report sheet
            if filtered_payment_report is not None and not filtered_payment_report.empty:
                filtered_payment_report.to_excel(writer, index=False, sheet_name="Payment_Report")
            
            # Add a summary sheet
            summary_data = []
            
            # Report 1 Summary
            if filtered_report_1 and "dsa_summary" in filtered_report_1 and not filtered_report_1["dsa_summary"].empty:
                total_customers_r1 = 0
                total_payment_r1 = 0
                
                if "Customer_Count" in filtered_report_1["dsa_summary"].columns:
                    total_customers_r1 = int(filtered_report_1["dsa_summary"]["Customer_Count"].sum())
                
                if "qualified_customers" in filtered_report_1 and not filtered_report_1["qualified_customers"].empty:
                    payment_col = 'Payment (Customer Count *40)'
                    if payment_col in filtered_report_1["qualified_customers"].columns:
                        # Get only rows with payment values (first rows per DSA)
                        payment_rows = filtered_report_1["qualified_customers"][filtered_report_1["qualified_customers"][payment_col] != '']
                        if not payment_rows.empty:
                            total_payment_r1 = float(payment_rows[payment_col].sum())
                
                summary_data.append({
                    'Report': 'Report 1: DSA Performance',
                    'Total DSAs': int(filtered_report_1["dsa_summary"]["dsa_mobile"].nunique()),
                    'Total Customers': total_customers_r1,
                    'Total Payment (GMD)': total_payment_r1
                })
            
            # Report 2 Summary
            if filtered_report_2 and "report_2_results" in filtered_report_2 and not filtered_report_2["report_2_results"].empty:
                report2_summary_rows = filtered_report_2["report_2_results"][filtered_report_2["report_2_results"]['Customer Count'] != '']
                
                total_dsas_r2 = 0
                total_customers_r2 = 0
                total_payment_r2 = 0
                
                if not report2_summary_rows.empty:
                    total_dsas_r2 = int(report2_summary_rows['dsa_mobile'].nunique())
                    total_customers_r2 = int(pd.to_numeric(report2_summary_rows['Customer Count'], errors='coerce').sum())
                    total_payment_r2 = float(pd.to_numeric(report2_summary_rows['Payment'], errors='coerce').sum())
                
                summary_data.append({
                    'Report': 'Report 2: NO ONBOARDING',
                    'Total DSAs': total_dsas_r2,
                    'Total Customers': total_customers_r2,
                    'Total Payment (GMD)': total_payment_r2
                })
            
            # Payment Report Summary
            if filtered_payment_report is not None and not filtered_payment_report.empty:
                total_dsas_pr = len(filtered_payment_report) - 1  # Exclude Total row
                total_qualified_payment = 0
                total_not_onboarded_payment = 0
                total_payable = 0
                
                if filtered_payment_report['DSA_Mobile'].iloc[-1] == 'Total':
                    totals_row = filtered_payment_report.iloc[-1]
                    total_qualified_payment = float(totals_row['Payment for Qualified Customers'])
                    total_not_onboarded_payment = float(totals_row['Payment for not onboarded Customers'])
                    total_payable = float(totals_row['Total Amount Payable'])
                
                summary_data.append({
                    'Report': 'Payment Report',
                    'Total DSAs': total_dsas_pr,
                    'Total Qualified Payment': total_qualified_payment,
                    'Total Not Onboarded Payment': total_not_onboarded_payment,
                    'Total Amount Payable': total_payable
                })
            
            # Create summary DataFrame
            if summary_data:
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, index=False, sheet_name="Summary")
        
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error creating master Excel report: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

def display_metrics(data, report_type):
    """Display key metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    if report_type == "report_1":
        if "dsa_summary" in data and not data["dsa_summary"].empty:
            with col1:
                total_dsas = data["dsa_summary"]["dsa_mobile"].nunique()
                st.metric("Total DSAs", f"{total_dsas:,}")
            
            with col2:
                if "onboarded_customers" in data:
                    total_customers = len(data["onboarded_customers"])
                    st.metric("Total Onboarded Customers", f"{total_customers:,}")
                else:
                    st.metric("Total Onboarded Customers", "0")
            
            with col3:
                if "qualified_customers" in data:
                    # Count unique customers from qualified_customers
                    qualified_count = data["qualified_customers"]["customer_mobile"].nunique()
                    st.metric("Qualified Customers", f"{qualified_count:,}")
                else:
                    st.metric("Qualified Customers", "0")
            
            with col4:
                if "qualified_customers" in data and not data["qualified_customers"].empty:
                    # Calculate total payment from Customer Count column (only in first rows)
                    payment_rows = data["qualified_customers"][data["qualified_customers"]['Customer Count'] != '']
                    if not payment_rows.empty:
                        total_payment = payment_rows['Payment (Customer Count *40)'].sum()
                    else:
                        total_payment = 0
                    st.metric("Total Payment (GMD)", f"GMD {total_payment:,.2f}")
                else:
                    st.metric("Total Payment (GMD)", "GMD 0.00")
        else:
            col1.metric("Total DSAs", "0")
            col2.metric("Total Onboarded Customers", "0")
            col3.metric("Qualified Customers", "0")
            col4.metric("Total Payment (GMD)", "GMD 0.00")
    
    elif report_type == "report_2":
        # REPORT 2 METRICS
        if "report_2_results" in data and not data["report_2_results"].empty:
            # Get summary rows (rows with Customer Count filled)
            summary_rows = data["report_2_results"][
                (data["report_2_results"]['Customer Count'] != '') & 
                (data["report_2_results"]['Customer Count'] != 0)
            ].copy()
            
            # Clean numeric columns
            if not summary_rows.empty:
                summary_rows['Customer Count'] = pd.to_numeric(summary_rows['Customer Count'], errors='coerce').fillna(0)
                summary_rows['Ticket Count'] = pd.to_numeric(summary_rows['Ticket Count'], errors='coerce').fillna(0)
                summary_rows['Scan To Send Count'] = pd.to_numeric(summary_rows['Scan To Send Count'], errors='coerce').fillna(0)
                summary_rows['Payment'] = pd.to_numeric(summary_rows['Payment'], errors='coerce').fillna(0)
                
                with col1:
                    total_dsas = summary_rows['dsa_mobile'].nunique()
                    st.metric("Total DSAs", f"{int(total_dsas):,}")
                
                with col2:
                    total_customers = int(summary_rows['Customer Count'].sum())
                    st.metric("NO ONBOARDING Customers", f"{total_customers:,}")
                
                with col3:
                    total_tickets = int(summary_rows['Ticket Count'].sum())
                    st.metric("Total Tickets", f"{total_tickets:,}")
                
                with col4:
                    total_payment = float(summary_rows['Payment'].sum())
                    st.metric("Total Payment (GMD)", f"GMD {total_payment:,.2f}")
            else:
                col1.metric("Total DSAs", "0")
                col2.metric("NO ONBOARDING Customers", "0")
                col3.metric("Total Tickets", "0")
                col4.metric("Total Payment (GMD)", "GMD 0.00")
        else:
            col1.metric("Total DSAs", "0")
            col2.metric("NO ONBOARDING Customers", "0")
            col3.metric("Total Tickets", "0")
            col4.metric("Total Payment (GMD)", "GMD 0.00")

def display_payment_metrics(payment_df):
    """Display key metrics for Payment report"""
    if payment_df.empty or payment_df['DSA_Mobile'].iloc[-1] != 'Total':
        return
    
    # Get totals from the last row
    totals_row = payment_df.iloc[-1]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        # Count DSAs (excluding the "Total" row)
        total_dsas = len(payment_df) - 1
        st.metric("Total DSAs", f"{total_dsas:,}")
    
    with col2:
        total_qualified = totals_row['Payment for Qualified Customers']
        st.metric("Qualified Customers Payment", f"GMD {total_qualified:,.2f}")
    
    with col3:
        total_not_onboarded = totals_row['Payment for not onboarded Customers']
        st.metric("Not Onboarded Customers Payment", f"GMD {total_not_onboarded:,.2f}")
    
    with col4:
        total_payable = totals_row['Total Amount Payable']
        st.metric("Total Amount Payable", f"GMD {total_payable:,.2f}")

def display_filters():
    """Display filtering options in sidebar"""
    st.sidebar.markdown("### 🔍 Filters")
    
    # Toggle for showing column names
    st.session_state.show_columns = st.sidebar.checkbox("Show Column Names", value=True)
    
    # Date range filter
    st.sidebar.markdown("**Date Range Filter**")
    date_option = st.sidebar.selectbox(
        "Select Date Range",
        ["All Time", "Last 7 Days", "Last 30 Days", "Last 90 Days", "Custom Range"]
    )
    
    # Set default dates based on selection
    if date_option == "Last 7 Days":
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
    elif date_option == "Last 30 Days":
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
    elif date_option == "Last 90 Days":
        start_date = datetime.now() - timedelta(days=90)
        end_date = datetime.now()
    elif date_option == "Custom Range":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", datetime.now())
    else:  # All Time
        start_date = None
        end_date = None
    
    # DSA filter
    st.sidebar.markdown("**DSA Filter**")
    dsa_option = st.sidebar.selectbox(
        "Select DSA Filter Mode",
        ["All DSAs", "Single DSA", "Multiple DSAs"]
    )
    
    dsa_list = []
    if 'report_1_data' in st.session_state and st.session_state.report_1_data:
        if "dsa_summary" in st.session_state.report_1_data and not st.session_state.report_1_data["dsa_summary"].empty:
            dsa_list = st.session_state.report_1_data["dsa_summary"]["dsa_mobile"].unique().tolist()
    
    if dsa_option == "Single DSA":
        selected_dsa = st.sidebar.selectbox("Select DSA", dsa_list if dsa_list else ["No data available"])
    elif dsa_option == "Multiple DSAs":
        selected_dsas = st.sidebar.multiselect("Select DSAs", dsa_list if dsa_list else ["No data available"])
    else:
        selected_dsa = selected_dsas = None
    
    # Additional filters
    st.sidebar.markdown("**Additional Filters**")
    min_customers = st.sidebar.number_input("Minimum Customers", min_value=0, value=0)
    min_payment = st.sidebar.number_input("Minimum Payment (GMD)", min_value=0, value=0)
    
    apply_filters = st.sidebar.button("Apply Filters", type="primary")
    
    return {
        "date_option": date_option,
        "start_date": start_date,
        "end_date": end_date,
        "dsa_option": dsa_option,
        "selected_dsa": selected_dsa if dsa_option == "Single DSA" else None,
        "selected_dsas": selected_dsas if dsa_option == "Multiple DSAs" else None,
        "min_customers": min_customers,
        "min_payment": min_payment,
        "apply_filters": apply_filters
    }

def clean_numeric_column(series):
    """Clean a numeric column that may contain mixed types"""
    if series.dtype == 'object':
        return pd.to_numeric(series.replace('', '0'), errors='coerce').fillna(0)
    return series

def create_visualizations(data, report_type):
    """Create visualizations for the dashboard"""
    if report_type == "report_1":
        if "dsa_summary" not in data or data["dsa_summary"].empty:
            return None, None
        
        # Visualization 1: Top DSAs by Customer Count
        top_dsas = data["dsa_summary"].nlargest(10, "Customer_Count")
        
        fig1 = px.bar(
            top_dsas,
            x="dsa_mobile",
            y="Customer_Count",
            title="Top 10 DSAs by Customer Count",
            labels={"dsa_mobile": "DSA Mobile", "Customer_Count": "Number of Customers"},
            color="Customer_Count",
            color_continuous_scale="Viridis"
        )
        
        # Visualization 2: Conversion Rates
        conversion_cols = []
        if "Deposit_Conversion_Rate" in data["dsa_summary"].columns:
            conversion_cols.append("Deposit_Conversion_Rate")
        if "Ticket_Conversion_Rate" in data["dsa_summary"].columns:
            conversion_cols.append("Ticket_Conversion_Rate")
        if "Scan_Conversion_Rate" in data["dsa_summary"].columns:
            conversion_cols.append("Scan_Conversion_Rate")
        
        if conversion_cols:
            fig2 = px.bar(
                data["dsa_summary"].nlargest(10, conversion_cols[0]),
                x="dsa_mobile",
                y=conversion_cols,
                title="Top 10 DSAs by Conversion Rates",
                labels={"dsa_mobile": "DSA Mobile", "value": "Conversion Rate (%)"},
                barmode="group"
            )
        else:
            fig2 = None
        
        return fig1, fig2
    
    elif report_type == "report_2":
        if "report_2_results" not in data or data["report_2_results"].empty:
            return None, None
        
        # Clean numeric columns
        if 'Payment' in data["report_2_results"].columns:
            data["report_2_results"]['Payment_clean'] = clean_numeric_column(data["report_2_results"]['Payment'])
        
        summary_rows = data["report_2_results"][
            (data["report_2_results"]['Customer Count'] != '') & 
            (data["report_2_results"]['Customer Count'] != 0)
        ]
        
        if summary_rows.empty:
            return None, None
        
        # Visualization 1: Top DSAs by Payment
        top_payment = summary_rows.nlargest(10, "Payment_clean" if "Payment_clean" in summary_rows.columns else "Payment")
        
        fig1 = px.bar(
            top_payment,
            x="dsa_mobile",
            y="Payment_clean" if "Payment_clean" in top_payment.columns else "Payment",
            title="Top 10 DSAs by Payment (GMD)",
            labels={"dsa_mobile": "DSA Mobile", "Payment_clean": "Payment Amount (GMD)", "Payment": "Payment Amount (GMD)"},
            color="Payment_clean" if "Payment_clean" in top_payment.columns else "Payment",
            color_continuous_scale="Plasma"
        )
        
        # Visualization 2: Only show NO ONBOARDING distribution
        if 'match_status' in data["report_2_results"].columns:
            # Filter for NO ONBOARDING only
            no_onboarding_data = data["report_2_results"][data["report_2_results"]['match_status'] == 'NO ONBOARDING']
            if not no_onboarding_data.empty:
                # Count by DSA
                dsa_counts = no_onboarding_data['dsa_mobile'].value_counts().reset_index()
                dsa_counts.columns = ["DSA Mobile", "NO ONBOARDING Customers"]
                
                fig2 = px.bar(
                    dsa_counts.head(10),
                    x="DSA Mobile",
                    y="NO ONBOARDING Customers",
                    title="Top 10 DSAs by NO ONBOARDING Customers",
                    labels={"DSA Mobile": "DSA Mobile", "NO ONBOARDING Customers": "Number of Customers"},
                    color="NO ONBOARDING Customers",
                    color_continuous_scale="Reds"
                )
            else:
                fig2 = None
        else:
            fig2 = None
        
        return fig1, fig2
    
    else:
        return None, None

def debug_report_2_missing_customers(report_2_data, deposit_df, ticket_df, scan_df):
    """Debug function to identify why customers are being missed in Report 2"""
    if not report_2_data or "report_2_results" not in report_2_data:
        return
    
    results_df = report_2_data["report_2_results"]
    
    # Get all unique customers from deposit data
    all_deposit_customers = set(deposit_df['customer_mobile_clean'].dropna().unique())
    
    # Get customers in report
    report_customers = set(results_df['customer_mobile'].dropna().unique())
    
    # Find missing customers
    missing_customers = all_deposit_customers - report_customers
    
    if missing_customers:
        st.warning(f"Found {len(missing_customers)} customers in deposit data but not in Report 2")
        
        # Sample some missing customers to debug
        sample_missing = list(missing_customers)[:5]
        st.write("Sample missing customers:", sample_missing)
        
        # Check why they're missing
        for customer in sample_missing:
            # Check if they have ticket or scan activity
            has_ticket = customer in set(ticket_df['customer_mobile_clean'].dropna().unique())
            has_scan = customer in set(scan_df['customer_mobile_clean'].dropna().unique())
            
            st.write(f"Customer {customer}: Ticket={has_ticket}, Scan={has_scan}")

# Main application
def main():
    # Sidebar for file uploads
    st.sidebar.markdown("### 📁 Upload Data Files")
    
    # File uploaders
    onboarding_file = st.sidebar.file_uploader("Onboarding Data (CSV)", type=['csv'])
    ticket_file = st.sidebar.file_uploader("Ticket Data (CSV)", type=['csv'])
    conversion_file = st.sidebar.file_uploader("Conversion Data (CSV) - Optional", type=['csv'])
    deposit_file = st.sidebar.file_uploader("Deposit Data (CSV)", type=['csv'])
    scan_file = st.sidebar.file_uploader("Scan Data (CSV)", type=['csv'])
    
    # Process files when uploaded
    if onboarding_file and ticket_file and deposit_file and scan_file:
        try:
            # Read uploaded files
            onboarding_df = pd.read_csv(onboarding_file)
            ticket_df = pd.read_csv(ticket_file)
            deposit_df = pd.read_csv(deposit_file, low_memory=False)
            scan_df = pd.read_csv(scan_file, low_memory=False)
            conversion_df = pd.DataFrame()
            if conversion_file:
                conversion_df = pd.read_csv(conversion_file)
            
            # Store in session state
            st.session_state.uploaded_files = {
                "onboarding": onboarding_df,
                "ticket": ticket_df,
                "conversion": conversion_df,
                "deposit": deposit_df,
                "scan": scan_df
            }
            
            # Process both reports
            with st.spinner("Processing Report 1..."):
                report_1_data = process_report_1(onboarding_df, ticket_df, conversion_df, deposit_df, scan_df)
                if report_1_data:
                    st.session_state.report_1_data = report_1_data
                    st.success("✓ Report 1 processed successfully!")
                else:
                    st.warning("Report 1 processing completed with warnings")
            
            with st.spinner("Processing Report 2..."):
                report_2_data = process_report_2(onboarding_df, deposit_df, ticket_df, scan_df)
                if report_2_data:
                    st.session_state.report_2_data = report_2_data
                    st.success("✓ Report 2 processed successfully!")
                    
                    # Debug: Check for missing customers
                    if 'deposit_df' in locals():
                        debug_report_2_missing_customers(
                            report_2_data, 
                            report_2_data.get("deposit_df", deposit_df), 
                            report_2_data.get("ticket_df", ticket_df), 
                            report_2_data.get("scan_df", scan_df)
                        )
                else:
                    st.warning("Report 2 processing completed with warnings")
            
            # Generate Payment report
            with st.spinner("Generating Payment Report..."):
                payment_report = generate_payment_report(
                    st.session_state.report_1_data, 
                    st.session_state.report_2_data
                )
                if not payment_report.empty:
                    st.session_state.payment_report_data = payment_report
                    st.success("✓ Payment Report generated successfully!")
            
            st.sidebar.success("✓ All files processed successfully!")
            
        except Exception as e:
            st.sidebar.error(f"Error processing files: {str(e)}")
            import traceback
            st.sidebar.error(f"Traceback: {traceback.format_exc()}")
    
    # Get filters
    filters = display_filters()
    
    # Main content area
    if st.session_state.report_1_data or st.session_state.report_2_data:
        # Create tabs for different reports
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📋 Report 1: DSA Performance", 
            "📊 Report 2: NO ONBOARDING Analysis", 
            "💰 Payment Report",
            "📈 Visualizations", 
            "📥 Download Reports",
            "📊 Master Report"
        ])
        
        # Initialize filtered data variables
        filtered_report_1 = None
        filtered_report_2 = None
        filtered_payment_report = None
        
        # Check if filters are applied
        if filters["apply_filters"]:
            # Process reports with date filters
            with st.spinner("Applying filters to all reports..."):
                # Reprocess Report 1 with date filters
                filtered_report_1 = process_report_1(
                    st.session_state.uploaded_files["onboarding"],
                    st.session_state.uploaded_files["ticket"],
                    st.session_state.uploaded_files["conversion"],
                    st.session_state.uploaded_files["deposit"],
                    st.session_state.uploaded_files["scan"],
                    start_date=filters["start_date"],
                    end_date=filters["end_date"]
                )
                
                # Reprocess Report 2 with date filters
                filtered_report_2 = process_report_2(
                    st.session_state.uploaded_files["onboarding"],
                    st.session_state.uploaded_files["deposit"],
                    st.session_state.uploaded_files["ticket"],
                    st.session_state.uploaded_files["scan"],
                    start_date=filters["start_date"],
                    end_date=filters["end_date"]
                )
                
                # Apply additional filters (DSA, min customers, min payment)
                if filtered_report_1:
                    filtered_report_1 = apply_filters_to_data(filtered_report_1, filters, "report_1")
                    st.session_state.filtered_report_1 = filtered_report_1
                
                if filtered_report_2:
                    filtered_report_2 = apply_filters_to_data(filtered_report_2, filters, "report_2")
                    st.session_state.filtered_report_2 = filtered_report_2
                
                # Generate filtered payment report
                if filtered_report_1 and filtered_report_2:
                    filtered_payment_report = generate_payment_report(filtered_report_1, filtered_report_2)
                    st.session_state.filtered_payment_data = filtered_payment_report
                else:
                    filtered_payment_report = st.session_state.payment_report_data
                
                st.success("✓ All filters applied successfully!")
        else:
            # Use original data if no filters applied
            filtered_report_1 = st.session_state.report_1_data
            filtered_report_2 = st.session_state.report_2_data
            filtered_payment_report = st.session_state.payment_report_data
        
        with tab1:
            if filtered_report_1:
                st.markdown('<div class="sub-header">Report 1: DSA Performance Summary (GMD)</div>', unsafe_allow_html=True)
                
                # Display date filter info if applied
                if filters["apply_filters"] and "filtered_dates" in filtered_report_1:
                    dates = filtered_report_1["filtered_dates"]
                    if dates["start_date"] or dates["end_date"]:
                        date_range = ""
                        if dates["start_date"]:
                            date_range += f"From: {dates['start_date'].strftime('%Y-%m-%d')} "
                        if dates["end_date"]:
                            date_range += f"To: {dates['end_date'].strftime('%Y-%m-%d')}"
                        st.info(f"📅 **Date Filter Applied:** {date_range}")
                
                # Display metrics
                display_metrics(filtered_report_1, "report_1")
                
                # Display data
                if "dsa_summary" in filtered_report_1 and not filtered_report_1["dsa_summary"].empty:
                    st.markdown("#### DSA Summary Table")
                    st.dataframe(filtered_report_1["dsa_summary"], use_container_width=True)
                    
                    # Show qualified customers
                    with st.expander("View Qualified Customers Details"):
                        qualified_df = filtered_report_1.get("qualified_customers", pd.DataFrame())
                        if not qualified_df.empty:
                            st.markdown("**Qualified Customers (Customers who deposited AND bought ticket/did scan):**")
                            st.dataframe(qualified_df, use_container_width=True)
                            st.caption("Note: Payment is GMD 40 per qualified customer. Summary columns shown only for first customer per DSA.")
                        else:
                            st.info("No qualified customers found.")
                else:
                    st.info("No data available for Report 1 with current filters.")
        
        with tab2:
            if filtered_report_2 and "report_2_results" in filtered_report_2 and not filtered_report_2["report_2_results"].empty:
                st.markdown('<div class="sub-header">Report 2: NO ONBOARDING Analysis (GMD)</div>', unsafe_allow_html=True)
                
                # Display date filter info if applied
                if filters["apply_filters"] and "filtered_dates" in filtered_report_2:
                    dates = filtered_report_2["filtered_dates"]
                    if dates["start_date"] or dates["end_date"]:
                        date_range = ""
                        if dates["start_date"]:
                            date_range += f"From: {dates['start_date'].strftime('%Y-%m-%d')} "
                        if dates["end_date"]:
                            date_range += f"To: {dates['end_date'].strftime('%Y-%m-%d')}"
                        st.info(f"📅 **Date Filter Applied:** {date_range}")
                
                # Display metrics
                display_metrics(filtered_report_2, "report_2")
                
                # Display data
                st.markdown("#### NO ONBOARDING Customers Analysis")
                st.dataframe(filtered_report_2["report_2_results"], use_container_width=True)
                
                # Show statistics
                with st.expander("View Detailed Statistics"):
                    if not filtered_report_2["report_2_results"].empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Transaction Patterns (NO ONBOARDING only)**")
                            summary_rows = filtered_report_2["report_2_results"][filtered_report_2["report_2_results"]['Customer Count'] != '']
                            if not summary_rows.empty:
                                # Clean numeric columns
                                summary_rows['Customer Count'] = clean_numeric_column(summary_rows['Customer Count'])
                                summary_rows['Ticket Count'] = clean_numeric_column(summary_rows['Ticket Count'])
                                summary_rows['Scan To Send Count'] = clean_numeric_column(summary_rows['Scan To Send Count'])
                                
                                st.write(f"Total DSAs with NO ONBOARDING: {summary_rows['dsa_mobile'].nunique()}")
                                st.write(f"Total NO ONBOARDING Customers: {int(summary_rows['Customer Count'].sum())}")
                                st.write(f"Total Tickets Purchased: {int(summary_rows['Ticket Count'].sum())}")
                                st.write(f"Total Scans Completed: {int(summary_rows['Scan To Send Count'].sum())}")
                                st.write(f"Average Payment per DSA: GMD {float(summary_rows['Payment'].mean()):,.2f}")
                        
                        with col2:
                            st.markdown("**Payment Summary**")
                            if not summary_rows.empty:
                                st.write(f"Total Payment (GMD): GMD {float(summary_rows['Payment'].sum()):,.2f}")
                                st.write(f"Minimum Payment: GMD {float(summary_rows['Payment'].min()):,.2f}")
                                st.write(f"Maximum Payment: GMD {float(summary_rows['Payment'].max()):,.2f}")
                                st.write(f"Average Customers per DSA: {float(summary_rows['Customer Count'].mean()):.1f}")
            else:
                st.info("Report 2 data not available or empty.")
        
        with tab3:
            if filtered_payment_report is not None and not filtered_payment_report.empty:
                st.markdown('<div class="sub-header">💰 Payment Report: DSA Earnings Summary (GMD)</div>', unsafe_allow_html=True)
                
                # Display metrics
                display_payment_metrics(filtered_payment_report)
                
                # Display the payment table
                st.markdown("#### Payment Summary")
                
                # Format the DataFrame for better display
                display_df = filtered_payment_report.copy()
                
                # Format currency columns
                currency_columns = ['Payment for Qualified Customers', 'Payment for not onboarded Customers', 'Total Amount Payable']
                for col in currency_columns:
                    display_df[col] = display_df[col].apply(lambda x: f"GMD {x:,.2f}" if pd.notna(x) and not isinstance(x, str) else x)
                
                # Display the table with special styling for the Total row
                st.dataframe(display_df, use_container_width=True)
                
                # Add summary statistics
                with st.expander("View Payment Statistics"):
                    col1, col2 = st.columns(2)
                    
                    # Calculate statistics (excluding the Total row)
                    if len(filtered_payment_report) > 1:
                        df_stats = filtered_payment_report.iloc[:-1]  # Exclude Total row
                        
                        with col1:
                            st.markdown("**Average Payments per DSA:**")
                            avg_qualified = df_stats['Payment for Qualified Customers'].mean()
                            avg_not_onboarded = df_stats['Payment for not onboarded Customers'].mean()
                            avg_total = df_stats['Total Amount Payable'].mean()
                            
                            st.write(f"Average Qualified Payment: GMD {avg_qualified:,.2f}")
                            st.write(f"Average Not Onboarded Payment: GMD {avg_not_onboarded:,.2f}")
                            st.write(f"Average Total Payment: GMD {avg_total:,.2f}")
                        
                        with col2:
                            st.markdown("**Payment Distribution:**")
                            
                            # Count DSAs with different payment types
                            dsas_with_qualified = (df_stats['Payment for Qualified Customers'] > 0).sum()
                            dsas_with_not_onboarded = (df_stats['Payment for not onboarded Customers'] > 0).sum()
                            dsas_with_both = ((df_stats['Payment for Qualified Customers'] > 0) & 
                                              (df_stats['Payment for not onboarded Customers'] > 0)).sum()
                            
                            st.write(f"DSAs with Qualified Earnings: {dsas_with_qualified}")
                            st.write(f"DSAs with Not Onboarded Earnings: {dsas_with_not_onboarded}")
                            st.write(f"DSAs with Both Earnings: {dsas_with_both}")
                    
                    # Payment breakdown
                    st.markdown("**Payment Breakdown:**")
                    if not filtered_payment_report.empty and filtered_payment_report['DSA_Mobile'].iloc[-1] == 'Total':
                        totals = filtered_payment_report.iloc[-1]
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Total Qualified Payments:** GMD {totals['Payment for Qualified Customers']:,.2f}")
                            st.write(f"**Total Not Onboarded Payments:** GMD {totals['Payment for not onboarded Customers']:,.2f}")
                            st.write(f"**Grand Total Payable:** GMD {totals['Total Amount Payable']:,.2f}")
                        
                        with col2:
                            if totals['Total Amount Payable'] > 0:
                                qualified_pct = (totals['Payment for Qualified Customers'] / totals['Total Amount Payable']) * 100
                                not_onboarded_pct = (totals['Payment for not onboarded Customers'] / totals['Total Amount Payable']) * 100
                                
                                st.write(f"**Qualified Payments:** {qualified_pct:.1f}%")
                                st.write(f"**Not Onboarded Payments:** {not_onboarded_pct:.1f}%")
            else:
                st.info("No payment data available. Please check if both Report 1 and Report 2 have been processed.")
        
        with tab4:
            st.markdown('<div class="sub-header">Data Visualizations</div>', unsafe_allow_html=True)
            
            # Create visualizations for Report 1
            if filtered_report_1:
                fig1_r1, fig2_r1 = create_visualizations(filtered_report_1, "report_1")
                
                if fig1_r1:
                    st.plotly_chart(fig1_r1, use_container_width=True)
                if fig2_r1:
                    st.plotly_chart(fig2_r1, use_container_width=True)
                if not fig1_r1 and not fig2_r1:
                    st.info("No visualization data available for Report 1.")
            
            # Create visualizations for Report 2
            if filtered_report_2 and "report_2_results" in filtered_report_2 and not filtered_report_2["report_2_results"].empty:
                fig1_r2, fig2_r2 = create_visualizations(filtered_report_2, "report_2")
                
                if fig1_r2:
                    st.plotly_chart(fig1_r2, use_container_width=True)
                if fig2_r2:
                    st.plotly_chart(fig2_r2, use_container_width=True)
                if not fig1_r2 and not fig2_r2:
                    st.info("No visualization data available for Report 2.")
            
            # Add Payment report visualization
            if filtered_payment_report is not None and not filtered_payment_report.empty:
                st.markdown("#### Payment Report Visualizations")
                
                if len(filtered_payment_report) > 1:  # Excluding Total row
                    # Exclude the Total row for visualizations
                    viz_data = filtered_payment_report[filtered_payment_report['DSA_Mobile'] != 'Total'].copy()
                    
                    if not viz_data.empty:
                        # Create visualization for top earners
                        viz_data_sorted = viz_data.nlargest(15, 'Total Amount Payable')
                        
                        fig_payment = go.Figure()
                        fig_payment.add_trace(go.Bar(
                            x=viz_data_sorted['DSA_Mobile'],
                            y=viz_data_sorted['Payment for Qualified Customers'],
                            name='Qualified Customers',
                            marker_color='#2E86AB'
                        ))
                        fig_payment.add_trace(go.Bar(
                            x=viz_data_sorted['DSA_Mobile'],
                            y=viz_data_sorted['Payment for not onboarded Customers'],
                            name='Not Onboarded Customers',
                            marker_color='#A23B72'
                        ))
                        
                        fig_payment.update_layout(
                            title='Top 15 DSAs by Total Payment (GMD)',
                            xaxis_title='DSA Mobile',
                            yaxis_title='Payment Amount (GMD)',
                            barmode='stack',
                            height=500
                        )
                        
                        st.plotly_chart(fig_payment, use_container_width=True)
                        
                        # Pie chart showing payment distribution
                        if len(filtered_payment_report) > 0 and filtered_payment_report['DSA_Mobile'].iloc[-1] == 'Total':
                            totals = filtered_payment_report.iloc[-1]
                            if totals['Total Amount Payable'] > 0:
                                fig_pie = go.Figure(data=[go.Pie(
                                    labels=['Qualified Customers', 'Not Onboarded Customers'],
                                    values=[totals['Payment for Qualified Customers'], totals['Payment for not onboarded Customers']],
                                    hole=0.3,
                                    marker_colors=['#2E86AB', '#A23B72']
                                )])
                                fig_pie.update_layout(
                                    title='Overall Payment Distribution'
                                )
                                st.plotly_chart(fig_pie, use_container_width=True)
        
        with tab5:
            st.markdown('<div class="sub-header">Download Reports (GMD)</div>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if filtered_report_1:
                    st.markdown("#### Report 1: DSA Performance")
                    
                    # Create Excel file for Report 1
                    if "qualified_customers" in filtered_report_1 and not filtered_report_1["qualified_customers"].empty:
                        output_1 = BytesIO()
                        with pd.ExcelWriter(output_1, engine='openpyxl') as writer:
                            filtered_report_1["qualified_customers"].to_excel(writer, index=False, sheet_name="Qualified_Customers")
                            if "dsa_summary" in filtered_report_1 and not filtered_report_1["dsa_summary"].empty:
                                filtered_report_1["dsa_summary"].to_excel(writer, index=False, sheet_name="DSA_Summary")
                        output_1.seek(0)
                        
                        st.download_button(
                            label="📥 Download Report 1 (Excel)",
                            data=output_1,
                            file_name=f"DSA_Performance_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    # CSV download for qualified customers
                    if "qualified_customers" in filtered_report_1 and not filtered_report_1["qualified_customers"].empty:
                        csv_1 = filtered_report_1["qualified_customers"].to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Qualified Customers (CSV)",
                            data=csv_1,
                            file_name=f"Qualified_Customers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
            
            with col2:
                if filtered_report_2 and "report_2_results" in filtered_report_2 and not filtered_report_2["report_2_results"].empty:
                    st.markdown("#### Report 2: NO ONBOARDING Analysis")
                    
                    # Create Excel file for Report 2
                    output_2 = BytesIO()
                    with pd.ExcelWriter(output_2, engine='openpyxl') as writer:
                        filtered_report_2["report_2_results"].to_excel(writer, index=False, sheet_name="NO_ONBOARDING_Analysis")
                    output_2.seek(0)
                    
                    st.download_button(
                        label="📥 Download Report 2 (Excel)",
                        data=output_2,
                        file_name=f"DSA_NO_ONBOARDING_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # CSV download
                    csv_2 = filtered_report_2["report_2_results"].to_csv(index=False, sep='\t').encode('utf-8')
                    st.download_button(
                        label="📥 Download Analysis (CSV)",
                        data=csv_2,
                        file_name=f"DSA_NO_ONBOARDING_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            with col3:
                if filtered_payment_report is not None and not filtered_payment_report.empty:
                    st.markdown("#### Payment Report")
                    
                    # Create Excel file for Payment report
                    output_payment = BytesIO()
                    with pd.ExcelWriter(output_payment, engine='openpyxl') as writer:
                        filtered_payment_report.to_excel(writer, index=False, sheet_name="Payment_Report")
                    output_payment.seek(0)
                    
                    st.download_button(
                        label="📥 Download Payment Report (Excel)",
                        data=output_payment,
                        file_name=f"DSA_Payment_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                    
                    # CSV download
                    csv_payment = filtered_payment_report.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Payment Report (CSV)",
                        data=csv_payment,
                        file_name=f"DSA_Payment_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
        
        with tab6:
            st.markdown('<div class="sub-header">📊 Master Report: All Reports in One Excel Workbook</div>', unsafe_allow_html=True)
            
            st.info("""
            **Master Report Features:**
            - Contains ALL reports in one Excel file
            - Each report in separate worksheet
            - Includes Summary worksheet
            - All filters are applied (date range, DSA selection, min customers, min payment)
            """)
            
            # Create master report
            if filtered_report_1 or filtered_report_2 or filtered_payment_report is not None:
                with st.spinner("Generating Master Report..."):
                    master_excel = create_master_excel_report(filtered_report_1, filtered_report_2, filtered_payment_report)
                    
                    if master_excel:
                        st.success("✓ Master Report generated successfully!")
                        
                        # Display summary
                        st.markdown("#### Master Report Contents:")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            if filtered_report_1 and "qualified_customers" in filtered_report_1 and not filtered_report_1["qualified_customers"].empty:
                                st.metric("Report 1 Sheets", "6")
                        
                        with col2:
                            if filtered_report_2 and "report_2_results" in filtered_report_2 and not filtered_report_2["report_2_results"].empty:
                                st.metric("Report 2 Sheets", "1")
                        
                        with col3:
                            if filtered_payment_report is not None and not filtered_payment_report.empty:
                                st.metric("Total Sheets", "8+")
                        
                        # Download button for master report
                        st.download_button(
                            label="📥 Download Master Report (All-in-One Excel)",
                            data=master_excel,
                            file_name=f"DSA_Master_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            type="primary"
                        )
                        
                        # Show sheet preview
                        with st.expander("View Sheet List"):
                            sheets = [
                                "Report1_Qualified_Customers",
                                "Report1_DSA_Summary", 
                                "Report1_All_Customers",
                                "Report1_Ticket_Details",
                                "Report1_Scan_Details",
                                "Report1_Deposit_Details",
                                "Report2_NO_ONBOARDING",
                                "Payment_Report",
                                "Summary"
                            ]
                            
                            for sheet in sheets:
                                st.write(f"📄 {sheet}")
            else:
                st.warning("No data available to generate Master Report.")
    
    else:
        # Show instructions when no data is uploaded
        st.info("👋 Welcome to the DSA Performance Dashboard (GMD)!")
        st.markdown("""
        ### To get started:
        1. **Upload your data files** using the sidebar on the left
        2. **Required files:**
           - Onboarding Data (CSV)
           - Ticket Data (CSV) 
           - Deposit Data (CSV)
           - Scan Data (CSV)
        3. **Optional file:**
           - Conversion Data (CSV)
        
        ### Features:
        - 📋 **Three comprehensive reports** with different analysis approaches
        - 🔍 **Interactive filtering** by DSA, date range, and performance metrics
        - 📊 **Data visualizations** for insights
        - 📥 **Download reports** individually or as Master Report
        - 🔄 **Real-time calculations** based on your filters
        - 💰 **GMD currency** support for all financial metrics
        
        ### Report Details:
        - **Report 1**: Shows qualified customers who deposited AND bought ticket/did scan (GMD 40 per customer)
        - **Report 2**: Shows ONLY NO ONBOARDING customers with deposit and ticket/scan activity (GMD 25 per customer)
        - **Payment Report**: Combines earnings from Report 1 and Report 2 with total amount payable
        - **Master Report**: All reports in one Excel workbook with separate worksheets
        
        ### Filter Options:
        - **Date Range**: Last 7/30/90 days or custom range
        - **DSA Selection**: All DSAs, single DSA, or multiple DSAs
        - **Minimum Customers**: Filter DSAs with minimum number of customers
        - **Minimum Payment**: Filter DSAs with minimum payment amount
        """)
        
        # Show sample data preview
        with st.expander("View Sample Data Structure"):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown("**Deposit Data Columns:**")
                st.write("- User Identifier")
                st.write("- Transaction Type (CR/DR)")
                st.write("- Amount (GMD)")
                st.write("- Created By")
                st.write("- Full Name")
                st.write("- Created At (for date filtering)")
            
            with col2:
                st.markdown("**Onboarding Data Columns:**")
                st.write("- Mobile")
                st.write("- Customer Referrer Mobile")
                st.write("- Full Name")
                st.write("- Status")
                st.write("- Registration Date (for date filtering)")

if __name__ == "__main__":
    main()
