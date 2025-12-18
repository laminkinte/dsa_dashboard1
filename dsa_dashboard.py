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
if 'show_columns' not in st.session_state:
    st.session_state.show_columns = True

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
        
        # Clean ticket customer column
        ticket_customer_col = find_column(ticket_df, ["created_by", "user_id", "User Identifier", "customer_mobile", "Customer Mobile", "Mobile"])
        
        if ticket_customer_col is None:
            st.error(f"No suitable customer column found in Ticket data")
            return None
        
        ticket_df = ticket_df.rename(columns={ticket_customer_col: "customer_mobile"})
        
        # Clean scan customer column
        scan_customer_col = find_column(scan_df, ['Created By', 'Customer Mobile', 'Mobile', 'User Identifier', 'user_id', 'customer_mobile'])
        
        if scan_customer_col is None:
            st.error(f"No suitable customer column found in Scan data")
            return None
        
        scan_df = scan_df.rename(columns={scan_customer_col: "customer_mobile"})
        
        # Clean mobile numbers
        for df, col in [(onboarding_df, "customer_mobile"), (onboarding_df, "dsa_mobile"),
                        (deposit_df, "customer_mobile"), (ticket_df, "customer_mobile"),
                        (scan_df, "customer_mobile"), (conversion_df, "dsa_mobile")]:
            if col in df.columns:
                df[col] = safe_str_access(df[col])
        
        # Clean numeric columns for ticket data
        ticket_amount_col = find_column(ticket_df, ["amount", "ticket_amount", "Amount", "transaction_amount"])
        if ticket_amount_col:
            ticket_df["ticket_amount"] = ticket_df[ticket_amount_col].apply(clean_currency_amount)
        elif "ticket_amount" not in ticket_df.columns:
            ticket_df["ticket_amount"] = 0
        
        # Clean numeric columns for scan data
        scan_amount_col = find_column(scan_df, ["Amount", "scan_amount", "amount", "transaction_amount"])
        if scan_amount_col:
            scan_df["scan_amount"] = scan_df[scan_amount_col].apply(clean_currency_amount)
        elif "scan_amount" not in scan_df.columns:
            scan_df["scan_amount"] = 0
        
        # Filter ticket data for customers only
        if "entity_name" in ticket_df.columns:
            ticket_df["entity_name"] = safe_str_access(ticket_df["entity_name"])
            ticket_df = ticket_df[ticket_df["entity_name"].str.lower() == "customer"]
        
        # Aggregate ticket data
        ticket_agg = ticket_df.groupby("customer_mobile").agg(
            ticket_amount=("ticket_amount", "sum"),
            ticket_count=("ticket_amount", lambda x: (x > 0).sum())
        ).reset_index()
        ticket_agg["bought_ticket"] = (ticket_agg["ticket_amount"] > 0).astype(int)
        
        # Aggregate scan data
        scan_summary = scan_df.groupby("customer_mobile").agg(
            scan_amount=("scan_amount", "sum"),
            scan_count=("scan_amount", "count")
        ).reset_index()
        scan_summary["did_scan"] = (scan_summary["scan_amount"] > 0).astype(int)
        
        # Get unique depositors
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
        
        onboarded_customers["bought_ticket"] = onboarded_customers["bought_ticket"].fillna(0).astype(int)
        onboarded_customers["did_scan"] = onboarded_customers["did_scan"].fillna(0).astype(int)
        onboarded_customers["deposited"] = onboarded_customers["deposited"].fillna(0).astype(int)
        onboarded_customers["ticket_amount"] = onboarded_customers["ticket_amount"].fillna(0)
        onboarded_customers["scan_amount"] = onboarded_customers["scan_amount"].fillna(0)
        
        # Create qualified customers table - EXACTLY as in sample
        qualified_customers = onboarded_customers[
            (onboarded_customers["deposited"] == 1) & 
            ((onboarded_customers["bought_ticket"] == 1) | (onboarded_customers["did_scan"] == 1))
        ].copy()
        
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
            "qualified_customers": qualified_customers_final,  # Using the exact format
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
        # Clean column names
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
            
            if ticket_date_col:
                ticket_df = filter_by_date(ticket_df, ticket_date_col, start_date, end_date)
            
            if scan_date_col:
                scan_df = filter_by_date(scan_df, scan_date_col, start_date, end_date)
            
            if onboarding_date_col:
                onboarding_df = filter_by_date(onboarding_df, onboarding_date_col, start_date, end_date)
        
        # Display column names in sidebar
        if 'show_columns' in st.session_state and st.session_state.show_columns:
            with st.sidebar.expander("📋 View Column Names", expanded=False):
                st.markdown("**Onboarding Data Columns:**")
                st.markdown(f'<div class="column-display">{list(onboarding_df.columns)}</div>', unsafe_allow_html=True)
                
                st.markdown("**Deposit Data Columns:**")
                st.markdown(f'<div class="column-display">{list(deposit_df.columns)}</div>', unsafe_allow_html=True)
                
                st.markdown("**Ticket Data Columns:**")
                st.markdown(f'<div class="column-display">{list(ticket_df.columns)}</div>', unsafe_allow_html=True)
                
                st.markdown("**Scan Data Columns:**")
                st.markdown(f'<div class="column-display">{list(scan_df.columns)}</div>', unsafe_allow_html=True)
        
        # Clean mobile numbers in onboarding data
        if 'Mobile' in onboarding_df.columns:
            onboarding_df['Mobile'] = onboarding_df['Mobile'].apply(clean_mobile_number)
        if 'Customer Referrer Mobile' in onboarding_df.columns:
            onboarding_df['Customer Referrer Mobile'] = onboarding_df['Customer Referrer Mobile'].apply(clean_mobile_number)
        
        # Find transaction type columns based on your actual column names
        deposit_tx_type_col = find_column(deposit_df, ['Transaction Type', 'transaction_type', 'TransactionType', 'Type', 'transaction type', 'txn_type', 'TXN_TYPE', 'transactionType'])
        ticket_tx_type_col = find_column(ticket_df, ['transaction_type', 'Transaction Type', 'TransactionType', 'Type', 'transaction type', 'txn_type', 'TXN_TYPE', 'transactionType'])
        scan_tx_type_col = find_column(scan_df, ['Transaction Type', 'transaction_type', 'TransactionType', 'Type', 'transaction type', 'txn_type', 'TXN_TYPE', 'transactionType'])
        
        # Find customer mobile columns based on your actual column names
        deposit_customer_col = find_column(deposit_df, ['User Identifier', 'user_id', 'UserIdentifier', 'Customer Mobile', 'customer_mobile', 'Mobile', 'USER_ID', 'User ID', 'UserID'])
        ticket_customer_col = find_column(ticket_df, ['user_id', 'User Identifier', 'UserIdentifier', 'Customer Mobile', 'customer_mobile', 'Mobile', 'USER_ID', 'User ID', 'UserID', 'created_by'])
        scan_customer_col = find_column(scan_df, ['User Identifier', 'user_id', 'UserIdentifier', 'Customer Mobile', 'customer_mobile', 'Mobile', 'USER_ID', 'User ID', 'UserID', 'Created By'])
        
        # Find DSA/agent columns based on your actual column names
        deposit_dsa_col = find_column(deposit_df, ['Created By', 'created_by', 'CreatedBy', 'DSA Mobile', 'dsa_mobile', 'Agent Mobile', 'agent_mobile', 'Referrer Mobile'])
        
        # Clean mobile numbers in other dataframes
        if deposit_customer_col:
            deposit_df[deposit_customer_col] = deposit_df[deposit_customer_col].apply(clean_mobile_number)
        if deposit_dsa_col:
            deposit_df[deposit_dsa_col] = deposit_df[deposit_dsa_col].apply(clean_mobile_number)
        if ticket_customer_col:
            ticket_df[ticket_customer_col] = ticket_df[ticket_customer_col].apply(clean_mobile_number)
        if scan_customer_col:
            scan_df[scan_customer_col] = scan_df[scan_customer_col].apply(clean_mobile_number)
        
        # Get customer names from all sources
        customer_names = {}
        onboarding_map = {}
        
        # Get names from onboarding data
        if 'Mobile' in onboarding_df.columns:
            name_col = find_column(onboarding_df, ['Full Name', 'full_name', 'Name', 'Customer Name', 'customer_name'])
            if name_col:
                for _, row in onboarding_df.dropna(subset=['Mobile']).iterrows():
                    mobile = row['Mobile']
                    name = row.get(name_col)
                    if mobile and name and pd.notna(name):
                        customer_names[mobile] = str(name).strip()
                    
                    # Get onboarding mapping
                    referrer = row.get('Customer Referrer Mobile')
                    if mobile and referrer and pd.notna(referrer):
                        onboarding_map[mobile] = referrer
        
        # Get names from deposit data
        if deposit_customer_col:
            deposit_name_col = find_column(deposit_df, ['Full Name', 'full_name', 'Name', 'Customer Name', 'customer_name'])
            if deposit_name_col:
                for _, row in deposit_df.dropna(subset=[deposit_customer_col]).iterrows():
                    mobile = row[deposit_customer_col]
                    name = row.get(deposit_name_col)
                    if mobile and name and pd.notna(name) and mobile not in customer_names:
                        customer_names[mobile] = str(name).strip()
        
        # Get names from ticket data
        if ticket_customer_col:
            ticket_name_col = find_column(ticket_df, ['full_name', 'Full Name', 'Name', 'Customer Name', 'customer_name'])
            if ticket_name_col:
                for _, row in ticket_df.dropna(subset=[ticket_customer_col]).iterrows():
                    mobile = row[ticket_customer_col]
                    name = row.get(ticket_name_col)
                    if mobile and name and pd.notna(name) and mobile not in customer_names:
                        customer_names[mobile] = str(name).strip()
        
        # Get names from scan data
        if scan_customer_col:
            scan_name_col = find_column(scan_df, ['Full Name', 'full_name', 'Name', 'Customer Name', 'customer_name'])
            if scan_name_col:
                for _, row in scan_df.dropna(subset=[scan_customer_col]).iterrows():
                    mobile = row[scan_customer_col]
                    name = row.get(scan_name_col)
                    if mobile and name and pd.notna(name) and mobile not in customer_names:
                        customer_names[mobile] = str(name).strip()
        
        # Analyze transactions
        dsa_customers = {}
        
        # Filter deposits for customer deposits (CR) if we have transaction type column
        if deposit_tx_type_col and deposit_customer_col and deposit_dsa_col:
            # Use the variable, not hardcoded column name
            customer_deposits = deposit_df[deposit_df[deposit_tx_type_col] == 'CR'].copy()
        elif deposit_customer_col and deposit_dsa_col:
            # If no transaction type column, assume all are customer deposits
            customer_deposits = deposit_df.copy()
        else:
            customer_deposits = pd.DataFrame()
            st.warning("Could not find required columns in deposit data for Report 2")
        
        for _, row in customer_deposits.iterrows():
            customer_mobile = row.get(deposit_customer_col) if deposit_customer_col else None
            dsa_mobile = row.get(deposit_dsa_col) if deposit_dsa_col else None
            
            if not customer_mobile or not dsa_mobile or customer_mobile == dsa_mobile:
                continue
            
            if dsa_mobile not in dsa_customers:
                dsa_customers[dsa_mobile] = {}
            
            if customer_mobile not in dsa_customers[dsa_mobile]:
                dsa_customers[dsa_mobile][customer_mobile] = {
                    'full_name': customer_names.get(customer_mobile, 'Unknown'),
                    'deposit_count': 0,
                    'bought_ticket': 0,
                    'did_scan': 0,
                    'onboarded_by': onboarding_map.get(customer_mobile, 'NOT ONBOARDED'),
                    'match_status': 'NO ONBOARDING' if customer_mobile not in onboarding_map else 'MISMATCH'
                }
            
            dsa_customers[dsa_mobile][customer_mobile]['deposit_count'] += 1
        
        # Analyze ticket purchases
        if ticket_tx_type_col and ticket_customer_col:
            # Use the variable, not hardcoded column name - Note: ticket data uses lowercase 'transaction_type'
            customer_tickets = ticket_df[ticket_df[ticket_tx_type_col] == 'DR'].copy()
        elif ticket_customer_col:
            # If no transaction type column, assume all are ticket purchases
            customer_tickets = ticket_df.copy()
        else:
            customer_tickets = pd.DataFrame()
            st.warning("Could not find required columns in ticket data for Report 2")
        
        for _, row in customer_tickets.iterrows():
            customer_mobile = row.get(ticket_customer_col) if ticket_customer_col else None
            if not customer_mobile:
                continue
            
            for dsa_mobile, customers in dsa_customers.items():
                if customer_mobile in customers:
                    customers[customer_mobile]['bought_ticket'] += 1
                    break
        
        # Analyze scan transactions
        if scan_tx_type_col and scan_customer_col:
            # Use the variable, not hardcoded column name
            customer_scans = scan_df[scan_df[scan_tx_type_col] == 'DR'].copy()
        elif scan_customer_col:
            # If no transaction type column, assume all are scan transactions
            customer_scans = scan_df.copy()
        else:
            customer_scans = pd.DataFrame()
            st.warning("Could not find required columns in scan data for Report 2")
        
        for _, row in customer_scans.iterrows():
            customer_mobile = row.get(scan_customer_col) if scan_customer_col else None
            if not customer_mobile:
                continue
            
            for dsa_mobile, customers in dsa_customers.items():
                if customer_mobile in customers:
                    customers[customer_mobile]['did_scan'] += 1
                    break
        
        # Update match status
        for dsa_mobile, customers in dsa_customers.items():
            for customer_mobile, customer_data in customers.items():
                onboarded_by = customer_data['onboarded_by']
                if onboarded_by != 'NOT ONBOARDED':
                    customer_data['match_status'] = 'MATCH' if onboarded_by == dsa_mobile else 'MISMATCH'
        
        # Create formatted output - ONLY NO ONBOARDING customers
        all_rows = []
        
        for dsa_mobile, customers in dsa_customers.items():
            # Filter for NO ONBOARDING customers only
            no_onboarding_customers = []
            for customer_mobile, customer_data in customers.items():
                if customer_data['match_status'] == 'NO ONBOARDING' and (customer_data['bought_ticket'] > 0 or customer_data['did_scan'] > 0):
                    no_onboarding_customers.append(customer_mobile)
            
            if not no_onboarding_customers:
                continue
            
            # Calculate summary for this DSA (NO ONBOARDING only)
            customer_count = len(no_onboarding_customers)
            deposit_count = sum(customers[c]['deposit_count'] for c in no_onboarding_customers)
            ticket_count = sum(customers[c]['bought_ticket'] for c in no_onboarding_customers)
            scan_count = sum(customers[c]['did_scan'] for c in no_onboarding_customers)
            payment = customer_count * 25  # $25 per active customer as in original
            
            # Add summary row for first customer
            first_customer = no_onboarding_customers[0] if no_onboarding_customers else None
            if first_customer:
                first_customer_data = customers[first_customer]
                
                summary_row = {
                    'dsa_mobile': dsa_mobile,
                    'customer_mobile': first_customer,
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
            
            # Add remaining customer rows (NO ONBOARDING only)
            for customer_mobile in no_onboarding_customers[1:]:
                customer_data = customers[customer_mobile]
                
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
            
            # Add empty separator row after each DSA (like original format)
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
        results_df = pd.DataFrame(all_rows)
        
        # Define column order exactly as original sample
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
        
        return {
            "report_2_results": results_df,
            "customer_names": customer_names,
            "onboarding_map": onboarding_map,
            "dsa_customers": dsa_customers,
            "filtered_dates": {"start_date": start_date, "end_date": end_date}
        }
        
    except Exception as e:
        st.error(f"Error processing Report 2: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        return None

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

def create_excel_download(data, report_type):
    """Create Excel file for download"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if report_type == "report_1":
                if "qualified_customers" in data and not data["qualified_customers"].empty:
                    data["qualified_customers"].to_excel(writer, index=False, sheet_name="Qualified_Customers")
                if "dsa_summary" in data and not data["dsa_summary"].empty:
                    data["dsa_summary"].to_excel(writer, index=False, sheet_name="DSA_Summary")
                if "onboarded_customers" in data and not data["onboarded_customers"].empty:
                    data["onboarded_customers"].to_excel(writer, index=False, sheet_name="All_Customers")
                if "ticket_details" in data and not data["ticket_details"].empty:
                    data["ticket_details"].to_excel(writer, index=False, sheet_name="Ticket_Details")
                if "scan_details" in data and not data["scan_details"].empty:
                    data["scan_details"].to_excel(writer, index=False, sheet_name="Scan_Details")
                if "deposit_details" in data and not data["deposit_details"].empty:
                    data["deposit_details"].to_excel(writer, index=False, sheet_name="Deposit_Details")
            elif report_type == "report_2":
                if "report_2_results" in data and not data["report_2_results"].empty:
                    data["report_2_results"].to_excel(writer, index=False, sheet_name="DSA_Analysis")
            elif report_type == "payment":
                data.to_excel(writer, index=False, sheet_name="Payment_Report")
        
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error creating Excel file: {str(e)}")
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

def filter_data(data, filters, report_type):
    """Filter data based on selected filters"""
    if report_type == "report_1":
        if "dsa_summary" not in data or data["dsa_summary"].empty:
            return pd.DataFrame()
        df_to_filter = data["dsa_summary"].copy()
    elif report_type == "report_2":
        if "report_2_results" not in data or data["report_2_results"].empty:
            return pd.DataFrame()
        df_to_filter = data["report_2_results"].copy()
    else:
        return pd.DataFrame()
    
    # Apply DSA filter
    if filters["dsa_option"] == "Single DSA" and filters["selected_dsa"]:
        df_to_filter = df_to_filter[df_to_filter["dsa_mobile"] == filters["selected_dsa"]]
    elif filters["dsa_option"] == "Multiple DSAs" and filters["selected_dsas"]:
        df_to_filter = df_to_filter[df_to_filter["dsa_mobile"].isin(filters["selected_dsas"])]
    
    # Apply minimum customers filter
    if report_type == "report_1":
        if filters["min_customers"] > 0 and "Customer_Count" in df_to_filter.columns:
            df_to_filter = df_to_filter[df_to_filter["Customer_Count"] >= filters["min_customers"]]
    elif report_type == "report_2":
        if filters["min_customers"] > 0 and 'Customer Count' in df_to_filter.columns:
            # Clean Customer Count column
            df_to_filter['Customer Count'] = pd.to_numeric(df_to_filter['Customer Count'].replace('', '0'), errors='coerce').fillna(0)
            summary_rows = df_to_filter[df_to_filter['Customer Count'] > 0]
            if not summary_rows.empty:
                valid_dsas = summary_rows[summary_rows['Customer Count'] >= filters["min_customers"]]['dsa_mobile']
                df_to_filter = df_to_filter[df_to_filter['dsa_mobile'].isin(valid_dsas)]
    
    # Apply minimum payment filter
    if report_type == "report_1":
        if filters["min_payment"] > 0 and "qualified_customers" in data and not data["qualified_customers"].empty:
            # Get payment from first rows where payment is filled
            payment_data = data["qualified_customers"][data["qualified_customers"]['Payment (Customer Count *40)'] != '']
            if not payment_data.empty:
                payment_by_dsa = payment_data.groupby("dsa_mobile")['Payment (Customer Count *40)'].first().reset_index()
                valid_dsas = payment_by_dsa[payment_by_dsa['Payment (Customer Count *40)'] >= filters["min_payment"]]["dsa_mobile"]
                df_to_filter = df_to_filter[df_to_filter["dsa_mobile"].isin(valid_dsas)]
    elif report_type == "report_2":
        if filters["min_payment"] > 0 and 'Payment' in df_to_filter.columns:
            # Clean Payment column
            df_to_filter['Payment'] = pd.to_numeric(df_to_filter['Payment'].replace('', '0'), errors='coerce').fillna(0)
            summary_rows = df_to_filter[df_to_filter['Payment'] > 0]
            if not summary_rows.empty:
                valid_dsas = summary_rows[summary_rows['Payment'] >= filters["min_payment"]]['dsa_mobile']
                df_to_filter = df_to_filter[df_to_filter['dsa_mobile'].isin(valid_dsas)]
    
    return df_to_filter

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
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📋 Report 1: DSA Performance", 
            "📊 Report 2: NO ONBOARDING Analysis", 
            "💰 Payment Report",
            "📈 Visualizations", 
            "📥 Download Reports"
        ])
        
        with tab1:
            if st.session_state.report_1_data:
                # Check if we need to reprocess with date filters
                if filters["apply_filters"]:
                    with st.spinner("Applying filters to Report 1..."):
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
                        
                        if filtered_report_1:
                            st.session_state.filtered_report_1 = filtered_report_1
                            data_to_display = filtered_report_1
                            st.success(f"✓ Report 1 filtered for date range: {filters['start_date']} to {filters['end_date']}")
                        else:
                            data_to_display = st.session_state.report_1_data
                            st.warning("Could not apply date filter, showing all data")
                else:
                    data_to_display = st.session_state.report_1_data
                
                st.markdown('<div class="sub-header">Report 1: DSA Performance Summary (GMD)</div>', unsafe_allow_html=True)
                
                # Display date filter info if applied
                if filters["apply_filters"] and "filtered_dates" in data_to_display:
                    dates = data_to_display["filtered_dates"]
                    if dates["start_date"] or dates["end_date"]:
                        date_range = ""
                        if dates["start_date"]:
                            date_range += f"From: {dates['start_date'].strftime('%Y-%m-%d')} "
                        if dates["end_date"]:
                            date_range += f"To: {dates['end_date'].strftime('%Y-%m-%d')}"
                        st.info(f"📅 **Date Filter Applied:** {date_range}")
                
                # Display metrics
                display_metrics(data_to_display, "report_1")
                
                # Apply other filters (DSA, min customers, min payment)
                if filters["apply_filters"]:
                    filtered_data = filter_data(data_to_display, filters, "report_1")
                else:
                    filtered_data = data_to_display.get("dsa_summary", pd.DataFrame())
                
                # Display data
                if not filtered_data.empty:
                    st.markdown("#### DSA Summary Table")
                    st.dataframe(filtered_data, use_container_width=True)
                    
                    # Show qualified customers - EXACT FORMAT as sample
                    with st.expander("View Qualified Customers Details"):
                        qualified_df = data_to_display.get("qualified_customers", pd.DataFrame())
                        if not qualified_df.empty:
                            st.markdown("**Qualified Customers (Customers who deposited AND bought ticket/did scan):**")
                            st.dataframe(qualified_df, use_container_width=True)
                            st.caption("Note: Payment is GMD 40 per qualified customer. Summary columns shown only for first customer per DSA.")
                        else:
                            st.info("No qualified customers found.")
                else:
                    st.info("No data available for Report 1 with current filters.")
        
        with tab2:
            if st.session_state.report_2_data and "report_2_results" in st.session_state.report_2_data and not st.session_state.report_2_data["report_2_results"].empty:
                # Check if we need to reprocess with date filters
                if filters["apply_filters"]:
                    with st.spinner("Applying filters to Report 2..."):
                        # Reprocess Report 2 with date filters
                        filtered_report_2 = process_report_2(
                            st.session_state.uploaded_files["onboarding"],
                            st.session_state.uploaded_files["deposit"],
                            st.session_state.uploaded_files["ticket"],
                            st.session_state.uploaded_files["scan"],
                            start_date=filters["start_date"],
                            end_date=filters["end_date"]
                        )
                        
                        if filtered_report_2:
                            st.session_state.filtered_report_2 = filtered_report_2
                            data_to_display = filtered_report_2
                            st.success(f"✓ Report 2 filtered for date range: {filters['start_date']} to {filters['end_date']}")
                        else:
                            data_to_display = st.session_state.report_2_data
                            st.warning("Could not apply date filter, showing all data")
                else:
                    data_to_display = st.session_state.report_2_data
                
                st.markdown('<div class="sub-header">Report 2: NO ONBOARDING Analysis (GMD)</div>', unsafe_allow_html=True)
                
                # Display date filter info if applied
                if filters["apply_filters"] and "filtered_dates" in data_to_display:
                    dates = data_to_display["filtered_dates"]
                    if dates["start_date"] or dates["end_date"]:
                        date_range = ""
                        if dates["start_date"]:
                            date_range += f"From: {dates['start_date'].strftime('%Y-%m-%d')} "
                        if dates["end_date"]:
                            date_range += f"To: {dates['end_date'].strftime('%Y-%m-%d')}"
                        st.info(f"📅 **Date Filter Applied:** {date_range}")
                
                # Display metrics
                display_metrics(data_to_display, "report_2")
                
                # Apply other filters (DSA, min customers, min payment)
                if filters["apply_filters"]:
                    filtered_data = filter_data(data_to_display, filters, "report_2")
                else:
                    filtered_data = data_to_display["report_2_results"]
                
                # Display data
                if not filtered_data.empty:
                    st.markdown("#### NO ONBOARDING Customers Analysis")
                    st.dataframe(filtered_data, use_container_width=True)
                    
                    # Show statistics
                    with st.expander("View Detailed Statistics"):
                        if not filtered_data.empty:
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.markdown("**Transaction Patterns (NO ONBOARDING only)**")
                                summary_rows = filtered_data[filtered_data['Customer Count'] != '']
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
                    st.info("No data available for Report 2 with current filters.")
            else:
                st.info("Report 2 data not available or empty.")
        
        with tab3:
            if not st.session_state.payment_report_data.empty:
                # Check if we need to regenerate with filtered data
                data_to_display = None
                if filters["apply_filters"]:
                    with st.spinner("Regenerating Payment Report with filtered data..."):
                        # Get filtered data
                        filtered_report_1 = st.session_state.filtered_report_1 if st.session_state.filtered_report_1 else st.session_state.report_1_data
                        filtered_report_2 = st.session_state.filtered_report_2 if st.session_state.filtered_report_2 else st.session_state.report_2_data
                        
                        # Regenerate payment report with filtered data
                        data_to_display = generate_payment_report(filtered_report_1, filtered_report_2)
                else:
                    data_to_display = st.session_state.payment_report_data
                
                if data_to_display is not None and not data_to_display.empty:
                    st.markdown('<div class="sub-header">💰 Payment Report: DSA Earnings Summary (GMD)</div>', unsafe_allow_html=True)
                    
                    # Display metrics
                    display_payment_metrics(data_to_display)
                    
                    # Display the payment table
                    st.markdown("#### Payment Summary")
                    
                    # Format the DataFrame for better display
                    display_df = data_to_display.copy()
                    
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
                        if len(data_to_display) > 1:
                            df_stats = data_to_display.iloc[:-1]  # Exclude Total row
                            
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
                        if not data_to_display.empty and data_to_display['DSA_Mobile'].iloc[-1] == 'Total':
                            totals = data_to_display.iloc[-1]
                            
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
            else:
                st.info("Payment report not available yet. Please upload and process the data files first.")
        
        with tab4:
            st.markdown('<div class="sub-header">Data Visualizations</div>', unsafe_allow_html=True)
            
            # Create visualizations for Report 1
            if st.session_state.report_1_data:
                # Use filtered data if available
                data_for_viz_1 = st.session_state.filtered_report_1 if st.session_state.filtered_report_1 else st.session_state.report_1_data
                fig1_r1, fig2_r1 = create_visualizations(data_for_viz_1, "report_1")
                
                if fig1_r1:
                    st.plotly_chart(fig1_r1, use_container_width=True)
                if fig2_r1:
                    st.plotly_chart(fig2_r1, use_container_width=True)
                if not fig1_r1 and not fig2_r1:
                    st.info("No visualization data available for Report 1.")
            
            # Create visualizations for Report 2
            if st.session_state.report_2_data and "report_2_results" in st.session_state.report_2_data and not st.session_state.report_2_data["report_2_results"].empty:
                # Use filtered data if available
                data_for_viz_2 = st.session_state.filtered_report_2 if st.session_state.filtered_report_2 else st.session_state.report_2_data
                fig1_r2, fig2_r2 = create_visualizations(data_for_viz_2, "report_2")
                
                if fig1_r2:
                    st.plotly_chart(fig1_r2, use_container_width=True)
                if fig2_r2:
                    st.plotly_chart(fig2_r2, use_container_width=True)
                if not fig1_r2 and not fig2_r2:
                    st.info("No visualization data available for Report 2.")
            
            # Add Payment report visualization
            if not st.session_state.payment_report_data.empty:
                st.markdown("#### Payment Report Visualizations")
                
                # Use filtered data if available
                if filters["apply_filters"]:
                    # Regenerate payment report with filtered data
                    filtered_report_1 = st.session_state.filtered_report_1 if st.session_state.filtered_report_1 else st.session_state.report_1_data
                    filtered_report_2 = st.session_state.filtered_report_2 if st.session_state.filtered_report_2 else st.session_state.report_2_data
                    payment_data = generate_payment_report(filtered_report_1, filtered_report_2)
                else:
                    payment_data = st.session_state.payment_report_data
                
                if not payment_data.empty and len(payment_data) > 1:  # Excluding Total row
                    # Exclude the Total row for visualizations
                    viz_data = payment_data[payment_data['DSA_Mobile'] != 'Total'].copy()
                    
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
                        if len(payment_data) > 0 and payment_data['DSA_Mobile'].iloc[-1] == 'Total':
                            totals = payment_data.iloc[-1]
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
                if st.session_state.report_1_data:
                    st.markdown("#### Report 1: DSA Performance")
                    # Use filtered data for download if available
                    data_for_download_1 = st.session_state.filtered_report_1 if st.session_state.filtered_report_1 else st.session_state.report_1_data
                    excel_file_1 = create_excel_download(data_for_download_1, "report_1")
                    
                    if excel_file_1:
                        st.download_button(
                            label="📥 Download Report 1 (Excel)",
                            data=excel_file_1,
                            file_name=f"DSA_Performance_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    # CSV download
                    if "dsa_summary" in data_for_download_1 and not data_for_download_1["dsa_summary"].empty:
                        csv_1 = data_for_download_1["dsa_summary"].to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Summary (CSV)",
                            data=csv_1,
                            file_name=f"DSA_Summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
            
            with col2:
                if st.session_state.report_2_data and "report_2_results" in st.session_state.report_2_data and not st.session_state.report_2_data["report_2_results"].empty:
                    st.markdown("#### Report 2: NO ONBOARDING Analysis")
                    # Use filtered data for download if available
                    data_for_download_2 = st.session_state.filtered_report_2 if st.session_state.filtered_report_2 else st.session_state.report_2_data
                    excel_file_2 = create_excel_download(data_for_download_2, "report_2")
                    
                    if excel_file_2:
                        st.download_button(
                            label="📥 Download Report 2 (Excel)",
                            data=excel_file_2,
                            file_name=f"DSA_NO_ONBOARDING_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    # CSV download
                    csv_2 = data_for_download_2["report_2_results"].to_csv(index=False, sep='\t').encode('utf-8')
                    st.download_button(
                        label="📥 Download Analysis (CSV)",
                        data=csv_2,
                        file_name=f"DSA_NO_ONBOARDING_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            with col3:
                if not st.session_state.payment_report_data.empty:
                    st.markdown("#### Payment Report")
                    
                    # Determine which data to use for download
                    if filters["apply_filters"]:
                        filtered_report_1 = st.session_state.filtered_report_1 if st.session_state.filtered_report_1 else st.session_state.report_1_data
                        filtered_report_2 = st.session_state.filtered_report_2 if st.session_state.filtered_report_2 else st.session_state.report_2_data
                        payment_data_for_download = generate_payment_report(filtered_report_1, filtered_report_2)
                    else:
                        payment_data_for_download = st.session_state.payment_report_data
                    
                    # Excel download for Payment report
                    if not payment_data_for_download.empty:
                        excel_file_payment = create_excel_download(payment_data_for_download, "payment")
                        
                        if excel_file_payment:
                            st.download_button(
                                label="📥 Download Payment Report (Excel)",
                                data=excel_file_payment,
                                file_name=f"DSA_Payment_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                            )
                        
                        # CSV download
                        csv_payment = payment_data_for_download.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Download Payment Report (CSV)",
                            data=csv_payment,
                            file_name=f"DSA_Payment_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
    
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
        - 📥 **Download reports** in Excel or CSV format
        - 🔄 **Real-time calculations** based on your filters
        - 💰 **GMD currency** support for all financial metrics
        
        ### Report Details:
        - **Report 1**: Shows qualified customers who deposited AND bought ticket/did scan (GMD 40 per customer)
        - **Report 2**: Shows ONLY NO ONBOARDING customers with deposit and ticket/scan activity (GMD 25 per customer)
        - **Payment Report**: Combines earnings from Report 1 and Report 2 with total amount payable
        
        ### Sample Data Format:
        The dashboard is designed to work with the sample data formats you provided:
        - Deposit data with User Identifier and Created By columns
        - Ticket data with Transaction Type and Amount
        - Scan data with Transaction Type and Amount
        - Onboarding data with Mobile and Customer Referrer Mobile columns
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
