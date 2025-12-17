import streamlit as st
import pandas as pd
import numpy as np
import warnings
from datetime import datetime, timedelta
from io import BytesIO
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="DSA Performance Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
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
    </style>
""", unsafe_allow_html=True)

# Main header
st.markdown('<div class="main-header">📊 DSA Performance Analysis Dashboard</div>', unsafe_allow_html=True)

# Initialize session state for file storage
if 'uploaded_files' not in st.session_state:
    st.session_state.uploaded_files = {}
if 'processed_data' not in st.session_state:
    st.session_state.processed_data = {}
if 'report_1_data' not in st.session_state:
    st.session_state.report_1_data = {}
if 'report_2_data' not in st.session_state:
    st.session_state.report_2_data = {}

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

def process_report_1(onboarding_df, ticket_df, conversion_df, deposit_df, scan_df):
    """Process data for Report 1"""
    try:
        # Clean column names
        for df in [onboarding_df, ticket_df, conversion_df, deposit_df, scan_df]:
            df.columns = df.columns.str.strip()
        
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
        name_col = next((c for c in name_cols if c in onboarding_df.columns), None)
        if name_col is None:
            onboarding_df["full_name"] = "Unknown"
        else:
            onboarding_df = onboarding_df.rename(columns={name_col: "full_name"})
        
        onboarding_df = onboarding_df.rename(columns={
            "Customer Referrer Mobile": "dsa_mobile",
            "Mobile": "customer_mobile"
        })
        
        # Clean deposit customer column
        possible_cols = ['customer_mobile', 'Customer Mobile', 'Mobile', 'User Identifier']
        deposit_customer_col = next((c for c in possible_cols if c in deposit_df.columns), None)
        if deposit_customer_col is None:
            st.error("No suitable customer/mobile column found in Deposit data")
            return None
        deposit_df = deposit_df.rename(columns={deposit_customer_col: "customer_mobile"})
        
        conversion_df = conversion_df.rename(columns={"Agent Mobile": "dsa_mobile", "Deposit Count": "deposit_count"})
        
        # Clean ticket customer column
        ticket_customer_cols = ["created_by", "user_id"]
        ticket_customer_col = next((c for c in ticket_customer_cols if c in ticket_df.columns), None)
        if ticket_customer_col is None:
            st.error(f"No suitable customer column found in Ticket data")
            return None
        ticket_df = ticket_df.rename(columns={ticket_customer_col: "customer_mobile", "amount": "ticket_amount"})
        
        # Clean scan customer column
        scan_customer_cols = ['Created By', 'Customer Mobile', 'Mobile', 'User Identifier']
        scan_customer_col = next((c for c in scan_customer_cols if c in scan_df.columns), None)
        if scan_customer_col is None:
            st.error(f"No suitable customer column found in Scan data")
            return None
        scan_df = scan_df.rename(columns={scan_customer_col: "customer_mobile", "Amount": "scan_amount"})
        
        # Clean mobile numbers
        for df, col in [(onboarding_df, "customer_mobile"), (onboarding_df, "dsa_mobile"),
                        (deposit_df, "customer_mobile"), (ticket_df, "customer_mobile"),
                        (scan_df, "customer_mobile"), (conversion_df, "dsa_mobile")]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        # Clean numeric columns
        if "ticket_amount" in ticket_df.columns:
            ticket_df["ticket_amount"] = pd.to_numeric(
                ticket_df["ticket_amount"].astype(str).str.replace(",", ""), 
                errors='coerce'
            ).fillna(0)
        
        if "scan_amount" in scan_df.columns:
            scan_df["scan_amount"] = pd.to_numeric(
                scan_df["scan_amount"].astype(str).str.replace(",", ""), 
                errors='coerce'
            ).fillna(0)
        
        # Filter ticket data for customers only
        if "entity_name" in ticket_df.columns:
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
        
        # Create qualified customers table
        qualified_customers = onboarded_customers[
            (onboarded_customers["deposited"] == 1) & 
            ((onboarded_customers["bought_ticket"] == 1) | (onboarded_customers["did_scan"] == 1))
        ].copy()
        
        # Sort and add running counts
        qualified_customers = qualified_customers.sort_values(["dsa_mobile", "customer_mobile"])
        qualified_customers["Customer Count"] = qualified_customers.groupby("dsa_mobile").cumcount() + 1
        qualified_customers["Deposit Count"] = qualified_customers.groupby("dsa_mobile")["deposited"].cumsum()
        qualified_customers["Ticket Count"] = qualified_customers.groupby("dsa_mobile")["bought_ticket"].cumsum()
        qualified_customers["Scan To Send Count"] = qualified_customers.groupby("dsa_mobile")["did_scan"].cumsum()
        qualified_customers["Payment (Customer Count *40)"] = qualified_customers.groupby("dsa_mobile")["Customer Count"].transform(lambda x: x.max() * 40)
        
        # Create DSA summary
        dsa_summary_all = onboarded_customers.groupby("dsa_mobile").agg(
            Customer_Count=("customer_mobile", "count"),
            Customers_who_deposited=("deposited", "sum"),
            Customers_who_bought_ticket=("bought_ticket", "sum"),
            Customers_who_did_scan=("did_scan", "sum"),
            Total_Ticket_Amount=("ticket_amount", "sum"),
            Total_Scan_Amount=("scan_amount", "sum")
        ).reset_index()
        
        if "dsa_mobile" in conversion_df.columns:
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
            "qualified_customers": qualified_customers,
            "dsa_summary": dsa_summary_all,
            "onboarded_customers": onboarded_customers,
            "ticket_details": ticket_df,
            "scan_details": scan_df,
            "deposit_details": deposit_df
        }
        
    except Exception as e:
        st.error(f"Error processing Report 1: {str(e)}")
        return None

def process_report_2(onboarding_df, deposit_df, ticket_df, scan_df):
    """Process data for Report 2"""
    try:
        # Clean column names
        for df in [onboarding_df, deposit_df, ticket_df, scan_df]:
            df.columns = df.columns.str.strip()
        
        # Clean mobile numbers in all dataframes
        # Onboarding data
        if 'Mobile' in onboarding_df.columns:
            onboarding_df['Mobile'] = onboarding_df['Mobile'].apply(clean_mobile_number)
        if 'Customer Referrer Mobile' in onboarding_df.columns:
            onboarding_df['Customer Referrer Mobile'] = onboarding_df['Customer Referrer Mobile'].apply(clean_mobile_number)
        
        # Deposit data
        if 'User Identifier' in deposit_df.columns:
            deposit_df['User Identifier'] = deposit_df['User Identifier'].apply(clean_mobile_number)
        if 'Created By' in deposit_df.columns:
            deposit_df['Created By'] = deposit_df['Created By'].apply(clean_mobile_number)
        
        # Ticket data
        if 'User Identifier' in ticket_df.columns:
            ticket_df['User Identifier'] = ticket_df['User Identifier'].apply(clean_mobile_number)
        if 'Created By' in ticket_df.columns:
            ticket_df['Created By'] = ticket_df['Created By'].apply(clean_mobile_number)
        
        # Scan data
        if 'User Identifier' in scan_df.columns:
            scan_df['User Identifier'] = scan_df['User Identifier'].apply(clean_mobile_number)
        if 'Created By' in scan_df.columns:
            scan_df['Created By'] = scan_df['Created By'].apply(clean_mobile_number)
        
        # Get customer names from all sources
        customer_names = {}
        onboarding_map = {}
        
        # Get names from onboarding data
        if 'Mobile' in onboarding_df.columns and 'Full Name' in onboarding_df.columns:
            for _, row in onboarding_df.dropna(subset=['Mobile']).iterrows():
                mobile = row['Mobile']
                name = row.get('Full Name')
                if mobile and name and pd.notna(name):
                    customer_names[mobile] = str(name).strip()
                
                # Get onboarding mapping
                referrer = row.get('Customer Referrer Mobile')
                if mobile and referrer and pd.notna(referrer):
                    onboarding_map[mobile] = referrer
        
        # Get names from deposit data
        if 'User Identifier' in deposit_df.columns and 'Full Name' in deposit_df.columns:
            for _, row in deposit_df.dropna(subset=['User Identifier']).iterrows():
                mobile = row['User Identifier']
                name = row.get('Full Name')
                if mobile and name and pd.notna(name) and mobile not in customer_names:
                    customer_names[mobile] = str(name).strip()
        
        # Get names from ticket data
        if 'User Identifier' in ticket_df.columns and 'Full Name' in ticket_df.columns:
            for _, row in ticket_df.dropna(subset=['User Identifier']).iterrows():
                mobile = row['User Identifier']
                name = row.get('Full Name')
                if mobile and name and pd.notna(name) and mobile not in customer_names:
                    customer_names[mobile] = str(name).strip()
        
        # Get names from scan data
        if 'User Identifier' in scan_df.columns and 'Full Name' in scan_df.columns:
            for _, row in scan_df.dropna(subset=['User Identifier']).iterrows():
                mobile = row['User Identifier']
                name = row.get('Full Name')
                if mobile and name and pd.notna(name) and mobile not in customer_names:
                    customer_names[mobile] = str(name).strip()
        
        # Analyze transactions
        dsa_customers = {}
        
        # Filter deposits for customer deposits (CR)
        customer_deposits = deposit_df[deposit_df['Transaction Type'] == 'CR'].copy()
        
        for _, row in customer_deposits.iterrows():
            customer_mobile = row.get('User Identifier')
            dsa_mobile = row.get('Created By')
            
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
        customer_tickets = ticket_df[ticket_df['Transaction Type'] == 'DR'].copy()
        
        for _, row in customer_tickets.iterrows():
            customer_mobile = row.get('User Identifier')
            if not customer_mobile:
                continue
            
            for dsa_mobile, customers in dsa_customers.items():
                if customer_mobile in customers:
                    customers[customer_mobile]['bought_ticket'] += 1
                    break
        
        # Analyze scan transactions
        customer_scans = scan_df[scan_df['Transaction Type'] == 'DR'].copy()
        
        for _, row in customer_scans.iterrows():
            customer_mobile = row.get('User Identifier')
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
        
        # Create formatted output
        all_rows = []
        
        for dsa_mobile, customers in dsa_customers.items():
            # Calculate summary for this DSA
            active_customers = []
            for customer_mobile, customer_data in customers.items():
                if customer_data['bought_ticket'] > 0 or customer_data['did_scan'] > 0:
                    active_customers.append(customer_mobile)
            
            if not active_customers:
                continue
            
            customer_count = len(active_customers)
            deposit_count = sum(customers[c]['deposit_count'] for c in active_customers)
            ticket_count = sum(customers[c]['bought_ticket'] for c in active_customers)
            scan_count = sum(customers[c]['did_scan'] for c in active_customers)
            payment = customer_count * 25
            
            # Add summary row
            first_customer = active_customers[0] if active_customers else None
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
            
            # Add remaining customer rows
            for customer_mobile in active_customers[1:]:
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
        
        # Create DataFrame
        results_df = pd.DataFrame(all_rows)
        
        # Define column order
        columns = [
            'dsa_mobile', 'customer_mobile', 'full_name', 'bought_ticket', 
            'did_scan', 'deposited', 'onboarded_by', 'match_status',
            'Customer Count', 'Deposit Count', 'Ticket Count', 
            'Scan To Send Count', 'Payment'
        ]
        
        results_df = results_df[columns] if not results_df.empty else pd.DataFrame(columns=columns)
        
        return {
            "report_2_results": results_df,
            "customer_names": customer_names,
            "onboarding_map": onboarding_map,
            "dsa_customers": dsa_customers
        }
        
    except Exception as e:
        st.error(f"Error processing Report 2: {str(e)}")
        return None

def create_excel_download(data, report_type):
    """Create Excel file for download"""
    try:
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if report_type == "report_1":
                # Report 1 sheets
                data["qualified_customers"].to_excel(writer, index=False, sheet_name="Qualified_Customers")
                data["dsa_summary"].to_excel(writer, index=False, sheet_name="DSA_Summary")
                data["onboarded_customers"].to_excel(writer, index=False, sheet_name="All_Customers")
                data["ticket_details"].to_excel(writer, index=False, sheet_name="Ticket_Details")
                data["scan_details"].to_excel(writer, index=False, sheet_name="Scan_Details")
                data["deposit_details"].to_excel(writer, index=False, sheet_name="Deposit_Details")
            else:
                # Report 2 sheet
                data["report_2_results"].to_excel(writer, index=False, sheet_name="DSA_Analysis")
        
        output.seek(0)
        return output
    except Exception as e:
        st.error(f"Error creating Excel file: {str(e)}")
        return None

def display_metrics(data, report_type):
    """Display key metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    if report_type == "report_1":
        with col1:
            total_dsas = data["dsa_summary"]["dsa_mobile"].nunique()
            st.metric("Total DSAs", f"{total_dsas:,}")
        
        with col2:
            total_customers = len(data["onboarded_customers"])
            st.metric("Total Onboarded Customers", f"{total_customers:,}")
        
        with col3:
            qualified_count = len(data["qualified_customers"])
            st.metric("Qualified Customers", f"{qualified_count:,}")
        
        with col4:
            total_payment = (data["qualified_customers"]["Payment (Customer Count *40)"] / 2).sum()
            st.metric("Total Payment ($)", f"${total_payment:,.2f}")
    
    else:
        if not data["report_2_results"].empty:
            summary_rows = data["report_2_results"][data["report_2_results"]['Customer Count'] != '']
            
            with col1:
                total_dsas = summary_rows['dsa_mobile'].nunique()
                st.metric("Total DSAs", f"{total_dsas:,}")
            
            with col2:
                total_customers = summary_rows['Customer Count'].sum() if not summary_rows.empty else 0
                st.metric("Active Customers", f"{total_customers:,}")
            
            with col3:
                total_tickets = summary_rows['Ticket Count'].sum() if not summary_rows.empty else 0
                st.metric("Total Tickets", f"{total_tickets:,}")
            
            with col4:
                total_payment = summary_rows['Payment'].sum() if not summary_rows.empty else 0
                st.metric("Total Payment ($)", f"${total_payment:,.2f}")

def display_filters():
    """Display filtering options in sidebar"""
    st.sidebar.markdown("### 🔍 Filters")
    
    # Date range filter
    st.sidebar.markdown("**Date Range Filter**")
    date_option = st.sidebar.selectbox(
        "Select Date Range",
        ["All Time", "Last 7 Days", "Last 30 Days", "Custom Range"]
    )
    
    if date_option == "Custom Range":
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
        with col2:
            end_date = st.date_input("End Date", datetime.now())
    else:
        start_date = end_date = None
    
    # DSA filter
    st.sidebar.markdown("**DSA Filter**")
    dsa_option = st.sidebar.selectbox(
        "Select DSA Filter Mode",
        ["All DSAs", "Single DSA", "Multiple DSAs"]
    )
    
    dsa_list = []
    if 'processed_data' in st.session_state:
        if 'report_1_data' in st.session_state and st.session_state.report_1_data:
            dsa_list = st.session_state.report_1_data.get("dsa_summary", pd.DataFrame()).get("dsa_mobile", pd.Series()).unique().tolist()
    
    if dsa_option == "Single DSA":
        selected_dsa = st.sidebar.selectbox("Select DSA", dsa_list if dsa_list else ["No data available"])
    elif dsa_option == "Multiple DSAs":
        selected_dsas = st.sidebar.multiselect("Select DSAs", dsa_list if dsa_list else ["No data available"])
    else:
        selected_dsa = selected_dsas = None
    
    # Additional filters
    st.sidebar.markdown("**Additional Filters**")
    min_customers = st.sidebar.number_input("Minimum Customers", min_value=0, value=0)
    min_payment = st.sidebar.number_input("Minimum Payment ($)", min_value=0, value=0)
    
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
        df_to_filter = data["dsa_summary"].copy()
    else:
        df_to_filter = data["report_2_results"].copy()
    
    # Apply DSA filter
    if filters["dsa_option"] == "Single DSA" and filters["selected_dsa"]:
        df_to_filter = df_to_filter[df_to_filter["dsa_mobile"] == filters["selected_dsa"]]
    elif filters["dsa_option"] == "Multiple DSAs" and filters["selected_dsas"]:
        df_to_filter = df_to_filter[df_to_filter["dsa_mobile"].isin(filters["selected_dsas"])]
    
    # Apply minimum customers filter
    if report_type == "report_1":
        if filters["min_customers"] > 0:
            df_to_filter = df_to_filter[df_to_filter["Customer_Count"] >= filters["min_customers"]]
    else:
        if filters["min_customers"] > 0:
            summary_rows = df_to_filter[df_to_filter['Customer Count'] != '']
            valid_dsas = summary_rows[summary_rows['Customer Count'] >= filters["min_customers"]]['dsa_mobile']
            df_to_filter = df_to_filter[df_to_filter['dsa_mobile'].isin(valid_dsas)]
    
    # Apply minimum payment filter
    if report_type == "report_1":
        if filters["min_payment"] > 0:
            # Calculate payment for each DSA
            payment_data = data["qualified_customers"].groupby("dsa_mobile")["Payment (Customer Count *40)"].max().reset_index()
            valid_dsas = payment_data[payment_data["Payment (Customer Count *40)"] >= filters["min_payment"]]["dsa_mobile"]
            df_to_filter = df_to_filter[df_to_filter["dsa_mobile"].isin(valid_dsas)]
    else:
        if filters["min_payment"] > 0:
            summary_rows = df_to_filter[df_to_filter['Payment'] != '']
            valid_dsas = summary_rows[summary_rows['Payment'] >= filters["min_payment"]]['dsa_mobile']
            df_to_filter = df_to_filter[df_to_filter['dsa_mobile'].isin(valid_dsas)]
    
    return df_to_filter

def create_visualizations(data, report_type):
    """Create visualizations for the dashboard"""
    if report_type == "report_1":
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
        fig2 = px.bar(
            data["dsa_summary"].nlargest(10, "Deposit_Conversion_Rate"),
            x="dsa_mobile",
            y=["Deposit_Conversion_Rate", "Ticket_Conversion_Rate", "Scan_Conversion_Rate"],
            title="Top 10 DSAs by Conversion Rates",
            labels={"dsa_mobile": "DSA Mobile", "value": "Conversion Rate (%)"},
            barmode="group"
        )
        
        return fig1, fig2
    
    else:
        if data["report_2_results"].empty:
            return None, None
        
        # Get summary rows
        summary_rows = data["report_2_results"][data["report_2_results"]['Customer Count'] != '']
        
        if summary_rows.empty:
            return None, None
        
        # Visualization 1: Top DSAs by Payment
        top_payment = summary_rows.nlargest(10, "Payment")
        
        fig1 = px.bar(
            top_payment,
            x="dsa_mobile",
            y="Payment",
            title="Top 10 DSAs by Payment",
            labels={"dsa_mobile": "DSA Mobile", "Payment": "Payment Amount ($)"},
            color="Payment",
            color_continuous_scale="Plasma"
        )
        
        # Visualization 2: Match Status Distribution
        match_counts = data["report_2_results"]["match_status"].value_counts().reset_index()
        match_counts.columns = ["Match Status", "Count"]
        
        fig2 = px.pie(
            match_counts,
            values="Count",
            names="Match Status",
            title="Distribution of Customer Match Status",
            hole=0.4
        )
        
        return fig1, fig2

# Main application
def main():
    # Sidebar for file uploads
    st.sidebar.markdown("### 📁 Upload Data Files")
    
    # File uploaders
    onboarding_file = st.sidebar.file_uploader("Onboarding Data (CSV)", type=['csv'])
    ticket_file = st.sidebar.file_uploader("Ticket Data (CSV)", type=['csv'])
    conversion_file = st.sidebar.file_uploader("Conversion Data (CSV)", type=['csv'])
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
            conversion_df = pd.read_csv(conversion_file) if conversion_file else pd.DataFrame()
            
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
            
            with st.spinner("Processing Report 2..."):
                report_2_data = process_report_2(onboarding_df, deposit_df, ticket_df, scan_df)
                if report_2_data:
                    st.session_state.report_2_data = report_2_data
            
            st.sidebar.success("✓ Files processed successfully!")
            
        except Exception as e:
            st.sidebar.error(f"Error processing files: {str(e)}")
    
    # Get filters
    filters = display_filters()
    
    # Main content area
    if st.session_state.report_1_data or st.session_state.report_2_data:
        # Create tabs for different reports
        tab1, tab2, tab3, tab4 = st.tabs([
            "📋 Report 1: DSA Performance", 
            "📊 Report 2: Detailed Analysis", 
            "📈 Visualizations", 
            "📥 Download Reports"
        ])
        
        with tab1:
            if st.session_state.report_1_data:
                st.markdown('<div class="sub-header">Report 1: DSA Performance Summary</div>', unsafe_allow_html=True)
                
                # Display metrics
                display_metrics(st.session_state.report_1_data, "report_1")
                
                # Filter data if needed
                if filters["apply_filters"]:
                    filtered_data = filter_data(st.session_state.report_1_data, filters, "report_1")
                else:
                    filtered_data = st.session_state.report_1_data["dsa_summary"]
                
                # Display data
                st.markdown("#### DSA Summary Table")
                st.dataframe(filtered_data, use_container_width=True)
                
                # Show qualified customers
                with st.expander("View Qualified Customers Details"):
                    st.dataframe(st.session_state.report_1_data["qualified_customers"], use_container_width=True)
        
        with tab2:
            if st.session_state.report_2_data and not st.session_state.report_2_data["report_2_results"].empty:
                st.markdown('<div class="sub-header">Report 2: Detailed DSA Analysis</div>', unsafe_allow_html=True)
                
                # Display metrics
                display_metrics(st.session_state.report_2_data, "report_2")
                
                # Filter data if needed
                if filters["apply_filters"]:
                    filtered_data = filter_data(st.session_state.report_2_data, filters, "report_2")
                else:
                    filtered_data = st.session_state.report_2_data["report_2_results"]
                
                # Display data
                st.markdown("#### Detailed Analysis Results")
                st.dataframe(filtered_data, use_container_width=True)
                
                # Show statistics
                with st.expander("View Detailed Statistics"):
                    if not filtered_data.empty:
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**Transaction Patterns**")
                            summary_rows = filtered_data[filtered_data['Customer Count'] != '']
                            if not summary_rows.empty:
                                st.write(f"Total DSAs: {summary_rows['dsa_mobile'].nunique()}")
                                st.write(f"Total Active Customers: {summary_rows['Customer Count'].sum()}")
                                st.write(f"Total Tickets Purchased: {summary_rows['Ticket Count'].sum()}")
                                st.write(f"Total Scans Completed: {summary_rows['Scan To Send Count'].sum()}")
                        
                        with col2:
                            st.markdown("**Match Status**")
                            if 'match_status' in filtered_data.columns:
                                match_stats = filtered_data['match_status'].value_counts()
                                for status, count in match_stats.items():
                                    st.write(f"{status}: {count}")
        
        with tab3:
            st.markdown('<div class="sub-header">Data Visualizations</div>', unsafe_allow_html=True)
            
            # Create visualizations
            if st.session_state.report_1_data:
                fig1_r1, fig2_r1 = create_visualizations(st.session_state.report_1_data, "report_1")
                
                if fig1_r1 and fig2_r1:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.plotly_chart(fig1_r1, use_container_width=True)
                    with col2:
                        st.plotly_chart(fig2_r1, use_container_width=True)
            
            if st.session_state.report_2_data and not st.session_state.report_2_data["report_2_results"].empty:
                fig1_r2, fig2_r2 = create_visualizations(st.session_state.report_2_data, "report_2")
                
                if fig1_r2 and fig2_r2:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.plotly_chart(fig1_r2, use_container_width=True)
                    with col2:
                        st.plotly_chart(fig2_r2, use_container_width=True)
        
        with tab4:
            st.markdown('<div class="sub-header">Download Reports</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                if st.session_state.report_1_data:
                    st.markdown("#### Report 1: DSA Performance")
                    excel_file_1 = create_excel_download(st.session_state.report_1_data, "report_1")
                    
                    if excel_file_1:
                        st.download_button(
                            label="📥 Download Report 1 (Excel)",
                            data=excel_file_1,
                            file_name=f"DSA_Performance_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    # CSV download
                    csv_1 = st.session_state.report_1_data["dsa_summary"].to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Summary (CSV)",
                        data=csv_1,
                        file_name=f"DSA_Summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            
            with col2:
                if st.session_state.report_2_data and not st.session_state.report_2_data["report_2_results"].empty:
                    st.markdown("#### Report 2: Detailed Analysis")
                    excel_file_2 = create_excel_download(st.session_state.report_2_data, "report_2")
                    
                    if excel_file_2:
                        st.download_button(
                            label="📥 Download Report 2 (Excel)",
                            data=excel_file_2,
                            file_name=f"DSA_Detailed_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                    
                    # CSV download
                    csv_2 = st.session_state.report_2_data["report_2_results"].to_csv(index=False, sep='\t').encode('utf-8')
                    st.download_button(
                        label="📥 Download Analysis (CSV)",
                        data=csv_2,
                        file_name=f"DSA_Analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
    
    else:
        # Show instructions when no data is uploaded
        st.info("👋 Welcome to the DSA Performance Dashboard!")
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
        - 📋 **Two comprehensive reports** with different analysis approaches
        - 🔍 **Interactive filtering** by DSA, date range, and performance metrics
        - 📊 **Data visualizations** for insights
        - 📥 **Download reports** in Excel or CSV format
        - 🔄 **Real-time calculations** based on your filters
        
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
                st.write("- Transaction Type")
                st.write("- Amount")
                st.write("- Created By")
                st.write("- Full Name")
            
            with col2:
                st.markdown("**Onboarding Data Columns:**")
                st.write("- Mobile")
                st.write("- Customer Referrer Mobile")
                st.write("- Full Name")
                st.write("- Status")

if __name__ == "__main__":
    main()