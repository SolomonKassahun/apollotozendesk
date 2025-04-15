import pandas as pd
import streamlit as st
import phonenumbers
from phonenumbers.phonenumberutil import (
    region_code_for_number, 
    NumberParseException
)
import hashlib

st.title("Apollo to Zendesk Data Processor")

REQUIRED_COLUMNS = [
    "First Name", "Last Name", "Email", "Keywords",
    "Industry", "Corporate Phone", "Title", "Company"
]

def clean_phone(phone):
    """Standardize phone number format"""
    if pd.isna(phone) or not str(phone).strip():
        return ""
    
    phone = str(phone).strip()
    
    # Remove all non-digit characters except leading +
    cleaned = []
    for i, c in enumerate(phone):
        if c == '+' and i == 0:
            cleaned.append(c)
        elif c.isdigit():
            cleaned.append(c)
    cleaned = ''.join(cleaned)
    
    return cleaned if cleaned else ""

def validate_columns(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return missing

def get_region_tag(phone):
    """Determine region based on phone number"""
    if not phone:
        return "global"
    
    try:
        parsed = phonenumbers.parse(phone, None)
        country_code = region_code_for_number(parsed)
        
        if country_code == 'GB':
            return "region_uk"
        elif country_code == 'US':
            return "region_usa"
        else:
            return "global"
            
    except NumberParseException:
        # Fallback for numbers that can't be parsed
        if phone.startswith('+44') or phone.startswith('44'):
            return "region_uk"
        elif phone.startswith('+1') or phone.startswith('1'):
            return "region_usa"
        else:
            return "global"

def generate_external_id(email):
    """Generate consistent external_id from email"""
    if pd.isna(email) or not str(email).strip():
        return ""
    return hashlib.md5(email.strip().lower().encode()).hexdigest()

def process_file(file):
    try:
        df = pd.read_csv(file, dtype={'Corporate Phone': str})
        missing_columns = validate_columns(df)
        if missing_columns:
            return None, None, f"Missing columns: {', '.join(missing_columns)}"

        # Clean and standardize phone numbers
        df["Corporate Phone"] = df["Corporate Phone"].apply(clean_phone)
        
        # Filter valid rows
        valid_rows = df[
            df["First Name"].notna() & 
            df["First Name"].astype(str).str.strip().ne("") &
            df["Company"].notna() & 
            df["Company"].astype(str).str.strip().ne("") &
            df["Corporate Phone"].astype(str).str.strip().ne("") &
            df["Email"].notna() & 
            df["Email"].astype(str).str.strip().ne("")
        ].copy()

        if valid_rows.empty:
            return None, None, "No valid rows found."

        # Add region tags
        valid_rows["tags"] = valid_rows["Corporate Phone"].apply(get_region_tag)

        # Create user data
        user_df = pd.DataFrame({
            "name": valid_rows["First Name"].str.strip() + " " + 
                   valid_rows["Last Name"].fillna("").str.strip(),
            "email": valid_rows["Email"],
            "external_id": valid_rows["Email"].apply(generate_external_id),
            "details": valid_rows["Keywords"],
            "notes": valid_rows["Title"],
            "phone": valid_rows["Corporate Phone"].apply(
                lambda x: f"+{x}" if x and not x.startswith('+') else x
            ),
            "role": valid_rows["Title"],
            "restriction": "",
            "organization": valid_rows["Company"],
            "tags": valid_rows["tags"],
            "brand": "",
            "custom_fields.<fieldkey>": ""
        })

        # Create organization data
        org_df = valid_rows.groupby("Company").agg({
            "Industry": "first",
            "Corporate Phone": "first",
            "tags": "first"
        }).reset_index()
        org_df = pd.DataFrame({
            "name": org_df["Company"],
            "external_id": org_df["Company"].apply(generate_external_id),
            "notes": org_df["Industry"],
            "details": "",
            "default": "",
            "shared": "",
            "shared_comments": "",
            "group": "",
            "tags": org_df["tags"],
            "custom_fields.<fieldkey>": ""
        })

        return user_df, org_df, None

    except Exception as e:
        return None, None, f"Error while processing file: {str(e)}"

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

if uploaded_file:
    st.info("Processing file...")
    user_df, org_df, error = process_file(uploaded_file)

    if error:
        st.error(error)
    else:
        st.success("Files processed successfully!")
        
        # Show sample of phone numbers and their tags
        st.write("Sample Phone Number Tags:")
        st.write(user_df[["phone", "tags"]].head(10))

        user_csv = user_df.to_csv(index=False).encode('utf-8')
        org_csv = org_df.to_csv(index=False).encode('utf-8')

        st.download_button("Download User CSV", user_csv, "user.csv", "text/csv")
        st.download_button("Download Organization CSV", org_csv, "organization.csv", "text/csv")
