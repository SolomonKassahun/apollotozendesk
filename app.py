# app.py

import pandas as pd
import streamlit as st
import re
import io

st.title("Apollo to Zendesk data Processor")

# Expected columns
REQUIRED_COLUMNS = ["First Name", "Last Name", "Email", "Keywords", "Industry", "Corporate Phone", "Title", "Company"]

def clean_phone(phone):
    if pd.isna(phone):
        return ""
    return str(phone).replace("'", "")

def validate_columns(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return missing

def process_file(file):
    try:
        df = pd.read_csv(file, chunksize=10000)
        user_data = []
        organization_data = {}

        for chunk in df:
            # Column validation for each chunk
            missing_columns = validate_columns(chunk)
            if missing_columns:
                return None, None, f"Missing columns: {', '.join(missing_columns)}"

            user_chunk = pd.DataFrame({
                "name": chunk["First Name"] + " " + chunk["Last Name"],
                "email": chunk["Email"],
                "external_id": range(1234567, 1234567 + len(chunk)),  
                "details": chunk["Keywords"],
                "notes": chunk["Industry"],
                "phone": chunk["Corporate Phone"].apply(clean_phone),  
                "role": chunk["Title"],
                "restriction": "",
                "organization": chunk["Company"],
                "tags": "",
                "brand": "",
                "custom_fields.<fieldkey>": ""
            })
            user_data.append(user_chunk)

            for company in chunk["Company"].unique():
                if company not in organization_data:
                    org_chunk = chunk.loc[chunk["Company"] == company]
                    organization_data[company] = {
                        "name": company,
                        "external_id": len(organization_data) + 1234456,
                        "notes": org_chunk["Industry"].iloc[0] if not org_chunk["Industry"].isna().all() else "",
                        "details": "",
                        "default": "",
                        "shared": "",
                        "shared_comments": "",
                        "group": "",
                        "tags": "",
                        "custom_fields.<fieldkey>": ""
                    }

        user_df = pd.concat(user_data, ignore_index=True)
        org_df = pd.DataFrame.from_dict(organization_data, orient="index")

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

        # Download buttons
        user_csv = user_df.to_csv(index=False).encode('utf-8')
        org_csv = org_df.to_csv(index=False).encode('utf-8')

        st.download_button("Download User CSV", user_csv, "user.csv", "text/csv")
        st.download_button("Download Organization CSV", org_csv, "organization.csv", "text/csv")

