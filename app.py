import pandas as pd
import streamlit as st

st.title("Apollo to Zendesk Data Processor")

REQUIRED_COLUMNS = [
    "First Name", "Last Name", "Email", "Keywords",
    "Industry", "Corporate Phone", "Title", "Company"
]

def clean_phone(phone):
    if pd.isna(phone):
        return ""
    return str(phone).replace("'", "").strip()

def validate_columns(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return missing

def process_file(file):
    try:
        df = pd.read_csv(file, chunksize=10000)
        user_data = []
        organization_data = {}

        for chunk in df:
            missing_columns = validate_columns(chunk)
            if missing_columns:
                return None, None, f"Missing columns: {', '.join(missing_columns)}"

            chunk["Corporate Phone"] = chunk["Corporate Phone"].apply(clean_phone)

            valid_rows = chunk[
                chunk["First Name"].notna() & chunk["First Name"].astype(str).str.strip().ne("") &
                chunk["Company"].notna() & chunk["Company"].astype(str).str.strip().ne("") &
                chunk["Corporate Phone"].astype(str).str.strip().ne("")
            ]

            if valid_rows.empty:
                continue

            user_chunk = pd.DataFrame({
                "name": valid_rows["First Name"].astype(str).str.strip() + " " + valid_rows["Last Name"].fillna("").astype(str).str.strip(),
                "email": valid_rows["Email"],
                "external_id": range(1234567, 1234567 + len(valid_rows)),
                "details": valid_rows["Keywords"],
                "notes": valid_rows["Industry"],
                "phone": valid_rows["Corporate Phone"],
                "role": valid_rows["Title"],
                "restriction": "",
                "organization": valid_rows["Company"],
                "tags": "",
                "brand": "",
                "custom_fields.<fieldkey>": ""
            })
            user_data.append(user_chunk)

            for company in valid_rows["Company"].unique():
                org_chunk = valid_rows[valid_rows["Company"] == company]
                if company not in organization_data and not org_chunk.empty:
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

        if not user_data:
            return None, None, "No valid rows found. All rows were missing First Name, Company, or Phone."

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

        user_csv = user_df.to_csv(index=False).encode('utf-8')
        org_csv = org_df.to_csv(index=False).encode('utf-8')

        st.download_button("Download User CSV", user_csv, "user.csv", "text/csv")
        st.download_button("Download Organization CSV", org_csv, "organization.csv", "text/csv")
