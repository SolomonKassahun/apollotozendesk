import pandas as pd
import streamlit as st
import phonenumbers
from phonenumbers.phonenumberutil import region_code_for_number
import pytz
from timezonefinder import TimezoneFinder
from geopy.geocoders import Nominatim

st.title("Apollo to Zendesk Data Processor")

REQUIRED_COLUMNS = [
    "First Name", "Last Name", "Email", "Keywords",
    "Industry", "Corporate Phone", "Title", "Company"
]

geolocator = Nominatim(user_agent="timezone_locator")
tf = TimezoneFinder()

def clean_phone(phone):
    if pd.isna(phone):
        return ""
    phone_str = str(phone).strip().replace("'", "")

    try:
        parsed = phonenumbers.parse(phone_str, None)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except:
        pass

    # Add + based on known prefixes
    if phone_str.startswith("44") or phone_str.startswith("1"):
        return f"+{phone_str}"
    elif not phone_str.startswith("+"):
        return f"+{phone_str}"
    return phone_str

def validate_columns(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    return missing

def get_timezone_tag(phone):
    try:
        parsed = phonenumbers.parse(phone, None)
        region_code = region_code_for_number(parsed)
        location = geolocator.geocode(region_code)
        if not location:
            return "global"

        tz_str = tf.timezone_at(lng=location.longitude, lat=location.latitude)
        if not tz_str:
            return "global"

        tz = pytz.timezone(tz_str)
        utc_offset = tz.utcoffset(pd.Timestamp.now())

        if not utc_offset:
            return "global"

        hours = utc_offset.total_seconds() / 3600

        if -8 <= hours <= -3:
            return "region_usa"
        elif 0 <= hours <= 2:
            return "region_uk"
        else:
            return "global"
    except Exception:
        return "global"

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

            valid_rows["tags"] = valid_rows["Corporate Phone"].apply(get_timezone_tag)

            user_chunk = pd.DataFrame({
                "name": valid_rows["First Name"].astype(str).str.strip() + " " + valid_rows["Last Name"].fillna("").astype(str).str.strip(),
                "email": valid_rows["Email"],
                "external_id": range(1234567, 1234567 + len(valid_rows)),
                "details": valid_rows["Keywords"],
                "notes": valid_rows["Title"],
                "phone": valid_rows["Corporate Phone"],
                "role": valid_rows["Title"],
                "restriction": "",
                "organization": valid_rows["Company"],
                "tags": valid_rows["tags"],
                "brand": "",
                "custom_fields.<fieldkey>": ""
            })
            user_data.append(user_chunk)

            for company in valid_rows["Company"].unique():
                if company not in organization_data:
                    org_chunk = valid_rows[valid_rows["Company"] == company]
                    if not org_chunk.empty:
                        phone = org_chunk["Corporate Phone"].iloc[0]
                        tag = get_timezone_tag(phone)
                        organization_data[company] = {
                            "name": company,
                            "external_id": len(organization_data) + 1234456,
                            "notes": org_chunk["Industry"].iloc[0] if not org_chunk["Industry"].isna().all() else "",
                            "details": "",
                            "default": "",
                            "shared": "",
                            "shared_comments": "",
                            "group": "",
                            "tags": tag,
                            "custom_fields.<fieldkey>": ""
                        }

        if not user_data:
            return None, None, "No valid rows found. All rows were missing First Name, Company, or Phone."

        user_df = pd.concat(user_data, ignore_index=True)
        org_df = pd.DataFrame.from_dict(organization_data, orient="index")

        # Final cleaning: ensure all phone numbers have +
        user_df["phone"] = user_df["phone"].apply(clean_phone)

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
