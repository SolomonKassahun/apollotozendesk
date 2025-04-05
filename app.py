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

            # Only keep rows that have First Name, Company, and Corporate Phone
            chunk = chunk[
                chunk["First Name"].notna() & chunk["First Name"].astype(str).str.strip().ne("") &
                chunk["Company"].notna() & chunk["Company"].astype(str).str.strip().ne("") &
                chunk["Corporate Phone"].astype(str).str.strip().ne("")
            ]

            if chunk.empty:
                continue 

            user_chunk = pd.DataFrame({
                "name": chunk["First Name"].astype(str).str.strip() + " " + chunk["Last Name"].fillna("").astype(str).str.strip(),
                "email": chunk["Email"],
                "external_id": range(1234567, 1234567 + len(chunk)),
                "details": chunk["Keywords"],
                "notes": chunk["Industry"],
                "phone": chunk["Corporate Phone"],
                "role": chunk["Title"],
                "restriction": "",
                "organization": chunk["Company"],
                "tags": "",
                "brand": "",
                "custom_fields.<fieldkey>": ""
            })
            user_data.append(user_chunk)

            for company in chunk["Company"].unique():
                org_chunk = chunk[chunk["Company"] == company]
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
            return None, None, "No valid rows found. All rows were missing First Name, Company, or Corporate Phone."

        user_df = pd.concat(user_data, ignore_index=True)
        org_df = pd.DataFrame.from_dict(organization_data, orient="index")

        return user_df, org_df, None

    except Exception as e:
        return None, None, f"Error while processing file: {str(e)}"
