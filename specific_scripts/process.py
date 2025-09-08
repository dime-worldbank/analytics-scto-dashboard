role_suffix_map = {
    "RA": "_ra",
    "FC": "_fc",
    "Hybrid": "_hyb",
    "Other": ""  # Other fields have no suffix
}


base_columns = [
    'project_name', 'project_location', 'pi_name', 'pi_add',
    'project_blurb', 'hire_role', 'hire_role_other', 'KEY', 'hire_predoc_ra'
]

column_prefixes = ['number_role', 'hire_position_location', 'hire_desired_date',
                   'position_details_others', 'language', 'skill_labels',
                   'language_stata', 'language_r', 'language_scto', 'language_other', "language_bigdata", "language_python"]

long_rows = []

for _, row in df.iterrows():
    roles = str(row.get("hire_role", "")).split()

    for role in roles:
        role_clean = role.strip()
        if role_clean in role_suffix_map:
            suffix = role_suffix_map[role_clean]

            new_row = {col: row[col] for col in base_columns if col in row}
            if role_clean == "Other":
                role_other_value = row.get("hire_role_other", "").strip()
                new_row['hire_role'] = role_other_value or "Other"
            else:
                new_row['hire_role'] = role_clean

            for col_prefix in column_prefixes:
                full_col = f"{col_prefix}{suffix}"
                if full_col in row and pd.notna(row[full_col]):
                    new_row[col_prefix] = row[full_col]

            long_rows.append(new_row)

df = pd.DataFrame(long_rows)

df["hire_position_location"] = df["hire_position_location"].apply(
    lambda text: ", ".join([
        name for name in sorted([c.name for c in pycountry.countries] + ["Global"], key=lambda x: -len(x)) if name in str(text)
    ])
)

df["number_role"] = df["number_role"].astype(int)

df = df.replace("‚Äô", "'", regex=True)
df.loc[df["pi_add"] == "No", "pi_name"] = ""



print(list(df.columns))
