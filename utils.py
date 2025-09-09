import os
from dotenv import load_dotenv
import pandas as pd
import yaml
from io import StringIO
import pysurveycto as pcto
import importlib.util
import re
import subprocess
import tempfile
import streamlit as st
import requests
from requests.exceptions import RequestException
import pycountry



def load_config():
    """
    Loads and parses the `config.yaml` file from the current directory.
    This file contains metadata and instructions for how each tab in the app should behave,
    including data source names, filtering rules, column labels, and file attachment settings.
    """
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path) as f:
        return yaml.safe_load(f)


config = load_config()
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))



def connect_scto():
    """
    Connects to the SurveyCTO server using credentials from environment variables.
    Expected env vars: SCTO_SERVER, SCTO_USERNAME, SCTO_PASSWORD
    """
    server = os.environ.get("SCTO_SERVER")
    username = os.environ.get("SCTO_USERNAME")
    password = os.environ.get("SCTO_PASSWORD")

    if not all([server, username, password]):
        return None

    return pcto.SurveyCTOObject(server, username, password)


@st.cache_data(ttl=600, show_spinner=False)
def load_processed_dataset(tab_config):
    """
    Loads, processes, and returns a cleaned dataset from the SurveyCTO server based on the provided tab configuration.

    This function performs the following operations:
    1. Downloads the dataset from SurveyCTO using the source specified in `tab_config["source"]`.
    2. Applies optional row filtering based on a column-value pair in `tab_config["filter"]`.
    3. Merges attachment filename columns from a secondary form if any `_file` columns are missing.
    4. Executes any custom scripts located in the `specific_scripts/` directory on the dataset.
    5. Optionally applies SurveyCTO form definition cleaning to enrich metadata for select_one and select_multiple fields.
    6. Optionally applies column labels from a user-specified label mapping.

    Parameters:
        tab_config (dict): A dictionary defining the configuration for the tab. Expected keys include:
            - "source" (str): The SurveyCTO dataset name to fetch.
            - "filter" (dict, optional): A dictionary with keys "column" and "value" to filter rows.
            - "attachments" (dict, optional): Configuration for merging attachment filenames from another form.
            - "scto_cleaning" (dict, optional): XLSForm-based metadata cleaning settings.
            - "column_labels" (dict, optional): File and field mappings for applying user-friendly column names.

    Returns:
        tuple:
            - df (pd.DataFrame): The processed dataset with all transformations applied.
            - column_labels (dict): A mapping from raw column names to human-readable labels.
            - label_info (dict): Metadata for select_one/select_multiple field labeling used for display and filtering.
    """

    #try:
    scto = connect_scto()
    source = tab_config["source"]
    source_type = tab_config.get("source_type", "dataset")

    if scto:
        try:
            if source_type == "dataset":
                data_str = scto.get_server_dataset(source)
            elif source_type == "form":
                data_str = scto.get_form_data(source)
            else:
                raise ValueError(f"Unknown source type: {source_type}")

            df = pd.read_csv(StringIO(data_str.strip()))
        except Exception as e:
            st.warning(f"[WARNING] Could not fetch or parse SurveyCTO data: {e}")
            return df, column_labels, label_info
    else:
        try:
            df = pd.read_csv("dataset.csv")
        except Exception as e:
            st.error(f"[ERROR] Could not load fallback dataset.xlsx: {e}")
            return df, column_labels, label_info

    #     if source_type == "dataset":
    #         data_str = scto.get_server_dataset(source)
    #     elif source_type == "form":
    #         data_str = scto.get_form_data(source)
    #     else:
    #         raise ValueError(f"Unknown source type: {source_type}")


    # except Exception as e:
    #     st.warning(f"[WARNING] Could not fetch from SurveyCTO: {e}")
    #     return pd.DataFrame(), {}, {}
    

    # try:
    #     df = pd.read_csv(StringIO(data_str.strip()))
    # except Exception as e:
    #     st.error(f"[ERROR] Could not parse downloaded CSV: {e}")
    #     return pd.DataFrame(), {}, {}

    filter_config = tab_config.get("filter")
    if filter_config:
        col = filter_config.get("column")
        val = filter_config.get("value")
        if col in df.columns:
            df = df[df[col] == val]

    attach_cfg = tab_config.get("attachments", {})
    attachment_fields = attach_cfg.get("fields", []) if isinstance(attach_cfg, dict) else []

    if any(f"{col}_file" not in df.columns for col in attachment_fields):
        df = merge_attachments(df, tab_config)
    
    df = run_specific_scripts(df)

    label_info = {}
    scto_cleaning_config = tab_config.get("scto_cleaning", {})
    if scto_cleaning_config.get("enabled"):
        try:
            df, label_info = apply_scto_cleaning(
                df,
                file=scto_cleaning_config.get("definition_file"),
                survey_sheet=scto_cleaning_config.get("survey_sheet", "survey"),
                choices_sheet=scto_cleaning_config.get("choices_sheet", "choices")
            )
        except Exception as e:
            st.warning(f"Could not apply SCTO value-label transformation: {e}")

    column_labels = {}
    if "column_labels" in tab_config:
        try:
            column_labels = apply_column_labels(df.copy(), tab_config["column_labels"])
        except Exception as e:
            st.warning(f"Could not apply column labels: {e}")



    return df, column_labels, label_info




def get_attachment(url):
    """
    Downloads a file (attachment) from the SurveyCTO server given its URL.
    Assumes user credentials are available and valid.
    """
    scto = connect_scto()
    return scto.get_attachment(url)



def merge_attachments(df_main, tab_config):
    """
    Merges attachment columns from a separate form submission into the main dataset.
    - Downloads the secondary form based on `form_id` in `tab_config['attachments_from_form']`
    - Matches rows using a shared key (e.g., `KEY`, `email`, `caseid`)
    - Merges only the specified attachment fields
    - Avoids duplicate column names by renaming existing columns to `<field>_file`
    Returns the merged DataFrame.
    """
    attach_cfg = tab_config.get("attachments", {})
    form_cfg = attach_cfg.get("from_form")
    attachment_fields = attach_cfg.get("fields", [])

    if not form_cfg:
        return df_main 

    form_id = form_cfg.get("form_id")
    match_on = form_cfg.get("match_on")

    try:
        scto = connect_scto()
        form_str = scto.get_form_data(form_id=form_id, format='csv', shape='wide')
        df_form = pd.read_csv(StringIO(form_str.strip()))

        if match_on in df_main.columns and match_on in df_form.columns:
            for col in attachment_fields:
                if col in df_main.columns:
                    df_main = df_main.rename(columns={col: f"{col}_file"})


            keep_fields = [match_on] + [c for c in attachment_fields if c in df_form.columns]
            df_form = df_form[keep_fields]

            return pd.merge(df_main, df_form, on=match_on, how='left')

        else:
            print(f"[Warning] Cannot merge attachments: '{match_on}' not in both datasets.")
    except Exception as e:
        print(f"[Error] Failed to fetch attachment form: {e}")

    return df_main



def apply_column_labels(df, label_cfg):
    """
    Loads a mapping of internal column names to user-friendly labels from a CSV or Excel file.
    Falls back to title-case if no mapping is found.

    Inputs:
        - `label_cfg['file']`: path to CSV or Excel
        - `label_cfg['column_header_col']`: name of column containing raw header names
        - `label_cfg['column_label_col']`: name of column containing human-readable labels
    Returns:
        - Dictionary mapping raw column names → human-friendly labels
    """
    file = label_cfg["file"]
    header_col = label_cfg["column_header_col"]
    label_col = label_cfg["column_label_col"]
    sheet = label_cfg.get("sheet")

    if file.endswith(".csv"):
        label_df = pd.read_csv(file)
    else:
        label_df = pd.read_excel(file, sheet_name=sheet or 0)

    label_df = label_df.dropna(subset=[header_col, label_col])

    label_df[label_col] = label_df[label_col].astype(str).str.strip()

    label_map = dict(zip(label_df[header_col], label_df[label_col]))

    final_map = {
        col: label_map.get(col, col.replace("_", " ").title())
        for col in df.columns
    }

    return final_map




def apply_scto_cleaning(df, file, survey_sheet="survey", choices_sheet="choices"):
    """
    Extracts metadata to support label-based display of select_one and select_multiple fields
    without modifying the underlying dataset values.

    Returns:
        - original df (unchanged)
        - label_info: {
              "select_one": {field: list_name},
              "select_multiple": {field: list_name},
              "label_map": {list_name: {value: label}}
          }
    """
    survey_def = pd.read_excel(file, sheet_name=survey_sheet)
    choices_def = pd.read_excel(file, sheet_name=choices_sheet)

    select_one_fields = {}
    select_multiple_fields = {}

    for _, row in survey_def.iterrows():
        field_name = row.get("name")
        field_type = str(row.get("type"))
        if field_name and isinstance(field_type, str):
            if field_type.startswith("select_one "):
                list_name = field_type.replace("select_one ", "").strip()
                select_one_fields[field_name] = list_name
            elif field_type.startswith("select_multiple "):
                list_name = field_type.replace("select_multiple ", "").strip()
                select_multiple_fields[field_name] = list_name

    label_map = (
        choices_def.dropna(subset=["list_name", "value", "label"])
        .assign(value=lambda d: d["value"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip())
        .groupby("list_name")
        .apply(lambda g: dict(zip(g["value"], g["label"])))
        .to_dict()
    )

    label_info = {
        "select_one": select_one_fields,
        "select_multiple": select_multiple_fields,
        "label_map": label_map
    }

    return df, label_info

def get_label(field, value, label_info):
    list_name = label_info["select_one"].get(field)
    return label_info["label_map"].get(list_name, {}).get(str(value).strip(), value)

def get_labels_for_multiple(field, value_string, label_info):
    list_name = label_info["select_multiple"].get(field)
    if not list_name or not isinstance(value_string, str):
        return value_string
    codes = [code.strip() for code in value_string.strip().split()]
    return " | ".join([label_info["label_map"].get(list_name, {}).get(code, code) for code in codes])



def run_specific_scripts(df):
    """
    Searches for and executes any `.py` or `.R` scripts in the `specific_scripts/` folder.
    
    - For Python scripts:
      - Loads the script
      - Injects the current DataFrame as `df`
      - Expects the script to modify and return the same `df`
    
    - For R scripts:
      - Writes `df` to a temporary CSV
      - Calls the R script via subprocess
      - Reads the updated CSV back into `df`
    
    Returns the final version of the DataFrame after all scripts have run.
    """
    folder = "specific_scripts"
    for filename in sorted(os.listdir(folder)):
        path = os.path.join(folder, filename)
        
        
        if filename.endswith(".py"):
            try:
                print(f"[Info] Running script: {path}")
                with open(path) as f:
                    code = f.read()
                local_vars = {'df': df, 'pycountry': pycountry, 'pd': pd, 'os': os, '__file__': path }
                exec(code, local_vars)
                df = local_vars['df']  # Update df from script
            except Exception as e:
                print(f"[ERROR] Failed to run {filename}: {e}")
        
        
        
        elif filename.endswith(".R"):
            try:
                print(f"[Info] Running R script: {filename}")
                
                # Save current df to temp CSV
                temp_input = os.path.join(folder, "temp_input.csv")
                temp_output = os.path.join(folder, "temp_output.csv")
                df.to_csv(temp_input, index=False)

                # Call the R script with input/output file paths as arguments
                result = subprocess.run(
                    ["Rscript", path, temp_input, temp_output],
                    capture_output=True,
                    text=True
                )

                if result.returncode != 0:
                    print(f"[ERROR] R script failed: {result.stderr}")
                else:
                    df = pd.read_csv(temp_output)
            except Exception as e:
                print(f"[ERROR] Failed to run R script {filename}: {e}")

    return df


def write_dataset(df, dataset_id, tab_config=None, dataset_title=None, append=None):
    """
    Writes data to a SurveyCTO server dataset with safety checks to avoid accidental overwrites.

    Parameters:
        - df: pandas DataFrame to upload
        - dataset_id: str, ID of the dataset on the server
        - dataset_title: Optional str, new title to give the dataset
        - append: Must be explicitly True or False. Raises error if None.

    Returns:
        - dict: upload response
    """
    if append is None:
        raise ValueError(
            "You must explicitly set `append=True` to add data, or `append=False` and `confirm_replace=True` to overwrite. "
            "To avoid accidental data loss, this default is disabled."
        )
    
    if tab_config:
        attachments = tab_config.get("attachments", {})
        attachment_fields = attachments.get("fields", []) if isinstance(attachments, dict) else []
        exclude_columns = {f"{field}_file" for field in attachment_fields}
        df = df[[col for col in df.columns if col not in exclude_columns]]

    scto = connect_scto()

    headers = scto._SurveyCTOObject__auth()  
    csrf_token = headers["X-csrf-token"]

    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode='w', newline='') as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name

    try:
        upload_url = f"https://{scto.server_name}.surveycto.com/datasets/{dataset_id}/upload"
        files = {
            "dataset_file": (os.path.basename(tmp_path), open(tmp_path, "rb"), "text/csv")
        }
        payload = {
            "dataset_exists": "1",
            "dataset_id": dataset_id,
            "dataset_title": dataset_title or dataset_id,
            "dataset_upload_mode": "append" if append else "clear",
            "dataset_type": "SERVER"
        }

        response = scto._sesh.post(
            upload_url,
            data=payload,
            files=files,
            cookies=scto._sesh.cookies,
            headers={"X-csrf-token": csrf_token}
        )
        response.raise_for_status()

        return {"response": response.json()}

    finally:
        os.remove(tmp_path)


def run_custom_ui_script(action: str, tab_config: dict, **kwargs):
    key = tab_config.get("key")
    script_path = os.path.join("custom_ui", f"{key}_{action}.py")

    if not os.path.exists(script_path):
        return

    try:
        spec = importlib.util.spec_from_file_location(f"custom_ui.{action}", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if hasattr(module, "render"):
            module.render(config=tab_config, **kwargs)
    except Exception as e:
        st.warning(f"Custom UI error in '{script_path}': {e}")


def collect_row_attachments(row, tab_config):
    """
    Collects attachments for a single row.
    Returns a list of (filename, content) tuples.
    """
    attachments = []
    attachment_config = tab_config.get("attachments", {})
    attachment_fields = attachment_config.get("fields", [])

    for field in attachment_fields:
        link = row.get(field, '')
        filename = row.get(f"{field}_file", '') or f"{field}.dat"

        if isinstance(link, str) and link.startswith("http"):
            try:
                data = get_attachment(link)
                attachments.append((filename, data))
            except Exception as e:
                print(f"[WARNING] Could not download {field}: {e}")
    return attachments
