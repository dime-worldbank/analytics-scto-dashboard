import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
import pandas as pd

import os
import datetime


from utils import (
    config,
    load_processed_dataset,
    get_label,
    get_labels_for_multiple,
    run_custom_ui_script,
)


# --- PAGE SETUP ---
st.set_page_config(page_title=config['info'].get('title'), layout="wide")


# --- NAVIGATION ---
tabs = st.tabs(["Info", "Open positions"])

with tabs[0]:
    st.title(config['info'].get('title', 'Info'))
    st.markdown(config['info'].get('description', ''))
                
with tabs[1]:

    tab_config = next(t for t in config['tabs'] if t['key'] == 'pos')

    # --- LOAD DATASET ---
    dataset_key = tab_config['key']

    if dataset_key not in st.session_state:
        with st.spinner("Loading data..."):
            df, column_labels, label_info = load_processed_dataset(tab_config)

            st.session_state[dataset_key] = df
            st.session_state[f"{dataset_key}_labels"] = column_labels
            st.session_state[f"{dataset_key}_label_info"] = label_info


    # Retrieve from session
    full_df = st.session_state[dataset_key]
    column_labels = st.session_state.get(f"{dataset_key}_labels", {})


    label_info = st.session_state.get(f"{dataset_key}_label_info", {})
    label_map = label_info.get("label_map", {})
    select_one = label_info.get("select_one", {})
    select_multiple = label_info.get("select_multiple", {})

    filtered_df = full_df.copy()

    # --- DETERMINE DISPLAY COLUMNS ---
    if 'default_columns' in tab_config:
        display_columns = tab_config['default_columns'] 
    elif 'exclude_columns' in tab_config:
        display_columns = [col for col in filtered_df.columns if col not in tab_config['exclude_columns']]
    else:
        #st.error("No columns specified to display: please define either 'default_columns' or 'exclude_columns' in the config.")
        #st.stop()
        display_columns = list(filtered_df.columns)



    # --- DETAIL VIEW ---
    detail_index = st.session_state.get('detail_index')

    if detail_index is not None:
        # Get selected row
        row = full_df.loc[detail_index]

        # Combine lists and preserve order
        default_cols = tab_config.get("default_columns", [])
        detail_cols = tab_config.get("detail_columns", [])
        configured_cols = []

        for col in default_cols + detail_cols:
            if col not in configured_cols:
                configured_cols.append(col)

        # Filter out any columns not in the DataFrame
        ordered_cols = [col for col in configured_cols if col in full_df.columns]


        row_display = {}
        label_info = st.session_state.get(f"{dataset_key}_label_info", {})

        for col in ordered_cols:
            val = row.get(col)
            if col in label_info.get("select_one", {}):
                val = get_label(col, val, label_info)
            elif col in label_info.get("select_multiple", {}):
                val = get_labels_for_multiple(col, val, label_info)

            if isinstance(val, (pd.Timestamp, datetime.datetime)):
                val = val.strftime("%Y-%m-%d %H:%M")

            label = column_labels.get(col, col)
            row_display[label] = val


        button_cols = st.columns(10)

        with button_cols[0]:
            if st.button("<- Back", type="secondary", width="stretch"):
                del st.session_state['detail_index']
                st.rerun()

        with button_cols[-1]:
            st.link_button(label="Apply", url="https://survey.wb.surveycto.com/collect/dime_stc_application_fall2025?caseid=", type="secondary", width="stretch")

        if tab_config.get("custom_ui", False):
            key = tab_config.get("key")
            ui_folder = "custom_ui"
            pattern_prefix = f"{key}_"
            pattern_suffix = ".py"

            ui_scripts = [
                f for f in sorted(os.listdir(ui_folder))
                if f.startswith(pattern_prefix) and f.endswith(pattern_suffix)
            ][:5]
            
            total_slots = 5
            start_index = 4 

            for i, filename in enumerate(ui_scripts):
                action = filename[len(pattern_prefix):-len(pattern_suffix)]
                label = action.replace("_", " ").title()
                session_key = f"run_custom_ui_{action}"
                button_key = f"btn_{key}_{action}"

                col_index = start_index - i
                if col_index < 0:
                    break

                with button_cols[col_index]:
                    if st.button(label, key=button_key):
                        st.session_state[session_key] = True

        
        
            # Render custom UI full-width after button bar
            for filename in ui_scripts:
                action = filename[len(pattern_prefix):-len(pattern_suffix)]
                if st.session_state.get(f"run_custom_ui_{action}"):
                    run_custom_ui_script(action, tab_config, df=full_df, row=row)


        # Detail content display
        st.title("Position details")

        cols = st.columns(2)
        items = [
            (k, v) for k, v in row_display.items()
            if pd.notna(v) and str(v).strip() != ""
        ]

        for k, v in items:
            row = st.columns([1, 4])  # label col, value col
            with row[0]:
                st.markdown(f"**{k}**", unsafe_allow_html=True)
            with row[1]:
                st.markdown(str(v).replace("$", r"\$").replace(" | ", "<br>"), unsafe_allow_html=True)


    # --- TABLE VIEW ---
    else:
        button_cols = st.columns([1,1,1,1,1,1])
        
        st.title(tab_config['title'])

        selected_columns = [col for col in tab_config.get("default_columns", []) if col in full_df.columns]


        # Reapply style. THIS IS IMPORTANT TO PREVENT UI FROM FLICKERING
        st.markdown(
            """
            <style>
            div[data-baseweb="select"] {
                overflow: hidden !important;
                height: 40px !important;
            }
            div[data-baseweb="select"] > div {
                max-height: 40px;
                overflow-y: auto;
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # Prepare table
        visible_df = filtered_df[selected_columns].copy()
        visible_df['row_index'] = filtered_df.index
    
        for col in selected_columns:
            if col in select_one:
                list_name = select_one[col]
                visible_df[col] = visible_df[col].astype(str).str.strip().replace(r"\\.0$", "", regex=True)
                visible_df[col] = visible_df[col].map(label_map.get(list_name, {}))
            elif col in select_multiple:
                list_name = select_multiple[col]
                visible_df[col] = visible_df[col].apply(lambda val: get_labels_for_multiple(col, val, label_info))

        visible_df = visible_df.rename(columns=column_labels)

        if visible_df.columns.duplicated().any():
            duplicates = visible_df.columns[visible_df.columns.duplicated()].tolist()
            st.error(f"Duplicate column names detected: {duplicates}. Column names must be unique to display the table.")
            st.stop()


        # Configure AgGrid
        gb = GridOptionsBuilder.from_dataframe(visible_df)
        
        for col in visible_df.columns:
            if col != "row_index":
                gb.configure_column(col, minWidth=300)

        gb.configure_default_column(filter=True, sortable=True, resizable=True)
        gb.configure_column("row_index", hide=True)
        gb.configure_grid_options(
            domLayout='normal',
            rowSelection='single',
            quickFilter=True,
            paginationPageSize=20,
            pagination=True
        )
        grid_options = gb.build()

        # Display grid
        grid_response = AgGrid(
            visible_df,
            gridOptions=grid_options,
            use_container_width=True,
            height=1000,
            theme="material",
            update_mode=GridUpdateMode.SELECTION_CHANGED,
        )

        # Handle row selection for detail view
        selected_rows = grid_response.get('selected_rows', [])
        if isinstance(selected_rows, pd.DataFrame) and not selected_rows.empty:
            selected_row = selected_rows.iloc[0]

            if 'row_index' in selected_row:
                selected_index = selected_row['row_index']
                st.session_state['detail_index'] = selected_index
                st.rerun()
            else:
                st.warning("'row_index' not found in selected row")

        