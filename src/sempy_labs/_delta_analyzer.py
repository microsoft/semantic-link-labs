import pandas as pd
import re
import html as html_module
from datetime import datetime
import os
import uuid
from uuid import UUID
from typing import Dict, Optional
import pyarrow.parquet as pq
from sempy_labs._helper_functions import (
    create_abfss_path,
    save_as_delta_table,
    _get_column_aggregate,
    _create_dataframe,
    _update_dataframe_datatypes,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    _read_delta_table,
    _mount,
    _read_delta_table_history,
    resolve_workspace_id,
    resolve_lakehouse_id,
    _get_delta_table,
)
from sempy._utils._log import log
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.lakehouse._lakehouse import (
    lakehouse_attached,
)
from sempy_labs.lakehouse._helper import (
    is_v_ordered,
)
import sempy_labs._icons as icons
from sempy_labs._ui_components import (
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    DARK_THEME_VARS as _UI_DARK_VARS,
    scoped_header_css as _ui_scoped_header_css,
    scoped_attribution_css as _ui_scoped_attribution_css,
    render_header_html as _ui_render_header_html,
    render_attribution_html as _ui_render_attribution_html,
    theme_toggle_script as _ui_theme_toggle_script,
    fullscreen_css as _ui_fullscreen_css,
    fullscreen_toggle_script as _ui_fullscreen_toggle_script,
    display_html_widget as _ui_display_html_widget,
)
from tqdm.auto import tqdm


@log
def get_parquet_file_infos(path):

    import notebookutils

    files = []
    items = notebookutils.fs.ls(path)
    for item in items:
        if item.isDir:
            # Ignore the _delta_log directory
            if "_delta_log" not in item.path:
                files.extend(get_parquet_file_infos(item.path))
        else:
            # Filter out non-Parquet files and files with size 0
            if item.path.endswith(".parquet") and item.size > 0:
                files.append((item.path, item.size))
    return files


@log
def delta_analyzer(
    table_name: str,
    approx_distinct_count: bool = True,
    export: bool = False,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    column_stats: bool = True,
    skip_cardinality: bool = True,
    schema: Optional[str] = None,
    visualize: bool = True,
    dark_mode: bool = False,
) -> Dict[str, pd.DataFrame]:
    """
    Analyzes a delta table and shows the results in dictionary containing a set of 5 dataframes. If 'export' is set to True, the results will be saved to delta tables in the lakehouse attached to the notebook.

    The 5 dataframes returned by this function are:

    * Summary
    * Parquet Files
    * Row Groups
    * Column Chunks
    * Columns

    Read more about Delta Analyzer `here <https://github.com/microsoft/Analysis-Services/tree/master/DeltaAnalyzer>`_.

    Parameters
    ----------
    table_name : str
        The delta table name.
    approx_distinct_count: bool, default=True
        If True, uses approx_count_distinct to calculate the cardinality of each column. If False, uses COUNT(DISTINCT) instead.
    export : bool, default=False
        If True, exports the resulting dataframes to delta tables in the lakehouse attached to the notebook.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    column_stats : bool, default=True
        If True, collects data about column chunks and columns. If False, skips that step and only returns the other 3 dataframes.
    skip_cardinality : bool, default=True
        If True, skips the cardinality calculation for each column. If False, calculates the cardinality for each column.
    schema : str, default=None
        The name of the schema to which the table belongs (for schema-enabled lakehouses). If None, the default schema is used.
    visualize : bool, default=True
        If True, renders an HTML-styled interactive UI for viewing the Delta Analyzer results.
    dark_mode : bool, default=False
        If True, renders the visualization with a dark color palette. Has no effect when ``visualize`` is False.

    Returns
    -------
    Dict[str, pandas.DataFrame]
        A dictionary of pandas dataframes showing semantic model objects which violated the best practice analyzer rules.
    """

    # Must calculate column stats if calculating cardinality
    if not skip_cardinality:
        column_stats = True

    prefix = "SLL_DeltaAnalyzer_"
    now = datetime.now()
    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace=workspace)
    lakehouse_name, lakehouse_id = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace
    )

    delta_table_path = create_abfss_path(
        lakehouse_id, workspace_id, table_name, schema=schema
    )
    local_path = _mount(lakehouse=lakehouse, workspace=workspace)

    parquet_file_df_columns = {
        # "Dataset": "string",
        "Parquet File": "string",
        "Row Count": "int",
        "Row Groups": "int",
        "Created By": "string",
        "Total Table Rows": "int",
        "Total Table Row Groups": "int",
    }
    row_group_df_columns = {
        # "Dataset": "string",
        "Parquet File": "string",
        "Row Group ID": "int",
        "Row Count": "int",
        "Compressed Size": "int",
        "Uncompressed Size": "int",
        "Compression Ratio": "float",
        "Total Table Rows": "int",
        "Ratio Of Total Table Rows": "float",
        "Total Table Row Groups": "int",
    }
    column_chunk_df_columns = {
        # "Dataset": "string",
        "Parquet File": "string",
        "Column ID": "int",
        "Column Name": "string",
        "Column Type": "string",
        "Compressed Size": "int",
        "Uncompressed Size": "int",
        "Has Dict": "bool",
        "Dict Offset": "int_fillna",
        "Value Count": "int",
        "Encodings": "string",
        "Statistics": "string",
        "Primitive Type": "string",
    }

    parquet_file_df = _create_dataframe(columns=parquet_file_df_columns)
    row_group_df = _create_dataframe(columns=row_group_df_columns)
    column_chunk_df = _create_dataframe(columns=column_chunk_df_columns)

    row_groups = 0
    max_rows_per_row_group = 0
    min_rows_per_row_group = float("inf")

    is_vorder = is_v_ordered(
        table_name=table_name, lakehouse=lakehouse, workspace=workspace, schema=schema
    )

    # Get the common details of the Delta table
    delta_table = _get_delta_table(delta_table_path)
    table_df = delta_table.toDF()
    # total_partition_count = table_df.rdd.getNumPartitions()
    row_count = table_df.count()
    table_details = delta_table.detail().collect()[0].asDict()
    # created_at = table_details.get("createdAt")
    # last_modified = table_details.get("lastModified")
    # partition_columns = table_details.get("partitionColumns")
    # clustering_columns = table_details.get("clusteringColumns")
    num_latest_files = table_details.get("numFiles", 0)
    # size_in_bytes = table_details.get("sizeInBytes")
    # min_reader_version = table_details.get("minReaderVersion")
    # min_writer_version = table_details.get("minWriterVersion")

    latest_files = _read_delta_table(delta_table_path).inputFiles()
    # file_paths = [f.split("/")[-1] for f in latest_files]
    all_parquet_files = get_parquet_file_infos(delta_table_path)
    common_file_paths = set(
        [file_info[0] for file_info in all_parquet_files]
    ).intersection(set(latest_files))
    latest_version_files = [
        file_info
        for file_info in all_parquet_files
        if file_info[0] in common_file_paths
    ]

    for idx, (file_path, file_size) in enumerate(
        bar := tqdm(latest_version_files), start=1
    ):
        file_name = os.path.basename(file_path)
        bar.set_description(
            f"Analyzing the '{file_name}' parquet file ({idx}/{num_latest_files})..."
        )

        relative_path = file_path.split("Tables/")[1]
        file_system_path = f"{local_path}/Tables/{relative_path}"
        parquet_file = pq.ParquetFile(file_system_path)

        row_groups += parquet_file.num_row_groups

        # Generate rowgroup dataframe
        new_data = {
            # "Dataset": "Parquet Files",
            "Parquet File": file_name,
            "Row Count": parquet_file.metadata.num_rows,
            "Row Groups": parquet_file.num_row_groups,
            "Created By": parquet_file.metadata.created_by,
            "Total Table Rows": -1,
            "Total Table Row Groups": -1,
        }

        parquet_file_df = pd.concat(
            [parquet_file_df, pd.DataFrame(new_data, index=[0])], ignore_index=True
        )

        # Loop through the row groups
        for i in range(parquet_file.num_row_groups):
            row_group = parquet_file.metadata.row_group(i)
            num_rows = row_group.num_rows

            max_rows_per_row_group = max(max_rows_per_row_group, num_rows)
            min_rows_per_row_group = min(min_rows_per_row_group, num_rows)

            total_compressed_size = 0
            total_uncompressed_size = 0

            # Loop through the columns
            if column_stats:
                for j in range(row_group.num_columns):
                    column_chunk = row_group.column(j)
                    total_compressed_size += column_chunk.total_compressed_size
                    total_uncompressed_size += column_chunk.total_uncompressed_size

                    # Generate Column Chunk Dataframe
                    new_data = {
                        # "Dataset": "Column Chunks",
                        "Parquet File": file_name,
                        "Column ID": j,
                        "Column Name": column_chunk.path_in_schema,
                        "Column Type": column_chunk.physical_type,
                        "Compressed Size": column_chunk.total_compressed_size,
                        "Uncompressed Size": column_chunk.total_uncompressed_size,
                        "Has Dict": column_chunk.has_dictionary_page,
                        "Dict Offset": column_chunk.dictionary_page_offset,
                        "Value Count": column_chunk.num_values,
                        "Encodings": str(column_chunk.encodings),
                        "Statistics": column_chunk.statistics,
                        "Primitive Type": column_chunk.physical_type,
                    }

                    column_chunk_df = pd.concat(
                        [column_chunk_df, pd.DataFrame(new_data, index=[0])],
                        ignore_index=True,
                    )

            # Generate rowgroup dataframe
            new_data = {
                # "Dataset": "Row Groups",
                "Parquet File": file_name,
                "Row Group ID": i + 1,
                "Row Count": num_rows,
                "Compressed Size": total_compressed_size,
                "Uncompressed Size": total_uncompressed_size,
                "Compression Ratio": (
                    total_compressed_size / total_uncompressed_size
                    if column_stats
                    else 0
                ),
                "Total Table Rows": -1,
                "Total Table Row Groups": -1,
            }

            if not row_group_df.empty:
                row_group_df = pd.concat(
                    [row_group_df, pd.DataFrame(new_data, index=[0])], ignore_index=True
                )
            else:
                row_group_df = pd.DataFrame(new_data, index=[0])

    avg_rows_per_row_group = row_count / row_groups

    # Generate summary dataframe
    summary_df = pd.DataFrame(
        [
            {
                # "Dataset": "Summary",
                "Row Count": row_count,
                "Row Groups": row_groups,
                "Parquet Files": num_latest_files,
                "Max Rows Per Row Group": max_rows_per_row_group,
                "Min Rows Per Row Group": min_rows_per_row_group,
                "Avg Rows Per Row Group": avg_rows_per_row_group,
                "VOrder Enabled": is_vorder,
                # "VOrderLevel": v_order_level,
            }
        ]
    )

    # Clean up data types
    _update_dataframe_datatypes(dataframe=row_group_df, column_map=row_group_df_columns)
    _update_dataframe_datatypes(
        dataframe=parquet_file_df, column_map=parquet_file_df_columns
    )

    # Generate column dataframe
    if column_stats:
        _update_dataframe_datatypes(
            dataframe=column_chunk_df, column_map=column_chunk_df_columns
        )
        column_df = column_chunk_df.groupby(
            ["Column Name", "Column Type"], as_index=False
        ).agg({"Compressed Size": "sum", "Uncompressed Size": "sum"})

        # Add distinct count to column_df
        if not skip_cardinality:
            for ind, r in column_df.iterrows():
                col_name = r["Column Name"]
                if approx_distinct_count:
                    function = "approx"
                else:
                    function = "distinctcount"
                dc = _get_column_aggregate(
                    table_name=table_name,
                    column_name=col_name,
                    function=function,
                    lakehouse=lakehouse,
                    workspace=workspace,
                )

                if "Cardinality" not in column_df.columns:
                    column_df["Cardinality"] = None

                column_df.at[ind, "Cardinality"] = dc

        summary_df["Total Size"] = column_df["Compressed Size"].sum()

    parquet_file_df["Total Table Rows"] = parquet_file_df["Row Count"].sum()
    parquet_file_df["Total Table Row Groups"] = parquet_file_df["Row Groups"].sum()

    row_group_df["Total Table Rows"] = parquet_file_df["Row Count"].sum()
    row_group_df["Total Table Row Groups"] = parquet_file_df["Row Groups"].sum()
    total_rows = row_group_df["Row Count"].sum()
    row_group_df["Ratio Of Total Table Rows"] = (
        row_group_df["Row Count"] / total_rows * 100.0
    )

    if column_stats:
        column_df["Total Table Rows"] = parquet_file_df["Row Count"].sum()
        column_df["Table Size"] = column_df["Compressed Size"].sum()
        column_df["Size Percent Of Table"] = (
            column_df["Compressed Size"] / column_df["Table Size"] * 100.0
        )
    if not skip_cardinality and column_stats:
        column_df["Cardinality"] = column_df["Cardinality"].fillna(0).astype(int)
        column_df["Cardinality Of Total Rows"] = (
            column_df["Cardinality"] / column_df["Total Table Rows"] * 100.0
        )

    dataframes = {
        "Summary": summary_df,
        "Parquet Files": parquet_file_df,
        "Row Groups": row_group_df,
    }

    if column_stats:
        dataframes["Column Chunks"] = column_chunk_df
        dataframes["Columns"] = column_df

    save_table = f"{prefix}Summary"

    if export:
        if not lakehouse_attached():
            raise ValueError(
                f"{icons.red_dot} No lakehouse is attached to this notebook. Please attach a lakehouse to the notebook before running the Delta Analyzer."
            )
        dfL = get_lakehouse_tables()
        dfL_filt = dfL[dfL["Table Name"] == save_table]
        if dfL_filt.empty:
            runId = 1
        else:
            max_run_id = _get_column_aggregate(
                table_name=save_table,
            )
            runId = max_run_id + 1

    for name, df in dataframes.items():
        name = name.replace(" ", "")
        cols = {
            "Workspace Name": workspace_name,
            "Workspace Id": workspace_id,
            "Lakehouse Name": lakehouse_name,
            "Lakehouse Id": lakehouse_id,
            "Table Name": table_name,
        }
        for i, (col, param) in enumerate(cols.items()):
            df[col] = param
            df.insert(i, col, df.pop(col))

        df["Timestamp"] = now
        df["Timestamp"] = pd.to_datetime(df["Timestamp"])

        if export:
            df["Run Id"] = runId
            df["Run Id"] = df["Run Id"].astype(int)

            df.columns = df.columns.str.replace(" ", "")
            save_as_delta_table(
                dataframe=df,
                delta_table_name=f"{prefix}{name}",
                write_mode="append",
                merge_schema=True,
            )

    if visualize:
        _display_delta_analyzer_ui(
            dataframes=dataframes,
            table_name=table_name,
            schema=schema,
            dark_mode=dark_mode,
        )

    return dataframes


def _display_delta_analyzer_ui(
    dataframes: Dict[str, pd.DataFrame],
    table_name: str,
    schema: Optional[str] = None,
    dark_mode: bool = False,
) -> None:
    """Renders an interactive HTML dashboard for delta analyzer results."""

    uid = uuid.uuid4().hex[:8]
    root_selector = f".da-{uid}-root"
    theme_btn_id = f"da-theme-{uid}"
    fullscreen_btn_id = f"da-fullscreen-{uid}"
    fullscreen_class = "da-fullscreen"

    _skip_cols = {
        "Workspace Name",
        "Workspace Id",
        "Lakehouse Name",
        "Lakehouse Id",
        "Table Name",
        "Timestamp",
        "Run Id",
        "Statistics",
    }

    _tab_skip_cols = {
        "Parquet Files": {"Total Table Rows", "Total Table Row Groups"},
        "Row Groups": {"Total Table Rows", "Total Table Row Groups"},
        "Column Chunks": {"Column ID"},
        "Columns": {"Total Table Rows", "Table Size"},
    }

    def _fmt_int(v) -> str:
        try:
            return f"{int(v):,}"
        except Exception:
            return html_module.escape(str(v))

    def _fmt_float(v) -> str:
        try:
            return f"{float(v):,.2f}"
        except Exception:
            return html_module.escape(str(v))

    def _fmt_pct(v) -> str:
        try:
            return f"{float(v):.2f}%"
        except Exception:
            return html_module.escape(str(v))

    def _fmt_bytes(v) -> str:
        try:
            b = int(v)
            for unit in ["", "KB", "MB", "GB", "TB"]:
                if abs(b) < 1024:
                    return f"{b:,.1f} {unit}" if unit else f"{b:,} B"
                b /= 1024
            return f"{b:,.1f} PB"
        except Exception:
            return html_module.escape(str(v))

    def _fmt_val(col: str, v) -> str:
        if pd.isna(v):
            return "&mdash;"
        if isinstance(v, bool):
            return "True" if v else "False"
        s = str(v)
        col_lower = col.lower()
        if (
            "ratio of total" in col_lower
            or "percent" in col_lower
            or "% " in col_lower
            or "size percent" in col_lower
        ):
            return _fmt_pct(v)
        if col_lower == "compression ratio":
            try:
                return f"{float(v) * 100:.2f}%"
            except Exception:
                return html_module.escape(str(v))
        if "ratio" in col_lower:
            return _fmt_float(v)
        if col_lower in ("compressed size", "uncompressed size"):
            return _fmt_int(v)
        if (
            "size" in col_lower
            or col_lower == "total size"
            or col_lower == "table size"
        ):
            return _fmt_bytes(v)
        if isinstance(v, float):
            return _fmt_float(v)
        if isinstance(v, (int,)):
            return _fmt_int(v)
        return html_module.escape(s)

    def _is_text_col(df: pd.DataFrame, col: str) -> bool:
        dtype = df[col].dtype
        if dtype == object or dtype.name == "string" or dtype.name == "bool":
            return True
        return False

    # Extract metadata for the header
    summary_df = dataframes.get("Summary")
    meta_workspace = ""
    meta_lakehouse = ""
    for _df in dataframes.values():
        if "Workspace Name" in _df.columns and not _df.empty:
            meta_workspace = str(_df["Workspace Name"].iloc[0])
            meta_lakehouse = str(_df["Lakehouse Name"].iloc[0])
            break

    # Build summary cards HTML
    cards_html = ""
    if summary_df is not None and not summary_df.empty:
        row = summary_df.iloc[0]
        card_items = [
            ("Row Count", _fmt_int(row.get("Row Count", 0))),
            ("Parquet Files", _fmt_int(row.get("Parquet Files", 0))),
            ("Row Groups", _fmt_int(row.get("Row Groups", 0))),
            ("Avg Rows / RG", _fmt_int(row.get("Avg Rows Per Row Group", 0))),
            ("VOrder", "Yes" if row.get("VOrder Enabled") else "No"),
        ]
        if "Total Size" in row.index:
            card_items.insert(1, ("Total Size", _fmt_bytes(row["Total Size"])))
        for label, value in card_items:
            cards_html += f"""
            <div class="da-{uid}-card">
                <div class="da-{uid}-card-label">{html_module.escape(label)}</div>
                <div class="da-{uid}-card-value">{value}</div>
            </div>"""

    # Build table HTML for each dataframe tab
    tab_keys = [
        k
        for k in ["Parquet Files", "Row Groups", "Column Chunks", "Columns"]
        if k in dataframes
    ]
    _default_sort = {
        "Parquet Files": "Row Count",
        "Row Groups": "Compressed Size",
        "Column Chunks": "Compressed Size",
        "Columns": "Compressed Size",
    }
    tabs_html = ""
    panels_html = ""
    tab_row_counts = {}

    def _tab_icon(svg: str) -> str:
        return svg.replace("<svg ", f'<svg class="da-{uid}-tab-icon" ', 1)

    tab_icons = {
        "Parquet Files": _tab_icon(_UI_ICONS["partition"]),
        "Row Groups": _tab_icon(_UI_ICONS["table"]),
        "Column Chunks": _tab_icon(_UI_ICONS["column_chunk"]),
        "Columns": _tab_icon(_UI_ICONS["column"]),
    }
    for i, key in enumerate(tab_keys):
        active_cls = " da-{uid}-tab-active".format(uid=uid) if i == 0 else ""
        safe_key = html_module.escape(key)
        icon = tab_icons.get(key, "")
        tabs_html += f'<button class="da-{uid}-tab{active_cls}" data-da-tab-{uid}="{i}">{icon}{safe_key}</button>'

        df = dataframes[key]
        skip = _skip_cols | _tab_skip_cols.get(key, set())
        visible_cols = [c for c in df.columns if c not in skip]

        # Apply default descending sort
        sort_col = _default_sort.get(key)
        if sort_col and sort_col in df.columns:
            df = df.sort_values(by=sort_col, ascending=False)

        display_style = "block" if i == 0 else "none"
        # Header with resize handles; initial width based on header text
        header_cells = ""
        for c in visible_cols:
            label = html_module.escape(str(c))
            # ~7.5px per char at 11px uppercase + 32px padding + 16px resize handle
            col_w = max(int(len(str(c)) * 7.5) + 48, 80)
            arrow = (
                ' <span class="da-{uid}-sort-arrow">\u25bc</span>'.format(uid=uid)
                if c == sort_col
                else ""
            )
            align = "left" if _is_text_col(df, c) else "right"
            header_cells += f'<th style="width:{col_w}px;min-width:60px;text-align:{align}"><span class="da-{uid}-th-text">{label}{arrow}</span><div class="da-{uid}-resize"></div></th>'
        # Compute column max values for data bars
        col_maxes = {}
        for j, c in enumerate(visible_cols):
            if not _is_text_col(df, c):
                try:
                    max_val = df[c].abs().max()
                    if max_val > 0:
                        col_maxes[j] = float(max_val)
                except Exception:
                    pass

        # Body
        col_aligns = ["left" if _is_text_col(df, c) else "right" for c in visible_cols]
        tab_row_counts[i] = len(df)
        body_rows = ""
        for _, r in df.iterrows():
            cells = ""
            for j, c in enumerate(visible_cols):
                val = r[c]
                fmt_val = _fmt_val(str(c), val)
                if j in col_maxes:
                    try:
                        raw = abs(float(val)) if not pd.isna(val) else 0
                        pct = raw / col_maxes[j] * 100
                    except Exception:
                        pct = 0
                    cells += (
                        f'<td class="da-{uid}-bar-cell" style="text-align:{col_aligns[j]}">'
                        f'<div class="da-{uid}-bar" style="width:{pct:.1f}%"></div>'
                        f'<span class="da-{uid}-bar-value">{fmt_val}</span></td>'
                    )
                else:
                    cells += f'<td style="text-align:{col_aligns[j]}">{fmt_val}</td>'
            body_rows += f"<tr>{cells}</tr>"

        panels_html += f"""
        <div class="da-{uid}-panel" data-da-panel-{uid}="{i}" style="display:{display_style}">
            <div class="da-{uid}-table-wrap">
                <table class="da-{uid}-table">
                    <thead><tr>{header_cells}</tr></thead>
                    <tbody>{body_rows}</tbody>
                </table>
            </div>
        </div>"""

    # ── Shared header (title + table · workspace · lakehouse subtitle + theme btn) ──
    subtitle_workspace = ""
    if meta_workspace and meta_lakehouse:
        subtitle_workspace = f"{meta_workspace} · {meta_lakehouse}"
    elif meta_workspace:
        subtitle_workspace = meta_workspace
    elif meta_lakehouse:
        subtitle_workspace = meta_lakehouse

    header_table_name = f"{schema}.{table_name}" if schema else table_name
    header_html = _ui_render_header_html(
        title="Delta Analyzer",
        dataset_name=header_table_name,
        workspace_name=subtitle_workspace or None,
        theme_btn_id=theme_btn_id,
        dark_mode=dark_mode,
        fullscreen_btn_id=fullscreen_btn_id,
    )
    ui_header_css_scoped = _ui_scoped_header_css(root_selector)
    ui_attribution_css_scoped = _ui_scoped_attribution_css(root_selector)
    ui_fullscreen_css = _ui_fullscreen_css(
        root_selector,
        fullscreen_class,
        container_selector=f".da-{uid}-container",
        bg_var="var(--da-bg)",
    )
    attribution_html = _ui_render_attribution_html()

    full_html = f"""
    <style>
        {ui_header_css_scoped}
        {ui_fullscreen_css}
        .da-{uid}-root {{
            {_UI_LIGHT_VARS}
            --da-accent: var(--ui-accent);
            --da-accent-hover: var(--ui-accent-hover);
            --da-accent-soft: var(--ui-accent-soft);
            --da-bg: var(--ui-bg);
            --da-bg-secondary: var(--ui-bg-secondary);
            --da-bg-tertiary: var(--ui-bg-tertiary);
            --da-border: var(--ui-border);
            --da-border-strong: var(--ui-border-strong);
            --da-text: var(--ui-text);
            --da-text-secondary: var(--ui-text-secondary);
            --da-text-tertiary: var(--ui-text-tertiary);
            --da-shadow-sm: var(--ui-shadow-sm);
            --da-shadow-md: var(--ui-shadow-md);
            --da-shadow-lg: var(--ui-shadow-lg);
            --da-radius: 12px;
            --da-radius-sm: 8px;
            --da-transition: 0.25s cubic-bezier(0.4, 0, 0.2, 1);
            font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text',
                         'Helvetica Neue', Arial, sans-serif;
            color: var(--da-text);
            max-width: 1200px;
            margin: 24px auto;
            -webkit-font-smoothing: antialiased;
            -moz-osx-font-smoothing: grayscale;
        }}
        .da-{uid}-root.da-dark {{
            {_UI_DARK_VARS}
        }}
        .da-{uid}-root *, .da-{uid}-root *::before, .da-{uid}-root *::after {{
            box-sizing: border-box;
        }}
        .da-{uid}-container {{
            background: var(--da-bg);
            border-radius: var(--da-radius);
            box-shadow: var(--da-shadow-lg);
            overflow: hidden;
            border: 1px solid var(--da-border);
        }}
        .da-{uid}-header {{
            padding: 22px 24px 18px 24px;
            background: var(--da-bg);
        }}
        /* Summary cards */
        .da-{uid}-cards {{
            display: flex;
            gap: 12px;
            padding: 0 24px 16px 24px;
            flex-wrap: wrap;
        }}
        .da-{uid}-card {{
            flex: 1 1 110px;
            min-width: 110px;
            background: var(--da-bg-secondary);
            border: 1px solid var(--da-border);
            border-radius: var(--da-radius);
            padding: 14px 16px;
            transition: box-shadow var(--da-transition), transform var(--da-transition);
        }}
        .da-{uid}-card:hover {{
            box-shadow: var(--da-shadow-md);
            transform: translateY(-2px);
        }}
        .da-{uid}-card-label {{
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: var(--da-text-tertiary);
            margin-bottom: 4px;
        }}
        .da-{uid}-card-value {{
            font-size: 20px;
            font-weight: 600;
            letter-spacing: -0.3px;
            color: var(--da-text);
        }}
        /* Tabs */
        .da-{uid}-tabs {{
            display: flex;
            gap: 2px;
            padding: 0 24px;
            border-bottom: 1px solid var(--da-border);
            margin-bottom: 0;
            overflow-x: auto;
            scrollbar-width: none;
            -ms-overflow-style: none;
            background: var(--da-bg);
        }}
        .da-{uid}-tabs::-webkit-scrollbar {{
            display: none;
        }}
        .da-{uid}-tab {{
            background: none;
            border: none;
            padding: 10px 20px;
            font-size: 14px;
            font-weight: 500;
            color: var(--da-text-secondary);
            cursor: pointer;
            border-bottom: 2px solid transparent;
            transition: color var(--da-transition), border-color var(--da-transition);
            font-family: inherit;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        .da-{uid}-tab-icon {{
            width: 14px;
            height: 14px;
            flex-shrink: 0;
        }}
        .da-{uid}-tab:hover {{
            color: var(--da-text);
        }}
        .da-{uid}-tab-active {{
            color: var(--da-accent);
            font-weight: 600;
            border-bottom-color: var(--da-accent);
        }}
        /* Data table */
        .da-{uid}-table-wrap {{
            overflow-x: auto;
            overflow-y: auto;
            max-height: 520px;
            background: var(--da-bg);
        }}
        .da-{uid}-table {{
            table-layout: fixed;
            border-collapse: collapse;
            font-size: 13px;
            color: var(--da-text);
        }}
        .da-{uid}-table thead th {{
            position: sticky;
            top: 0;
            z-index: 1;
            background: var(--da-bg-secondary);
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.4px;
            color: var(--da-text-secondary);
            padding: 12px 16px;
            text-align: left;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            border-bottom: 1px solid var(--da-border);
            cursor: pointer;
            user-select: none;
        }}
        .da-{uid}-table thead th:hover {{
            color: var(--da-text);
            background: var(--da-accent-soft);
        }}
        .da-{uid}-th-text {{
            pointer-events: none;
        }}
        .da-{uid}-resize {{
            position: absolute;
            right: 0;
            top: 0;
            bottom: 0;
            width: 5px;
            cursor: col-resize;
            background: transparent;
            z-index: 2;
        }}
        .da-{uid}-resize:hover,
        .da-{uid}-resize.da-{uid}-resizing {{
            background: var(--da-accent);
            opacity: 0.4;
        }}
        .da-{uid}-table tbody tr {{
            background: var(--da-bg);
            transition: background var(--da-transition);
        }}
        .da-{uid}-table tbody tr td {{
            padding: 10px 16px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            border-bottom: 1px solid var(--da-border);
            color: var(--da-text);
            background: var(--da-bg);
        }}
        .da-{uid}-table tbody tr:nth-child(even) td {{
            background: var(--da-bg-tertiary);
        }}
        .da-{uid}-table tbody tr:last-child td {{
            border-bottom: none;
        }}
        .da-{uid}-table tbody tr:hover td {{
            background: var(--da-accent-soft);
            color: var(--da-text);
        }}
        /* Search */
        .da-{uid}-toolbar {{
            display: flex;
            align-items: center;
            justify-content: space-between;
            padding: 12px 24px;
            background: var(--da-bg-secondary);
            border-bottom: 1px solid var(--da-border);
        }}
        .da-{uid}-search {{
            font-family: inherit;
            font-size: 13px;
            padding: 6px 12px;
            border: 1px solid var(--da-border-strong);
            border-radius: var(--da-radius-sm);
            outline: none;
            width: 220px;
            background: var(--da-bg);
            color: var(--da-text);
            transition: border-color var(--da-transition), box-shadow var(--da-transition);
        }}
        .da-{uid}-search:focus {{
            border-color: var(--da-accent);
            box-shadow: 0 0 0 3px var(--da-accent-soft);
        }}
        .da-{uid}-search::placeholder {{
            color: var(--da-text-tertiary);
        }}
        /* Sort indicator */
        .da-{uid}-sort-arrow {{
            font-size: 10px;
            margin-left: 4px;
            opacity: 0.5;
        }}
        /* Data bars */
        .da-{uid}-table tbody td.da-{uid}-bar-cell {{
            position: relative;
            overflow: hidden;
        }}
        .da-{uid}-table tbody td.da-{uid}-bar-cell .da-{uid}-bar {{
            position: absolute;
            left: 0;
            top: 0;
            bottom: 0;
            background: var(--da-accent-soft);
            border-right: 2px solid var(--da-accent);
            opacity: 0.7;
            pointer-events: none;
        }}
        .da-{uid}-table tbody td.da-{uid}-bar-cell .da-{uid}-bar-value {{
            position: relative;
            z-index: 1;
        }}
        .da-{uid}-bars-off .da-{uid}-bar {{
            display: none;
        }}
        /* Data bar toggle */
        .da-{uid}-bar-toggle {{
            display: inline-flex;
            align-items: center;
            gap: 5px;
            padding: 4px 10px;
            font-size: 11px;
            font-weight: 500;
            font-family: inherit;
            color: var(--da-text-secondary);
            background: var(--da-bg);
            border: 1px solid var(--da-border-strong);
            border-radius: 6px;
            cursor: pointer;
            transition: color var(--da-transition), border-color var(--da-transition);
            white-space: nowrap;
            margin-left: 12px;
        }}
        .da-{uid}-bar-toggle:hover {{
            color: var(--da-text);
            border-color: var(--da-text-tertiary);
        }}
        .da-{uid}-bar-toggle.da-{uid}-bars-active {{
            color: var(--da-accent);
            border-color: var(--da-accent);
        }}
        .da-{uid}-bar-toggle .da-{uid}-toggle-icon {{
            width: 12px;
            height: 12px;
            flex-shrink: 0;
        }}
        /* Toolbar controls */
        .da-{uid}-toolbar-controls {{
            display: flex;
            align-items: center;
            gap: 12px;
        }}
        .da-{uid}-row-count {{
            font-size: 12px;
            font-weight: 500;
            color: var(--da-text-tertiary);
            letter-spacing: -0.01em;
        }}
        .da-{uid}-row-count span {{
            font-variant-numeric: tabular-nums;
        }}
        {ui_attribution_css_scoped}
    </style>

    <div class="da-{uid}-root{' da-dark' if dark_mode else ''}">
    <div class="da-{uid}-container">
        <div class="da-{uid}-header">{header_html}</div>
        <div class="da-{uid}-cards">
            {cards_html}
        </div>
        <div class="da-{uid}-tabs">
            {tabs_html}
        </div>
        <div class="da-{uid}-toolbar">
            <input type="text" class="da-{uid}-search" id="da-{uid}-search" placeholder="Search...">
            <div class="da-{uid}-toolbar-controls">
                <button class="da-{uid}-bar-toggle da-{uid}-bars-active" id="da-{uid}-bar-toggle" title="Toggle data bars"><svg class="da-{uid}-toggle-icon" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"><line x1="3" y1="12" x2="3" y2="6"/><line x1="7" y1="12" x2="7" y2="3"/><line x1="11" y1="12" x2="11" y2="8"/><line x1="1" y1="12" x2="13" y2="12"/></svg>Bars</button>
                <div class="da-{uid}-row-count" id="da-{uid}-row-count"><span>{tab_row_counts.get(0, 0):,}</span> row{'s' if tab_row_counts.get(0, 0) != 1 else ''}</div>
            </div>
        </div>
        <div class="da-{uid}-panels">
            {panels_html}
        </div>
    </div>
    {attribution_html}
    </div>

    <script>
    (function() {{
        var uid = '{uid}';
        var tabRowCounts = {tab_row_counts};
        // Tab switching
        var tabs = document.querySelectorAll('.da-' + uid + '-tab');
        var panels = document.querySelectorAll('[data-da-panel-' + uid + ']');
        tabs.forEach(function(tab) {{
            tab.addEventListener('click', function() {{
                var idx = this.getAttribute('data-da-tab-' + uid);
                tabs.forEach(function(t) {{ t.classList.remove('da-' + uid + '-tab-active'); }});
                this.classList.add('da-' + uid + '-tab-active');
                panels.forEach(function(p) {{
                    p.style.display = p.getAttribute('data-da-panel-' + uid) === idx ? 'block' : 'none';
                }});
                // Clear search on tab switch
                var si = document.getElementById('da-' + uid + '-search');
                if (si) {{ si.value = ''; filterRows(''); }}
                // Update row count
                var rc = document.getElementById('da-' + uid + '-row-count');
                var cnt = tabRowCounts[parseInt(idx)] || 0;
                rc.innerHTML = '<span>' + cnt.toLocaleString() + '</span> row' + (cnt !== 1 ? 's' : '');
            }});
        }});

        // Search / filter
        function filterRows(query) {{
            panels.forEach(function(p) {{
                if (p.style.display === 'none') return;
                var rows = p.querySelectorAll('tbody tr');
                var shown = 0;
                var total = rows.length;
                rows.forEach(function(row) {{
                    var text = row.textContent.toLowerCase();
                    var match = !query || text.indexOf(query) !== -1;
                    row.style.display = match ? '' : 'none';
                    if (match) shown++;
                }});
                var rc = document.getElementById('da-' + uid + '-row-count');
                if (rc) {{
                    rc.innerHTML = '<span>' + shown.toLocaleString() + '</span>' +
                        (shown !== total ? ' of <span>' + total.toLocaleString() + '</span>' : '') +
                        ' row' + (shown !== 1 ? 's' : '');
                }}
            }});
        }}
        var searchInput = document.getElementById('da-' + uid + '-search');
        if (searchInput) {{
            searchInput.addEventListener('input', function() {{
                filterRows(this.value.toLowerCase());
            }});
        }}

        // Column resizing
        document.querySelectorAll('.da-' + uid + '-resize').forEach(function(handle) {{
            handle.addEventListener('mousedown', function(e) {{
                e.preventDefault();
                e.stopPropagation();
                var th = this.parentElement;
                var startX = e.pageX;
                var startW = th.offsetWidth;
                handle.classList.add('da-' + uid + '-resizing');
                function onMove(ev) {{
                    var newW = Math.max(60, startW + ev.pageX - startX);
                    var diff = newW - th.offsetWidth;
                    th.style.width = newW + 'px';
                    var tbl = th.closest('table');
                    tbl.style.width = (tbl.offsetWidth + diff) + 'px';
                }}
                function onUp() {{
                    handle.classList.remove('da-' + uid + '-resizing');
                    document.removeEventListener('mousemove', onMove);
                    document.removeEventListener('mouseup', onUp);
                }}
                document.addEventListener('mousemove', onMove);
                document.addEventListener('mouseup', onUp);
            }});
        }});

        // Column sorting
        var sortState = {{}};
        document.querySelectorAll('.da-' + uid + '-table thead th').forEach(function(th) {{
            th.addEventListener('click', function() {{
                var table = this.closest('table');
                var colIdx = Array.from(this.parentNode.children).indexOf(this);
                var tbody = table.querySelector('tbody');
                var rows = Array.from(tbody.querySelectorAll('tr'));
                var key = table.id + '_' + colIdx;
                var asc = sortState[key] !== true;
                sortState[key] = asc;

                rows.sort(function(a, b) {{
                    var aVal = a.children[colIdx] ? a.children[colIdx].textContent.replace(/[,%]/g, '').trim() : '';
                    var bVal = b.children[colIdx] ? b.children[colIdx].textContent.replace(/[,%]/g, '').trim() : '';
                    var aNum = parseFloat(aVal);
                    var bNum = parseFloat(bVal);
                    if (!isNaN(aNum) && !isNaN(bNum)) {{
                        return asc ? aNum - bNum : bNum - aNum;
                    }}
                    return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                }});
                rows.forEach(function(row) {{ tbody.appendChild(row); }});

                // Update arrow indicators
                table.querySelectorAll('.da-' + uid + '-sort-arrow').forEach(function(el) {{ el.remove(); }});
                var arrow = document.createElement('span');
                arrow.className = 'da-' + uid + '-sort-arrow';
                arrow.textContent = asc ? ' \u25b2' : ' \u25bc';
                th.appendChild(arrow);
            }});
        }});

        // Data bar toggle (synced across all tabs)
        var barBtn = document.getElementById('da-' + uid + '-bar-toggle');
        if (barBtn) {{
            barBtn.addEventListener('click', function() {{
                var root = this.closest('.da-' + uid + '-root');
                if (!root) return;
                var wraps = root.querySelectorAll('.da-' + uid + '-table-wrap');
                var turnOff = !wraps[0].classList.contains('da-' + uid + '-bars-off');
                wraps.forEach(function(w) {{
                    if (turnOff) {{ w.classList.add('da-' + uid + '-bars-off'); }}
                    else {{ w.classList.remove('da-' + uid + '-bars-off'); }}
                }});
                if (turnOff) {{ this.classList.remove('da-' + uid + '-bars-active'); }}
                else {{ this.classList.add('da-' + uid + '-bars-active'); }}
            }});
        }}
    }})();
    </script>
    """

    theme_script = _ui_theme_toggle_script(
        btn_id=theme_btn_id,
        root_selector=root_selector,
        dark_class="da-dark",
    )

    fullscreen_script = _ui_fullscreen_toggle_script(
        btn_id=fullscreen_btn_id,
        root_selector=root_selector,
        fullscreen_class=fullscreen_class,
    )

    # Render through a lightweight anywidget so the output lives in the
    # notebook webview's light DOM (not the nested sandbox iframe used for raw
    # HTML output). This lets the full-screen toggle use the native Fullscreen
    # API and expand edge-to-edge, matching ``test()`` and the other tools.
    # Falls back to plain HTML output if anywidget isn't installed.
    _ui_display_html_widget(full_html + theme_script + fullscreen_script)


@log
def get_delta_table_history(
    table_name: str,
    lakehouse: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    schema: Optional[str] = None,
) -> pd.DataFrame:
    """
    Returns the history of a delta table as a pandas dataframe.

    Parameters
    ----------
    table_name : str
        The delta table name.
    lakehouse : str | uuid.UUID, default=None
        The Fabric lakehouse name or ID.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID used by the lakehouse.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    schema : str, default=None
        The name of the schema to which the table belongs (for schema-enabled lakehouses). If None, the default schema is used.

    Returns
    -------
    pandas.DataFrame
        A dataframe showing the history of the delta table.
    """

    def camel_to_title(text):
        return re.sub(r"([a-z])([A-Z])", r"\1 \2", text).title()

    workspace_id = resolve_workspace_id(workspace=workspace)
    lakehouse_id = resolve_lakehouse_id(lakehouse=lakehouse, workspace=workspace_id)
    path = create_abfss_path(lakehouse_id, workspace_id, table_name, schema)
    df = _read_delta_table_history(path=path)
    df.rename(columns=lambda col: camel_to_title(col), inplace=True)

    return df
