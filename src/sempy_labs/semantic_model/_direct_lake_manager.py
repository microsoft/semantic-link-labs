import sempy
import sempy.fabric as fabric
import json
import uuid
import html as _html
from typing import Optional
from uuid import UUID
import ipywidgets as widgets
from IPython.display import display
from sempy_labs.tom import connect_semantic_model
from sempy_labs.directlake._sources import (
    get_direct_lake_sources,
)
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_item_name_and_id,
)
from sempy_labs.directlake._generate_shared_expression import (
    generate_shared_expression,
)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------


def _collect_sources(dataset, workspace):
    """Retrieve Direct Lake source metadata including expression names."""
    return get_direct_lake_sources(dataset=dataset, workspace=workspace)


def _collect_tables(dataset, workspace):
    """Retrieve Direct Lake table / partition metadata."""
    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    table_list = []
    with connect_semantic_model(dataset=dataset, workspace=workspace) as tom:
        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The provided dataset is not a Direct Lake dataset."
            )
        for t in tom.model.Tables:
            table_name = t.Name
            for p in t.Partitions:
                if p.Mode == TOM.ModeType.DirectLake:
                    table_list.append(
                        {
                            "tableName": table_name,
                            "partitionName": p.Name,
                            "expressionName": p.Source.ExpressionSource.Name,
                            "entityName": p.Source.EntityName,
                            "schemaName": p.Source.SchemaName,
                            "mode": str(p.Mode),
                        }
                    )
    return table_list


# ---------------------------------------------------------------------------
# Apply changes callback
# ---------------------------------------------------------------------------

_dlm_callbacks = {}


def _apply_changes(uid, state_json):
    """Apply Direct Lake source and table changes to the semantic model."""
    info = _dlm_callbacks.get(uid)
    if not info:
        print(f"{icons.red_dot} No callback registered " f"for session {uid}.")
        return

    dataset_id = info["dataset_id"]
    workspace_id = info["workspace_id"]
    state = json.loads(state_json)
    new_sources = state.get("sources", [])
    new_tables = state.get("tables", [])

    with connect_semantic_model(
        dataset=dataset_id,
        workspace=workspace_id,
        readonly=False,
    ) as tom:
        existing_expr_names = {e.Name for e in tom.model.Expressions}
        new_expr_names = {
            src.get("expressionName", "")
            for src in new_sources
            if src.get("expressionName")
        }

        # Remove expressions that were deleted by the user
        removed_expr_names = existing_expr_names - new_expr_names
        for expr_name in removed_expr_names:
            expr_obj = tom.model.Expressions[expr_name]
            tom.remove_object(expr_obj)

        # Apply source (expression) changes
        for src in new_sources:
            expr_name = src.get("expressionName", "")
            item_name = src.get('itemName')
            item_type = src.get('itemType')
            item_workspace = src.get('workspaceName')
            item_use_sql_endpoint = src.get('usesSqlEndpoint')
            if not expr_name:
                continue

            expression = generate_shared_expression(
                item_name=item_name,
                item_type=item_type,
                workspace=item_workspace,
                use_sql_endpoint=item_use_sql_endpoint,
            )

            if expr_name in existing_expr_names:
                tom.model.Expressions[expr_name].Expression = expression
                # Lakehouse SQL Endpoint must use DatabaseQuery as the expression name
                #if item_type == 'Lakehouse' and item_use_sql_endpoint:
                #    tom.model.Expressions[expr_name].Name = 'DatabaseQuery'
            else:
                tom.add_expression(
                    name=expr_name,
                    expression=expression,
                )
        # Apply table partition changes
        for tbl in new_tables:
            table_name = tbl["tableName"]
            partition_name = tbl["partitionName"]
            for p in tom.model.Tables[table_name].Partitions:
                if p.Name == partition_name:
                    p.Source.ExpressionSource = tom.model.Expressions[
                        tbl["expressionName"]
                    ]
                    p.Source.EntityName = tbl["entityName"]
                    p.Source.SchemaName = tbl["schemaName"]


# ---------------------------------------------------------------------------
# HTML escape helper
# ---------------------------------------------------------------------------


def _esc(text):
    """Escape text for safe HTML rendering."""
    return _html.escape(str(text)) if text else ""


# ---------------------------------------------------------------------------
# Style constants
# ---------------------------------------------------------------------------

_C_PRIMARY = "#0078d4"
_C_PRIMARY_BG = "#deecf9"
_C_SUCCESS = "#107c10"
_C_SUCCESS_BG = "#dff6dd"
_C_WARNING = "#986f0b"
_C_WARNING_BG = "#fff4ce"
_C_DANGER = "#a4262c"
_C_DANGER_BG = "#fde7e9"
_C_SURFACE = "#ffffff"
_C_BG = "#faf9f8"
_C_BORDER = "#edebe9"
_C_TEXT = "#323130"
_C_SUBTLE = "#605e5c"
_C_HEADER_BG = "#f3f2f1"
_C_ROW_NEW = "#fffbe6"
_C_DOT = "#ffb900"


_SRC_GRID = "1.5fr 1.5fr 140px 1.5fr 80px"
_TBL_GRID = "1fr 1.5fr 120px 1.5fr"

_HEADER_CELL = (
    f"font-weight:600;font-size:11px;text-transform:uppercase;"
    f"letter-spacing:0.5px;color:{_C_SUBTLE}"
)

_SOURCE_TYPES = [
    "Lakehouse",
    "Warehouse",
    "MirroredDatabase",
    "SQLDatabase",
    "MirroredAzureDatabricksCatalog",
]
_SQL_ENDPOINT_TYPES = ["Lakehouse", "Warehouse"]

_TYPE_COLORS = {
    "Lakehouse": (_C_PRIMARY_BG, _C_PRIMARY),
    "Warehouse": ("#fff4ce", "#986f0b"),
    "MirroredDatabase": ("#e8dfec", "#6b3a8a"),
    "SQLDatabase": ("#d4edda", "#155724"),
    "MirroredAzureDatabricksCatalog": ("#fff0e0", "#8a4b08"),
}
_TYPE_COLOR_DEFAULT = (_C_HEADER_BG, _C_SUBTLE)


def _tag_html(text, bg, fg):
    """Render a small coloured pill / tag as inline HTML."""
    return (
        f'<span style="display:inline-block;font-size:11px;font-weight:600;'
        f'padding:2px 10px;border-radius:10px;background:{bg};color:{fg}">'
        f"{_esc(text)}</span>"
    )


def _change_dot():
    """Tiny orange dot indicating a changed value."""
    return (
        f'<span style="display:inline-block;width:6px;height:6px;'
        f"background:{_C_DOT};border-radius:50%;margin-left:5px;"
        f'vertical-align:middle"></span>'
    )


# ---------------------------------------------------------------------------
# ipywidgets-based UI
# ---------------------------------------------------------------------------


class _DirectLakeManagerUI:
    """Pure ipywidgets UI for the Direct Lake Manager."""

    def __init__(self, uid, sources, tables, dataset_name, dataset_id, workspace_id):
        self.uid = uid
        self.sources = [dict(s) for s in sources]
        self.tables = [dict(t) for t in tables]
        self.dataset_name = dataset_name
        self.dataset_id = dataset_id
        self.workspace_id = workspace_id
        self.df_temp = None
        self._temp_loaded = False
        self._temp_sort_col = "Temperature"
        self._temp_sort_asc = False
        self._temp_filter_text = ""

        # Deep-copy initial state for change tracking
        self._init_sources = json.loads(json.dumps(sources))
        self._init_tables = json.loads(json.dumps(tables))

        self._edit_src_idx = -1
        self._edit_tbl_idx = -1
        self._delete_src_idx = -1

        self._build()

    # ------------------------------------------------------------------
    # Lookup helpers for change tracking
    # ------------------------------------------------------------------

    @staticmethod
    def _get_expr(src):
        return src.get("expressionName") or src.get("ExpressionName") or ""

    def _find_init_src(self, expr_name):
        for s in self._init_sources:
            if self._get_expr(s) == expr_name:
                return s
        return None

    def _find_init_tbl(self, tbl_name, part_name):
        for t in self._init_tables:
            if t["tableName"] == tbl_name and t["partitionName"] == part_name:
                return t
        return None

    # ------------------------------------------------------------------
    # Build full widget tree
    # ------------------------------------------------------------------

    def _build(self):
        # Title bar
        self._title_bar = widgets.HTML(
            value=self._render_title_bar(),
            layout=widgets.Layout(margin="0"),
        )

        # ---- Sources page -------------------------------------------------
        self._add_src_btn = widgets.Button(
            description="   Add Source",
            icon="plus",
            button_style="primary",
            layout=widgets.Layout(height="32px"),
        )
        self._add_src_btn.on_click(self._on_add_source)

        src_header = widgets.HBox(
            [
                widgets.HTML(
                    value=(
                        f'<div style="font-size:16px;font-weight:600;'
                        f'color:{_C_TEXT}">Sources</div>'
                    )
                ),
                self._add_src_btn,
            ],
            layout=widgets.Layout(
                justify_content="space-between",
                align_items="center",
                padding="0 0 12px 0",
            ),
        )

        self._src_table_header = widgets.HBox(layout=widgets.Layout(display="none"))
        self._src_scroll = widgets.VBox(
            layout=widgets.Layout(overflow_y="auto", max_height="460px")
        )
        self._src_container = widgets.VBox(
            [self._src_table_header, self._src_scroll],
            layout=widgets.Layout(
                border=f"1px solid {_C_BORDER}",
                border_radius="12px",
                overflow="hidden",
                background=_C_SURFACE,
            ),
        )

        # Source edit form (hidden)
        self._src_form = self._build_source_form()
        self._src_form.layout.display = "none"

        # Source delete confirmation (hidden)
        self._del_confirm = self._build_delete_confirm()
        self._del_confirm.layout.display = "none"

        sources_page = widgets.VBox(
            [src_header, self._src_container, self._src_form, self._del_confirm],
            layout=widgets.Layout(padding="20px 24px"),
        )

        # ---- Tables page --------------------------------------------------
        self._tbl_table_header = widgets.HBox(layout=widgets.Layout(display="none"))
        self._tbl_scroll = widgets.VBox(
            layout=widgets.Layout(overflow_y="auto", max_height="460px")
        )
        self._tbl_container = widgets.VBox(
            [self._tbl_table_header, self._tbl_scroll],
            layout=widgets.Layout(
                border=f"1px solid {_C_BORDER}",
                border_radius="12px",
                overflow="hidden",
                background=_C_SURFACE,
            ),
        )

        self._tbl_form = self._build_table_form()
        self._tbl_form.layout.display = "none"

        tables_page = widgets.VBox(
            [
                widgets.HTML(
                    value=(
                        f'<div style="font-size:16px;font-weight:600;'
                        f'color:{_C_TEXT};padding-bottom:12px">Tables</div>'
                    )
                ),
                self._tbl_container,
                self._tbl_form,
            ],
            layout=widgets.Layout(padding="20px 24px"),
        )

        # ---- Temperature page ---------------------------------------------
        self._temp_container = widgets.HTML(
            value=(
                f'<div style="text-align:center;padding:44px 24px;'
                f'color:{_C_SUBTLE};font-size:14px">'
                f'<div style="font-size:32px;margin-bottom:8px">'
                f"\U0001F321\uFE0F</div>"
                f"Click the Temperature tab to load data.</div>"
            ),
            layout=widgets.Layout(margin="0"),
        )

        self._temp_rerun_btn = widgets.Button(
            description="   Refresh",
            icon="refresh",
            button_style="primary",
            layout=widgets.Layout(height="32px"),
        )
        self._temp_rerun_btn.on_click(lambda _: self._load_temperature())

        # Temperature filter / sort controls
        _temp_cols = [
            "Table Name",
            "Column Name",
            "Data Type",
            "Is Resident",
            "Temperature",
            "Last Accessed",
        ]
        self._temp_filter_input = widgets.Text(
            placeholder="Filter rows\u2026",
            layout=widgets.Layout(width="260px", height="32px"),
        )
        self._temp_filter_input.observe(self._on_temp_filter_change, names=["value"])

        self._temp_sort_dd = widgets.Dropdown(
            description="Sort by:",
            options=_temp_cols,
            value=self._temp_sort_col,
            style={"description_width": "50px"},
            layout=widgets.Layout(width="200px", height="32px"),
        )
        self._temp_sort_dd.observe(self._on_temp_sort_change, names=["value"])

        self._temp_sort_dir_btn = widgets.Button(
            description="\u25BC Desc",
            layout=widgets.Layout(width="80px", height="32px"),
        )
        self._temp_sort_dir_btn.on_click(self._on_temp_sort_dir_toggle)

        temp_toolbar = widgets.HBox(
            [
                self._temp_filter_input,
                widgets.HTML(value="", layout=widgets.Layout(flex="1")),
                self._temp_sort_dd,
                self._temp_sort_dir_btn,
            ],
            layout=widgets.Layout(
                padding="0 0 10px 0",
                align_items="center",
                gap="8px",
            ),
        )

        temp_header = widgets.HBox(
            [
                widgets.HTML(
                    value=(
                        f'<div style="font-size:16px;font-weight:600;'
                        f'color:{_C_TEXT}">Column Temperature</div>'
                    )
                ),
                self._temp_rerun_btn,
            ],
            layout=widgets.Layout(
                justify_content="space-between",
                align_items="center",
                padding="0 0 12px 0",
            ),
        )

        temp_page = widgets.VBox(
            [temp_header, temp_toolbar, self._temp_container],
            layout=widgets.Layout(padding="20px 24px"),
        )

        # ---- Manual tab navigation ----------------------------------------
        self._sources_page = sources_page
        self._tables_page = tables_page
        self._temp_page = temp_page
        self._tables_page.layout.display = "none"
        self._temp_page.layout.display = "none"

        self._tab_src_btn = widgets.Button(
            description="⛁  Sources",
            icon="database",
            button_style="primary",
            layout=widgets.Layout(
                height="34px",
                border_radius="980px",
            ),
        )
        self._tab_tbl_btn = widgets.Button(
            description="▦  Tables",
            icon="table",
            layout=widgets.Layout(
                height="34px",
                border_radius="980px",
            ),
        )
        self._tab_temp_btn = widgets.Button(
            description="🌡 Temperature",
            icon="thermometer-half",
            layout=widgets.Layout(
                height="34px",
                border_radius="980px",
            ),
        )
        self._tab_src_btn.on_click(lambda _: self._switch_tab("sources"))
        self._tab_tbl_btn.on_click(lambda _: self._switch_tab("tables"))
        self._tab_temp_btn.on_click(lambda _: self._switch_tab("temperature"))

        tab_bar = widgets.HBox(
            [self._tab_src_btn, self._tab_tbl_btn, self._tab_temp_btn],
            layout=widgets.Layout(
                padding="10px 24px",
                gap="6px",
                background=_C_SURFACE,
                border_bottom=f"1px solid {_C_BORDER}",
            ),
        )

        # ---- Bottom bar ---------------------------------------------------
        self._apply_btn = widgets.Button(
            description="   Apply Changes",
            icon="check",
            button_style="primary",
            layout=widgets.Layout(height="36px", margin="0 12px 0 0"),
        )
        self._apply_btn.on_click(self._on_apply)

        self._status = widgets.HTML(value="")

        self._bottom_bar = widgets.HBox(
            [self._apply_btn, self._status],
            layout=widgets.Layout(padding="12px 24px", align_items="center"),
        )

        # ---- Root container -----------------------------------------------
        self.widget = widgets.VBox(
            [
                self._title_bar,
                tab_bar,
                self._sources_page,
                self._tables_page,
                self._temp_page,
                self._bottom_bar,
            ],
            layout=widgets.Layout(
                border=f"1px solid {_C_BORDER}",
                border_radius="14px",
                overflow="hidden",
                background=_C_BG,
                max_width="1100px",
                box_shadow="0 2px 12px rgba(0,0,0,0.08)",
            ),
        )

        # Initial render
        self._render_sources()
        self._render_tables()
        self._update_apply_btn()

    # ------------------------------------------------------------------
    # Change detection
    # ------------------------------------------------------------------

    def _has_changes(self):
        """Return True if current state differs from the saved baseline."""
        return json.dumps(self.sources, sort_keys=True) != json.dumps(
            self._init_sources, sort_keys=True
        ) or json.dumps(self.tables, sort_keys=True) != json.dumps(
            self._init_tables, sort_keys=True
        )

    def _update_apply_btn(self):
        self._apply_btn.disabled = not self._has_changes()

    # ------------------------------------------------------------------
    # Tab switching
    # ------------------------------------------------------------------

    def _switch_tab(self, tab):
        pages = {
            "sources": self._sources_page,
            "tables": self._tables_page,
            "temperature": self._temp_page,
        }
        btns = {
            "sources": self._tab_src_btn,
            "tables": self._tab_tbl_btn,
            "temperature": self._tab_temp_btn,
        }
        for key, page in pages.items():
            page.layout.display = None if key == tab else "none"
        for key, btn in btns.items():
            btn.button_style = "primary" if key == tab else ""

        # Hide Apply Changes bar on the Temperature page
        self._bottom_bar.layout.display = "none" if tab == "temperature" else None

        # Lazy-load temperature data on first visit
        if tab == "temperature" and not self._temp_loaded:
            self._load_temperature()

    def _load_temperature(self):
        """Fetch column temperature data and refresh the table."""
        self._temp_container.value = (
            f'<div style="text-align:center;padding:44px 24px;'
            f'color:{_C_SUBTLE};font-size:14px">'
            f"Loading temperature data\u2026</div>"
        )
        try:
            df = fabric.list_columns(
                dataset=self.dataset_id,
                workspace=self.workspace_id,
                extended=True,
            )
            df = df[
                df["Type"] != "RowNumber"
            ].copy()  # Exclude system-generated columns
            self.df_temp = df[
                [
                    "Table Name",
                    "Column Name",
                    "Data Type",
                    "Is Resident",
                    "Temperature",
                    "Last Accessed",
                ]
            ]
            self._temp_loaded = True
            self._temp_container.value = self._render_temp_table()
        except Exception as e:
            self._temp_container.value = (
                f'<div style="text-align:center;padding:32px 24px;'
                f'color:{_C_DANGER};font-size:14px">'
                f"Error loading temperature data: {_esc(str(e))}</div>"
            )

    # ------------------------------------------------------------------
    # Title bar
    # ------------------------------------------------------------------

    def _render_title_bar(self):
        # SVG: Fluent-style semantic model icon – interconnected nodes
        # with a dynamic directional sweep, evoking data flow / graph
        logo_svg = (
            '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" '
            'viewBox="0 0 32 32" fill="none" style="flex-shrink:0">'
            # Sweep arc – conveys motion / flow
            '<path d="M6 26 C6 14 14 6 26 6" stroke="#0078d4" '
            'stroke-width="1.8" stroke-linecap="round" fill="none" '
            'opacity="0.25"/>'
            '<path d="M8 26 C8 16 16 8 26 8" stroke="#0078d4" '
            'stroke-width="1.2" stroke-linecap="round" fill="none" '
            'opacity="0.15"/>'
            # Connector lines (behind nodes)
            '<line x1="10" y1="22" x2="16" y2="14" '
            'stroke="#0078d4" stroke-width="1.5" opacity="0.5"/>'
            '<line x1="16" y1="14" x2="24" y2="10" '
            'stroke="#0078d4" stroke-width="1.5" opacity="0.5"/>'
            '<line x1="16" y1="14" x2="24" y2="20" '
            'stroke="#0078d4" stroke-width="1.5" opacity="0.5"/>'
            '<line x1="10" y1="22" x2="16" y2="26" '
            'stroke="#0078d4" stroke-width="1.5" opacity="0.5"/>'
            # Node: bottom-left (source)
            '<circle cx="10" cy="22" r="3.5" fill="#0078d4"/>'
            '<circle cx="10" cy="22" r="1.5" fill="#ffffff"/>'
            # Node: centre (hub / model)
            '<circle cx="16" cy="14" r="4" fill="#005a9e"/>'
            '<circle cx="16" cy="14" r="1.8" fill="#ffffff"/>'
            # Node: top-right
            '<circle cx="24" cy="10" r="3" fill="#0078d4"/>'
            '<circle cx="24" cy="10" r="1.3" fill="#ffffff"/>'
            # Node: bottom-right
            '<circle cx="24" cy="20" r="3" fill="#0078d4"/>'
            '<circle cx="24" cy="20" r="1.3" fill="#ffffff"/>'
            # Node: bottom-centre
            '<circle cx="16" cy="26" r="2.5" fill="#0078d4" opacity="0.7"/>'
            '<circle cx="16" cy="26" r="1.1" fill="#ffffff"/>'
            "</svg>"
        )

        return (
            f'<div style="display:flex;align-items:center;gap:12px;'
            f"padding:18px 24px;background:linear-gradient(135deg,"
            f"{_C_SURFACE} 0%,#f0f6fc 100%);"
            f'border-bottom:1px solid {_C_BORDER}">'
            f"{logo_svg}"
            f'<div style="flex:1">'
            f'<div style="font-size:19px;font-weight:700;'
            f'color:{_C_TEXT};letter-spacing:-0.3px;line-height:1.2">'
            f"Direct Lake Manager</div>"
            f'<div style="font-size:11px;color:{_C_SUBTLE};'
            f'margin-top:2px">Manage sources &amp; table mappings</div>'
            f"</div>"
            f'<span style="font-size:12px;font-weight:600;padding:5px 16px;'
            f"border-radius:980px;background:{_C_PRIMARY};color:#fff;"
            f'letter-spacing:0.2px">'
            f"{_esc(self.dataset_name)}</span></div>"
        )

    # ------------------------------------------------------------------
    # Source form
    # ------------------------------------------------------------------

    def _build_source_form(self):
        desc_w = "140px"

        self._src_form_title = widgets.HTML(value=self._form_heading("Add Source"))
        self._src_f_expr = widgets.Text(
            description="Expression Name:",
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._src_f_name = widgets.Text(
            description="Item Name:",
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._src_f_type = widgets.Dropdown(
            description="Item Type:",
            options=_SOURCE_TYPES,
            value="Lakehouse",
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._src_f_ws = widgets.Text(
            description="Workspace:",
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._src_f_sql = widgets.Dropdown(
            description="SQL Endpoint:",
            options=[("Yes", True), ("No", False)],
            value=False,
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )

        self._src_f_type.observe(self._on_type_change, names=["value"])

        save_btn = widgets.Button(
            description="Save",
            button_style="primary",
            layout=widgets.Layout(height="32px", width="90px"),
        )
        cancel_btn = widgets.Button(
            description="Cancel",
            layout=widgets.Layout(height="32px", width="90px"),
        )
        save_btn.on_click(self._on_save_source)
        cancel_btn.on_click(lambda _: self._hide_source_form())

        return widgets.VBox(
            [
                self._src_form_title,
                self._src_f_expr,
                self._src_f_name,
                self._src_f_type,
                self._src_f_ws,
                self._src_f_sql,
                widgets.HBox(
                    [cancel_btn, save_btn],
                    layout=widgets.Layout(
                        justify_content="flex-end",
                        gap="8px",
                        padding="8px 0 0 0",
                    ),
                ),
            ],
            layout=widgets.Layout(
                border=f"1px solid {_C_PRIMARY}",
                border_radius="12px",
                padding="16px 20px",
                margin="12px 0 0 0",
                background=_C_SURFACE,
            ),
        )

    # ------------------------------------------------------------------
    # Delete confirmation
    # ------------------------------------------------------------------

    def _build_delete_confirm(self):
        self._del_msg = widgets.HTML(value="")

        confirm_btn = widgets.Button(
            description="Delete",
            button_style="danger",
            layout=widgets.Layout(height="32px", width="90px"),
        )
        cancel_btn = widgets.Button(
            description="Cancel",
            layout=widgets.Layout(height="32px", width="90px"),
        )
        confirm_btn.on_click(self._on_confirm_delete)
        cancel_btn.on_click(self._on_cancel_delete)

        return widgets.VBox(
            [
                self._del_msg,
                widgets.HBox(
                    [cancel_btn, confirm_btn],
                    layout=widgets.Layout(
                        justify_content="flex-end",
                        gap="8px",
                        padding="8px 0 0 0",
                    ),
                ),
            ],
            layout=widgets.Layout(
                border=f"1px solid {_C_DANGER}",
                border_radius="12px",
                padding="16px 20px",
                margin="12px 0 0 0",
                background="#fef6f6",
            ),
        )

    # ------------------------------------------------------------------
    # Table form
    # ------------------------------------------------------------------

    def _build_table_form(self):
        desc_w = "140px"

        self._tbl_form_title = widgets.HTML(
            value=self._form_heading("Edit Table Mapping")
        )
        self._tbl_f_table = widgets.Text(
            description="Table Name:",
            disabled=True,
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._tbl_f_expr = widgets.Dropdown(
            description="Expression:",
            options=[],
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._tbl_f_entity = widgets.Text(
            description="Entity Name:",
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )
        self._tbl_f_schema = widgets.Text(
            description="Schema Name:",
            style={"description_width": desc_w},
            layout=widgets.Layout(width="100%"),
        )

        save_btn = widgets.Button(
            description="Save",
            button_style="primary",
            layout=widgets.Layout(height="32px", width="90px"),
        )
        cancel_btn = widgets.Button(
            description="Cancel",
            layout=widgets.Layout(height="32px", width="90px"),
        )
        save_btn.on_click(self._on_save_table)
        cancel_btn.on_click(lambda _: self._hide_table_form())

        return widgets.VBox(
            [
                self._tbl_form_title,
                self._tbl_f_table,
                self._tbl_f_expr,
                self._tbl_f_entity,
                self._tbl_f_schema,
                widgets.HBox(
                    [cancel_btn, save_btn],
                    layout=widgets.Layout(
                        justify_content="flex-end",
                        gap="8px",
                        padding="8px 0 0 0",
                    ),
                ),
            ],
            layout=widgets.Layout(
                border=f"1px solid {_C_PRIMARY}",
                border_radius="12px",
                padding="16px 20px",
                margin="12px 0 0 0",
                background=_C_SURFACE,
            ),
        )

    # ------------------------------------------------------------------
    # Render source rows
    # ------------------------------------------------------------------

    def _render_sources(self):
        if not self.sources:
            self._src_table_header.layout.display = "none"
            self._src_scroll.children = [
                widgets.HTML(
                    value=(
                        f'<div style="text-align:center;padding:44px 24px;'
                        f'color:{_C_SUBTLE};font-size:14px">'
                        f'<div style="font-size:32px;margin-bottom:8px">'
                        f"\U0001F4C1</div>No sources configured.</div>"
                    )
                )
            ]
            return

        header = widgets.HTML(
            value=(
                f'<div style="display:grid;grid-template-columns:{_SRC_GRID};'
                f"gap:12px;padding:11px 16px;background:{_C_HEADER_BG};"
                f'border-bottom:1px solid {_C_BORDER}">'
                f'<div style="{_HEADER_CELL}">Expression</div>'
                f'<div style="{_HEADER_CELL}">Item Name</div>'
                f'<div style="{_HEADER_CELL}">Type</div>'
                f'<div style="{_HEADER_CELL}">Workspace</div>'
                f'<div style="{_HEADER_CELL}">SQL</div></div>'
            ),
            layout=widgets.Layout(flex="1"),
        )
        self._src_table_header.children = [
            header,
            widgets.HTML(
                value="",
                layout=widgets.Layout(
                    min_width="76px" if len(self.sources) > 1 else "40px"
                ),
            ),
        ]
        self._src_table_header.layout.display = None

        rows = []
        for i, src in enumerate(self.sources):
            rows.append(self._make_source_row(i, src))

        self._src_scroll.children = rows

    def _make_source_row(self, idx, src):
        expr = self._get_expr(src)
        orig = self._find_init_src(expr)
        is_new = orig is None

        item_type = src.get("itemType", "")
        tc_bg, tc_fg = _TYPE_COLORS.get(item_type, _TYPE_COLOR_DEFAULT)

        sql_yes = bool(src.get("usesSqlEndpoint", False))
        sql_bg = _C_SUCCESS_BG if sql_yes else _C_HEADER_BG
        sql_fg = _C_SUCCESS if sql_yes else _C_SUBTLE
        sql_text = "Yes" if sql_yes else "No"

        dot = _change_dot()
        ch_name = (
            dot if (not is_new and orig.get("itemName") != src.get("itemName")) else ""
        )
        ch_type = (
            dot if (not is_new and orig.get("itemType") != src.get("itemType")) else ""
        )
        ch_ws = (
            dot
            if (not is_new and orig.get("workspaceName") != src.get("workspaceName"))
            else ""
        )
        ch_sql = (
            dot if (not is_new and bool(orig.get("usesSqlEndpoint")) != sql_yes) else ""
        )

        row_bg = _C_ROW_NEW if is_new else _C_SURFACE

        data_html = widgets.HTML(
            value=(
                f'<div style="display:grid;grid-template-columns:{_SRC_GRID};'
                f"gap:12px;padding:11px 16px;background:{row_bg};"
                f'align-items:center;font-size:13px">'
                f'<div style="color:{_C_TEXT}">{_esc(expr)}</div>'
                f'<div style="color:{_C_TEXT}">'
                f"{_esc(src.get('itemName', ''))}{ch_name}</div>"
                f"<div>{_tag_html(item_type, tc_bg, tc_fg)}{ch_type}</div>"
                f'<div style="color:{_C_TEXT}">'
                f"{_esc(src.get('workspaceName', ''))}{ch_ws}</div>"
                f"<div>{_tag_html(sql_text, sql_bg, sql_fg)}{ch_sql}</div>"
                f"</div>"
            ),
            layout=widgets.Layout(flex="1"),
        )

        edit_btn = widgets.Button(
            description="\u270E",
            tooltip="Edit",
            layout=widgets.Layout(width="34px", height="34px"),
        )
        edit_btn.on_click(lambda _, i=idx: self._on_edit_source(i))

        btns = [edit_btn]
        if len(self.sources) > 1:
            del_btn = widgets.Button(
                description="\u2715",
                tooltip="Delete",
                layout=widgets.Layout(width="34px", height="34px"),
            )
            del_btn.style.text_color = _C_DANGER
            del_btn.on_click(lambda _, i=idx: self._on_delete_source(i))
            btns.append(del_btn)

        return widgets.HBox(
            [data_html] + btns,
            layout=widgets.Layout(
                align_items="center",
                border_bottom=f"1px solid {_C_BORDER}",
                padding="0 8px 0 0",
            ),
        )

    # ------------------------------------------------------------------
    # Render temperature table
    # ------------------------------------------------------------------

    def _render_temp_table(self):
        """Render the temperature DataFrame as a styled HTML table."""
        if self.df_temp is None or self.df_temp.empty:
            return (
                f'<div style="text-align:center;padding:44px 24px;'
                f'color:{_C_SUBTLE};font-size:14px">'
                f'<div style="font-size:32px;margin-bottom:8px">'
                f"\U0001F321\uFE0F</div>No temperature data available.</div>"
            )

        _TEMP_COLORS = {
            "Hot": (_C_DANGER_BG, _C_DANGER),
            "Warm": (_C_WARNING_BG, _C_WARNING),
            "Cold": (_C_PRIMARY_BG, _C_PRIMARY),
        }
        _TEMP_DEFAULT = (_C_HEADER_BG, _C_SUBTLE)

        _RES_COLORS = {
            True: (_C_SUCCESS_BG, _C_SUCCESS, "Yes"),
            False: (_C_HEADER_BG, _C_SUBTLE, "No"),
        }

        # --- Apply filter ---
        df = self.df_temp
        filt = self._temp_filter_text.strip().lower()
        if filt:
            mask = df.apply(
                lambda r: any(filt in str(v).lower() for v in r),
                axis=1,
            )
            df = df[mask]

        # --- Apply sort ---
        sort_col = self._temp_sort_col
        if sort_col in df.columns:
            df = df.sort_values(
                by=sort_col,
                ascending=self._temp_sort_asc,
                na_position="last",
            )

        if df.empty:
            return (
                f'<div style="text-align:center;padding:44px 24px;'
                f'color:{_C_SUBTLE};font-size:14px">'
                f"No rows match the current filter.</div>"
            )

        # --- Sort indicator ---
        sort_indicator = "\u25B2" if self._temp_sort_asc else "\u25BC"

        html = (
            f'<div style="border:1px solid {_C_BORDER};border-radius:12px;'
            f'overflow-y:auto;max-height:500px;background:{_C_SURFACE};">'
            f'<table style="width:100%;border-collapse:separate;'
            f"border-spacing:0;font-size:13px;"
            f"font-family:-apple-system,BlinkMacSystemFont,"
            f'Segoe UI,Roboto,sans-serif">'
            f'<thead><tr style="background:{_C_HEADER_BG};'
            f'border-bottom:1px solid {_C_BORDER}">'
        )
        for col in self.df_temp.columns:
            indicator = ""
            if col == sort_col:
                indicator = (
                    f' <span style="color:{_C_PRIMARY};font-size:10px;'
                    f'margin-left:4px">{sort_indicator}</span>'
                )
            html += (
                f'<th style="padding:11px 16px;text-align:left;{_HEADER_CELL};'
                f"position:sticky;top:0;z-index:1;"
                f'background:{_C_HEADER_BG};">'
                f"{_esc(col)}{indicator}</th>"
            )
        html += "</tr></thead><tbody>"

        for _, row in df.iterrows():
            html += (
                f'<tr style="border-bottom:1px solid {_C_BORDER};"'
                f" onmouseover=\"this.style.background='#f9f9fb'\""
                f" onmouseout=\"this.style.background='transparent'\">"
            )
            for col in self.df_temp.columns:
                val = row[col]
                if col == "Temperature":
                    temp_str = str(val) if val is not None else ""
                    bg, fg = _TEMP_COLORS.get(temp_str, _TEMP_DEFAULT)
                    cell = _tag_html(temp_str, bg, fg)
                elif col == "Is Resident":
                    try:
                        res_key = bool(val)
                    except (TypeError, ValueError):
                        res_key = False
                    bg, fg, label = _RES_COLORS.get(res_key, _RES_COLORS[False])
                    cell = _tag_html(label, bg, fg)
                else:
                    cell = _esc(str(val) if val is not None else "")
                html += f'<td style="padding:11px 16px;color:{_C_TEXT}">{cell}</td>'
            html += "</tr>"

        html += "</tbody></table></div>"
        return html

    # ------------------------------------------------------------------
    # Temperature filter / sort callbacks
    # ------------------------------------------------------------------

    def _on_temp_filter_change(self, change):
        self._temp_filter_text = change["new"]
        if self._temp_loaded and self.df_temp is not None:
            self._temp_container.value = self._render_temp_table()

    def _on_temp_sort_change(self, change):
        self._temp_sort_col = change["new"]
        if self._temp_loaded and self.df_temp is not None:
            self._temp_container.value = self._render_temp_table()

    def _on_temp_sort_dir_toggle(self, _):
        self._temp_sort_asc = not self._temp_sort_asc
        self._temp_sort_dir_btn.description = (
            "\u25B2 Asc" if self._temp_sort_asc else "\u25BC Desc"
        )
        if self._temp_loaded and self.df_temp is not None:
            self._temp_container.value = self._render_temp_table()

    # ------------------------------------------------------------------
    # Render table rows
    # ------------------------------------------------------------------

    def _render_tables(self):
        if not self.tables:
            self._tbl_table_header.layout.display = "none"
            self._tbl_scroll.children = [
                widgets.HTML(
                    value=(
                        f'<div style="text-align:center;padding:44px 24px;'
                        f'color:{_C_SUBTLE};font-size:14px">'
                        f'<div style="font-size:32px;margin-bottom:8px">'
                        f"\U0001F4CB</div>No Direct Lake tables found.</div>"
                    )
                )
            ]
            return

        header = widgets.HTML(
            value=(
                f'<div style="display:grid;grid-template-columns:{_TBL_GRID};'
                f"gap:12px;padding:11px 16px;background:{_C_HEADER_BG};"
                f'border-bottom:1px solid {_C_BORDER}">'
                f'<div style="{_HEADER_CELL}">Table</div>'
                f'<div style="{_HEADER_CELL}">Entity Name</div>'
                f'<div style="{_HEADER_CELL}">Schema</div>'
                f'<div style="{_HEADER_CELL}">Expression</div></div>'
            ),
            layout=widgets.Layout(flex="1"),
        )
        self._tbl_table_header.children = [
            header,
            widgets.HTML(
                value="",
                layout=widgets.Layout(min_width="40px"),
            ),
        ]
        self._tbl_table_header.layout.display = None

        rows = []
        for i, tbl in enumerate(self.tables):
            rows.append(self._make_table_row(i, tbl))

        self._tbl_scroll.children = rows

    def _make_table_row(self, idx, tbl):
        orig = self._find_init_tbl(tbl["tableName"], tbl["partitionName"])

        dot = _change_dot()
        ch_entity = (
            dot if (orig and orig.get("entityName") != tbl.get("entityName")) else ""
        )
        ch_schema = (
            dot if (orig and orig.get("schemaName") != tbl.get("schemaName")) else ""
        )
        ch_expr = (
            dot
            if (orig and orig.get("expressionName") != tbl.get("expressionName"))
            else ""
        )

        data_html = widgets.HTML(
            value=(
                f'<div style="display:grid;grid-template-columns:{_TBL_GRID};'
                f"gap:12px;padding:11px 16px;align-items:center;"
                f'font-size:13px">'
                f'<div style="font-weight:500;color:{_C_TEXT}">'
                f"{_esc(tbl.get('tableName', ''))}</div>"
                f'<div style="color:{_C_TEXT}">'
                f"{_esc(tbl.get('entityName', ''))}{ch_entity}</div>"
                f'<div style="color:{_C_TEXT}">'
                f"{_esc(tbl.get('schemaName', ''))}{ch_schema}</div>"
                f'<div style="color:{_C_TEXT}">'
                f"{_esc(tbl.get('expressionName', ''))}{ch_expr}</div>"
                f"</div>"
            ),
            layout=widgets.Layout(flex="1"),
        )

        edit_btn = widgets.Button(
            description="\u270E",
            tooltip="Edit",
            layout=widgets.Layout(width="34px", height="34px"),
        )
        edit_btn.on_click(lambda _, i=idx: self._on_edit_table(i))

        return widgets.HBox(
            [data_html, edit_btn],
            layout=widgets.Layout(
                align_items="center",
                border_bottom=f"1px solid {_C_BORDER}",
                padding="0 8px 0 0",
            ),
        )

    # ------------------------------------------------------------------
    # Source form callbacks
    # ------------------------------------------------------------------

    def _on_type_change(self, change):
        type_val = change["new"]
        if type_val not in _SQL_ENDPOINT_TYPES:
            self._src_f_sql.value = False
            self._src_f_sql.disabled = True
        elif type_val == "Warehouse":
            self._src_f_sql.value = True
            self._src_f_sql.disabled = True
        else:
            self._src_f_sql.disabled = False

    def _on_add_source(self, _):
        self._edit_src_idx = -1
        self._src_form_title.value = self._form_heading("Add Source")
        self._src_f_expr.value = ""
        self._src_f_expr.disabled = False
        self._src_f_name.value = ""
        self._src_f_type.value = "Lakehouse"
        self._src_f_ws.value = ""
        self._src_f_sql.value = False
        self._src_f_sql.disabled = False
        self._src_form.layout.display = None
        self._clear_status()

    def _on_edit_source(self, idx):
        self._edit_src_idx = idx
        src = self.sources[idx]
        self._src_form_title.value = self._form_heading("Edit Source")
        self._src_f_expr.value = self._get_expr(src)
        self._src_f_expr.disabled = True
        self._src_f_name.value = src.get("itemName", "")
        self._src_f_type.value = src.get("itemType", "Lakehouse")
        self._src_f_ws.value = src.get("workspaceName", "")
        self._src_f_sql.value = bool(src.get("usesSqlEndpoint", False))
        self._on_type_change({"new": src.get("itemType", "Lakehouse")})
        self._src_form.layout.display = None
        self._clear_status()

    def _on_save_source(self, _):
        expr_name = self._src_f_expr.value.strip()
        item_name = self._src_f_name.value.strip()
        item_type = self._src_f_type.value
        ws_name = self._src_f_ws.value.strip()
        sql = self._src_f_sql.value

        if not expr_name:
            self._show_status("Expression name is required.", "warning")
            return
        if not item_name:
            self._show_status("Item name is required.", "warning")
            return
        if not ws_name:
            self._show_status("Workspace is required.", "warning")
            return

        obj = {
            "expressionName": expr_name,
            "itemId": (
                self.sources[self._edit_src_idx].get("itemId", "")
                if self._edit_src_idx >= 0
                else ""
            ),
            "itemName": item_name,
            "itemType": item_type,
            "workspaceId": (
                self.sources[self._edit_src_idx].get("workspaceId", "")
                if self._edit_src_idx >= 0
                else ""
            ),
            "workspaceName": ws_name,
            "usesSqlEndpoint": sql,
        }

        if self._edit_src_idx >= 0:
            self.sources[self._edit_src_idx] = obj
            self._show_status("Source updated.", "success")
        else:
            self.sources.append(obj)
            self._show_status("Source added.", "success")

        self._hide_source_form()
        self._render_sources()
        self._update_apply_btn()

    def _hide_source_form(self):
        self._src_form.layout.display = "none"

    # ------------------------------------------------------------------
    # Delete source callbacks
    # ------------------------------------------------------------------

    def _on_delete_source(self, idx):
        self._delete_src_idx = idx
        src_expr = self._get_expr(self.sources[idx])
        affected = [
            t["tableName"] for t in self.tables if t.get("expressionName") == src_expr
        ]

        msg = (
            f'<div style="font-size:14px;line-height:1.6">'
            f"Are you sure you want to remove source "
            f"<strong>{_esc(src_expr)}</strong>?"
        )
        if affected:
            msg += (
                f'<br><br><span style="color:{_C_DANGER}">'
                f"The following table(s) use this source and will "
                f"break:</span>"
                f'<ul style="margin:6px 0 0 16px">'
            )
            for name in affected:
                msg += f"<li>{_esc(name)}</li>"
            msg += "</ul>"
        msg += "</div>"

        self._del_msg.value = msg
        self._del_confirm.layout.display = None

    def _on_confirm_delete(self, _):
        if self._delete_src_idx >= 0:
            self.sources.pop(self._delete_src_idx)
            self._delete_src_idx = -1
            self._del_confirm.layout.display = "none"
            self._render_sources()
            self._update_apply_btn()
            self._show_status("Source removed.", "success")

    def _on_cancel_delete(self, _):
        self._delete_src_idx = -1
        self._del_confirm.layout.display = "none"

    # ------------------------------------------------------------------
    # Table form callbacks
    # ------------------------------------------------------------------

    def _on_edit_table(self, idx):
        self._edit_tbl_idx = idx
        tbl = self.tables[idx]
        self._tbl_f_table.value = tbl.get("tableName", "")

        # Populate expression dropdown from current sources
        expr_names = list(
            dict.fromkeys(self._get_expr(s) for s in self.sources if self._get_expr(s))
        )
        self._tbl_f_expr.options = expr_names
        self._tbl_f_expr.value = tbl.get("expressionName", "")
        self._tbl_f_entity.value = tbl.get("entityName", "")
        self._tbl_f_schema.value = tbl.get("schemaName", "")
        self._tbl_form.layout.display = None
        self._clear_status()

    def _on_save_table(self, _):
        expr = self._tbl_f_expr.value
        entity = self._tbl_f_entity.value.strip()
        schema = self._tbl_f_schema.value.strip()

        if not expr:
            self._show_status("Expression name is required.", "warning")
            return
        if not entity:
            self._show_status("Entity name is required.", "warning")
            return

        self.tables[self._edit_tbl_idx]["expressionName"] = expr
        self.tables[self._edit_tbl_idx]["entityName"] = entity
        self.tables[self._edit_tbl_idx]["schemaName"] = schema

        self._hide_table_form()
        self._render_tables()
        self._update_apply_btn()
        self._show_status("Table mapping updated.", "success")

    def _hide_table_form(self):
        self._tbl_form.layout.display = "none"

    # ------------------------------------------------------------------
    # Apply changes
    # ------------------------------------------------------------------

    def _on_apply(self, _):
        state = json.dumps({"sources": self.sources, "tables": self.tables})
        try:
            _apply_changes(self.uid, state)
            # Reset baseline so change dots clear
            self._init_sources = json.loads(json.dumps(self.sources))
            self._init_tables = json.loads(json.dumps(self.tables))
            self._render_sources()
            self._render_tables()
            self._update_apply_btn()
            self._show_status(
                f"{icons.green_dot} Changes applied successfully.", "success"
            )
        except Exception as e:
            self._show_status(f"Error: {e}", "danger")

    # ------------------------------------------------------------------
    # Status helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _form_heading(text):
        return (
            f'<div style="font-size:15px;font-weight:600;'
            f'color:{_C_TEXT};padding:0 0 8px 0">{_esc(text)}</div>'
        )

    def _show_status(self, msg, level="info"):
        colors = {
            "success": (_C_SUCCESS_BG, _C_SUCCESS),
            "warning": (_C_WARNING_BG, _C_WARNING),
            "danger": (_C_DANGER_BG, _C_DANGER),
            "info": (_C_PRIMARY_BG, _C_PRIMARY),
        }
        bg, fg = colors.get(level, colors["info"])
        self._status.value = (
            f'<div style="padding:6px 14px;border-radius:8px;background:{bg};'
            f'color:{fg};font-size:13px;font-weight:500">{msg}</div>'
        )

    def _clear_status(self):
        self._status.value = ""

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------

    def show(self):
        """Display the widget in the notebook."""
        display(self.widget)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def direct_lake_manager(
    dataset: str | UUID,
    workspace: Optional[str | UUID] = None,
):
    """
    Opens an interactive Direct Lake Manager UI for a semantic model.

    The manager displays two navigable pages:

    * **Sources** -- View, add, edit, and delete the Direct Lake sources
      (lakehouses / warehouses) that back the semantic model.
    * **Tables** -- View and edit the Direct Lake table partition mappings
      including expression name, entity name, and schema name.

    Parameters
    ----------
    dataset : str | uuid.UUID
        Name or ID of the semantic model.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    """

    # Validate & collect data
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_item_name_and_id(
        item=dataset, type="SemanticModel", workspace=workspace_id
    )
    with connect_semantic_model(dataset=dataset_id, workspace=workspace_id) as tom:
        if not tom.is_direct_lake():
            raise ValueError(
                f"{icons.red_dot} The provided dataset is not a Direct Lake dataset."
            )

    sources = _collect_sources(dataset=dataset_id, workspace=workspace_id)
    tables = _collect_tables(dataset=dataset_id, workspace=workspace_id)

    uid = uuid.uuid4().hex[:10]

    # Register callback for Apply Changes
    _dlm_callbacks[uid] = {
        "dataset_id": str(dataset_id),
        "workspace_id": str(workspace_id),
    }

    ui = _DirectLakeManagerUI(
        uid=uid,
        sources=sources,
        tables=tables,
        dataset_name=dataset_name,
        dataset_id=str(dataset_id),
        workspace_id=str(workspace_id),
    )
    ui.show()
