import sempy.fabric as fabric
import pandas as pd
import json
import csv
import re
import os
import inspect
import warnings
import datetime
from IPython.display import display, HTML
from sempy_labs._model_dependencies import get_model_calc_dependencies
from sempy_labs._helper_functions import (
    format_dax_object_name,
    create_relationship_name,
    save_as_delta_table,
    resolve_workspace_capacity,
    resolve_dataset_name_and_id,
    get_language_codes,
    _get_column_aggregate,
    resolve_workspace_name_and_id,
    _create_spark_session,
)
from sempy_labs.lakehouse import get_lakehouse_tables, lakehouse_attached
from sempy_labs.tom import connect_semantic_model
from typing import Optional, List
from sempy._utils._log import log
import sempy_labs._icons as icons
from pyspark.sql.functions import col, flatten
from pyspark.sql.types import StructType, StructField, StringType
from uuid import UUID
from pathlib import Path
import sempy_labs.semantic_model._bpa_rules as bpa_rules


severity_map = {
    "Warning": icons.warning,
    "Error": icons.error,
    "Info": icons.info,
}

_plural_map = {
    "Column": "Columns",
    "Calculated Column": "Calculated Columns",
    "Measure": "Measures",
    "Table": "Tables",
    "Calculated Table": "Calculated Tables",
    "Relationship": "Relationships",
    "Partition": "Partitions",
    "Hierarchy": "Hierarchies",
    "Role": "Roles",
    "Model": "Models",
    "Calculation Item": "Calculation Items",
    "Row Level Security": "Row Level Security",
    "Function": "Functions",
}

OBJ_TYPE_ICONS = {
    "Columns": "▯",
    "Calculated Columns": "▯",
    "Measures": "∑",
    "Tables": "▦",
    "Calculated Tables": "▦",
    "Relationships": "⟷",
    "Hierarchies": "≡",
    "Partitions": "▤",
    "Roles": "🔒",
    "Row Level Security": "🛡",
    "Calculation Items": "⚙",
    "Models": "⧈",
    "Functions": "𝑓",
}


def get_tom(dataset, workspace, check_dependencies, extended):

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset, workspace=workspace_id
    )

    if check_dependencies:
        dep = get_model_calc_dependencies(dataset=dataset_id, workspace=workspace_id)
    else:
        dep = pd.DataFrame(
            columns=[
                "Table Name",
                "Object Name",
                "Object Type",
                "Expression",
                "Referenced Table",
                "Referenced Object",
                "Referenced Object Type",
                "Full Object Name",
                "Referenced Full Object Name",
                "Parent Node",
            ]
        )

    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:

        if tom.model.Tables.Count == 0:
            print(
                f"{icons.warning} The '{dataset_name}' semantic model within the "
                f"'{workspace_name}' workspace has no tables and therefore there "
                f"are no valid BPA results."
            )
            return

        if extended:
            tom.set_vertipaq_annotations()

        return tom, dep


def capture_violations(tom, dep, rules):

    violations = pd.DataFrame(columns=["Object Name", "Scope", "Rule Name"])
    rules_df = pd.DataFrame(
        rules,
        columns=[
            "Category",
            "Scope",
            "Severity",
            "Name",
            "Expression",
            "Description",
            "URL",
        ],
    )

    scope_to_dataframe = {
        "Relationship": (
            tom.model.Relationships,
            lambda obj: create_relationship_name(
                obj.FromTable.Name,
                obj.FromColumn.Name,
                obj.ToTable.Name,
                obj.ToColumn.Name,
            ),
        ),
        "Column": (
            tom.all_columns(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Calculated Column": (
            tom.all_calculated_columns(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Measure": (tom.all_measures(), lambda obj: obj.Name),
        "Hierarchy": (
            tom.all_hierarchies(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Table": (tom.model.Tables, lambda obj: obj.Name),
        "Calculated Table": (tom.all_calculated_tables(), lambda obj: obj.Name),
        "Role": (tom.model.Roles, lambda obj: obj.Name),
        "Model": (tom.model, lambda obj: obj.Model.Name),
        "Calculation Item": (
            tom.all_calculation_items(),
            lambda obj: format_dax_object_name(obj.Parent.Table.Name, obj.Name),
        ),
        "Row Level Security": (
            tom.all_rls(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Partition": (
            tom.all_partitions(),
            lambda obj: format_dax_object_name(obj.Parent.Name, obj.Name),
        ),
        "Function": (
            tom.all_functions(),
            lambda obj: obj.Name,
        ),
    }

    import sempy

    sempy.fabric._client._utils._init_analysis_services()
    import Microsoft.AnalysisServices.Tabular as TOM

    rows = []
    for rule in rules:
        ruleName = rule.get("Name")
        expr_text = rule.get("Expression")
        scopes = rule.get("Scope")
        expr = eval(f"lambda obj, tom, dep, TOM, re: ({expr_text})")

        if isinstance(scopes, str):
            scopes = [scopes]

        for scope in scopes:
            func = scope_to_dataframe[scope][0]
            nm = scope_to_dataframe[scope][1]

            if scope == "Model":
                x = []
                if expr(func, tom, dep, TOM, re):
                    x = ["Model"]
            elif scope == "Measure":
                x = [
                    nm(obj)
                    for obj in tom.all_measures()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Function":
                x = [
                    nm(obj)
                    for obj in tom.all_functions()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Column":
                x = [
                    nm(obj) for obj in tom.all_columns() if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Partition":
                x = [
                    nm(obj)
                    for obj in tom.all_partitions()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Hierarchy":
                x = [
                    nm(obj)
                    for obj in tom.all_hierarchies()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Table":
                x = [
                    nm(obj) for obj in tom.model.Tables if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Calculated Table":
                x = [
                    nm(obj)
                    for obj in tom.all_calculated_tables()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Relationship":
                x = [
                    nm(obj)
                    for obj in tom.model.Relationships
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Role":
                x = [nm(obj) for obj in tom.model.Roles if expr(obj, tom, dep, TOM, re)]
            elif scope == "Row Level Security":
                x = [nm(obj) for obj in tom.all_rls() if expr(obj, tom, dep, TOM, re)]
            elif scope == "Calculation Item":
                x = [
                    nm(obj)
                    for obj in tom.all_calculation_items()
                    if expr(obj, tom, dep, TOM, re)
                ]
            elif scope == "Calculated Column":
                x = [
                    nm(obj)
                    for obj in tom.all_calculated_columns()
                    if expr(obj, tom, dep, TOM, re)
                ]

            if len(x) > 0:
                rows.append(
                    {
                        "Object Name": x,
                        "Scope": scope,
                        "Rule Name": ruleName,
                    }
                )

    if rows:
        violations = pd.DataFrame(rows, columns=["Object Name", "Scope", "Rule Name"])
        violations = violations.explode("Object Name", ignore_index=True)

    prepDF = pd.merge(
        violations,
        rules_df[["Name", "Category", "Severity", "Description", "URL"]],
        left_on="Rule Name",
        right_on="Name",
        how="left",
    )

    prepDF.rename(columns={"Scope": "Object Type"}, inplace=True)

    prepDF["Object Type"] = (
        prepDF["Object Type"].map(_plural_map).fillna(prepDF["Object Type"])
    )
    finalDF = prepDF[
        [
            "Category",
            "Rule Name",
            "Severity",
            "Object Type",
            "Object Name",
            "Description",
            "URL",
        ]
    ]

    finalDF = (
        finalDF[
            [
                "Category",
                "Rule Name",
                "Object Type",
                "Object Name",
                "Severity",
                "Description",
                "URL",
            ]
        ]
        .sort_values(["Category", "Rule Name", "Object Type", "Object Name"])
        .set_index(["Category", "Rule Name"])
    )

    violations_df = finalDF.reset_index()

    return violations_df


# ==========================================================================
#  HTML rendering helpers
# ==========================================================================


def _build_results_html(
    categories_data,
    all_rule_names,
    all_object_types,
    all_severities,
    total_violations,
    dataset_name,
):
    """Build the results view HTML string with embedded JS for tabs/filters/collapsible groups."""

    data_json = json.dumps(categories_data)
    rule_names_json = json.dumps(all_rule_names)
    object_types_json = json.dumps(all_object_types)
    severities_json = json.dumps(all_severities)
    severity_label_json = json.dumps(icons.severity_mapping)
    obj_type_icons_json = json.dumps(OBJ_TYPE_ICONS)
    dataset_name_json = json.dumps(dataset_name)
    uid = f"bpar{id(categories_data)}"

    return f"""
    <style>
        .{uid} {{
            font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
            color: #1d1d1f; max-width: 100%;
            -webkit-font-smoothing: antialiased; -moz-osx-font-smoothing: grayscale;
        }}
        .{uid} .bpar-header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:20px; padding-bottom:16px; border-bottom:1px solid #e8e8ed; }}
        .{uid} .bpar-header-left {{ display:flex; align-items:center; gap:12px; }}
        .{uid} .bpar-header-icon {{ width:36px; height:36px; background:linear-gradient(135deg,#0071e3 0%,#40a9ff 100%); border-radius:10px; display:flex; align-items:center; justify-content:center; }}
        .{uid} .bpar-header-icon svg {{ width:20px; height:20px; stroke:#fff; }}
        .{uid} .bpar-header h2 {{ margin:0; font-size:20px; font-weight:700; color:#1d1d1f; }}
        .{uid} .bpar-header-sub {{ font-size:13px; color:#86868b; margin:2px 0 0 0; }}
        .{uid} .bpar-summary {{ display:flex; gap:12px; margin-bottom:20px; flex-wrap:wrap; }}
        .{uid} .bpar-card {{ flex:1; min-width:120px; background:#fff; border:1px solid #e8e8ed; border-radius:12px; padding:16px 20px; text-align:center; transition:box-shadow 0.2s; }}
        .{uid} .bpar-card:hover {{ box-shadow:0 2px 12px rgba(0,0,0,0.06); }}
        .{uid} .bpar-card-value {{ font-size:28px; font-weight:700; color:#1d1d1f; line-height:1; }}
        .{uid} .bpar-card-label {{ font-size:11px; font-weight:600; color:#86868b; text-transform:uppercase; letter-spacing:0.5px; margin-top:6px; }}
        .{uid} .bpar-card.warn .bpar-card-value {{ color:#ff9500; }}
        .{uid} .bpar-card.err .bpar-card-value {{ color:#ff3b30; }}
        .{uid} .bpar-card.info .bpar-card-value {{ color:#007aff; }}
        .{uid} .bpar-tabs {{ display:flex; gap:6px; padding:4px; background:#f5f5f7; border-radius:12px; margin-bottom:16px; flex-wrap:wrap; }}
        .{uid} .bpar-tab {{ display:flex; align-items:center; gap:10px; padding:10px 18px; border:none; border-radius:10px; background:transparent; cursor:pointer; font-size:13px; font-weight:500; color:#6e6e73; transition:all 0.25s ease; text-align:left; line-height:1.4; white-space:nowrap; font-family:inherit; }}
        .{uid} .bpar-tab:hover {{ background:rgba(0,0,0,0.04); }}
        .{uid} .bpar-tab.active {{ background:#fff; color:#1d1d1f; box-shadow:0 1px 4px rgba(0,0,0,0.08); font-weight:600; }}
        .{uid} .bpar-tab .tab-count {{ font-size:11px; font-weight:400; color:#86868b; display:block; margin-top:2px; }}
        .{uid} .bpar-tab-text {{ display:flex; flex-direction:column; }}
        .{uid} .bpar-cat-icon {{ width:20px; height:20px; flex-shrink:0; opacity:0.55; }}
        .{uid} .bpar-tab.active .bpar-cat-icon {{ opacity:1; color:#0071e3; }}
        .{uid} .bpar-filters {{ display:flex; gap:10px; margin-bottom:14px; flex-wrap:wrap; align-items:center; }}
        .{uid} .bpar-filter-label {{ font-size:12px; font-weight:600; color:#86868b; text-transform:uppercase; letter-spacing:0.5px; margin-right:2px; }}
        .{uid} .bpar-select {{ padding:7px 30px 7px 12px; border:1px solid #d2d2d7; border-radius:8px; background:#fff; font-size:13px; color:#1d1d1f; font-family:inherit; appearance:none; -webkit-appearance:none; background-image:url("data:image/svg+xml,%3Csvg width='10' height='6' viewBox='0 0 10 6' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M1 1L5 5L9 1' stroke='%2386868b' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E"); background-repeat:no-repeat; background-position:right 10px center; cursor:pointer; transition:border-color 0.2s; min-width:140px; }}
        .{uid} .bpar-select:hover {{ border-color:#0071e3; }}
        .{uid} .bpar-select:focus {{ outline:none; border-color:#0071e3; box-shadow:0 0 0 3px rgba(0,113,227,0.15); }}
        .{uid} .bpar-sev-dd {{ position:relative; display:inline-block; min-width:140px; }}
        .{uid} .bpar-ot-dd {{ position:relative; display:inline-block; min-width:160px; }}
        .{uid} .bpar-sev-toggle, .{uid} .bpar-ot-toggle {{ padding:7px 30px 7px 12px; border:1px solid #d2d2d7; border-radius:8px; background:#fff; font-size:13px; color:#1d1d1f; cursor:pointer; transition:border-color 0.2s; box-sizing:border-box; user-select:none; white-space:nowrap; background-image:url("data:image/svg+xml,%3Csvg width='10' height='6' viewBox='0 0 10 6' fill='none' xmlns='http://www.w3.org/2000/svg'%3E%3Cpath d='M1 1L5 5L9 1' stroke='%2386868b' stroke-width='1.5' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E"); background-repeat:no-repeat; background-position:right 10px center; }}
        .{uid} .bpar-sev-toggle {{ min-width:140px; }}
        .{uid} .bpar-ot-toggle {{ min-width:160px; }}
        .{uid} .bpar-sev-toggle:hover, .{uid} .bpar-ot-toggle:hover {{ border-color:#0071e3; }}
        .{uid} .bpar-sev-dd.open .bpar-sev-toggle, .{uid} .bpar-ot-dd.open .bpar-ot-toggle {{ border-color:#0071e3; box-shadow:0 0 0 3px rgba(0,113,227,0.15); }}
        .{uid} .bpar-sev-menu, .{uid} .bpar-ot-menu {{ display:none; position:absolute; top:100%; left:0; right:0; margin-top:2px; background:#fff; border:1px solid #d2d2d7; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.1); z-index:100; padding:4px 0; }}
        .{uid} .bpar-sev-dd.open .bpar-sev-menu, .{uid} .bpar-ot-dd.open .bpar-ot-menu {{ display:block; }}
        .{uid} .bpar-sev-item, .{uid} .bpar-ot-item {{ padding:6px 12px; cursor:pointer; font-size:13px; color:#1d1d1f; white-space:nowrap; display:flex; align-items:center; }}
        .{uid} .bpar-sev-item:hover, .{uid} .bpar-ot-item:hover {{ background:#f0f0f5; }}
        .{uid} .bpar-sev-item.selected, .{uid} .bpar-ot-item.selected {{ background:#e8f0fe; }}
        .{uid} .bpar-sev-icon, .{uid} .bpar-ot-icon {{ display:inline-block; width:1.2em; text-align:center; flex-shrink:0; }}
        .{uid} .bpar-sev-label, .{uid} .bpar-ot-label {{ margin-left:6px; }}
        .{uid} .bpar-rule-group {{ background:#fff; border:1px solid #e8e8ed; border-radius:12px; margin-bottom:10px; overflow:hidden; transition:box-shadow 0.2s ease; }}
        .{uid} .bpar-rule-group:hover {{ box-shadow:0 2px 12px rgba(0,0,0,0.06); }}
        .{uid} .bpar-rule-hdr {{ display:flex; align-items:center; padding:14px 18px; cursor:pointer; user-select:none; gap:12px; transition:background 0.15s; }}
        .{uid} .bpar-rule-hdr:hover {{ background:#fafafa; }}
        .{uid} .bpar-chevron {{ width:18px; height:18px; flex-shrink:0; transition:transform 0.25s ease; color:#86868b; }}
        .{uid} .bpar-rule-group.open .bpar-chevron {{ transform:rotate(90deg); }}
        .{uid} .bpar-rule-title {{ font-size:14px; font-weight:600; color:#1d1d1f; flex:1; }}
        .{uid} .bpar-rule-title a {{ color:#0071e3; text-decoration:none; }}
        .{uid} .bpar-rule-title a:hover {{ text-decoration:underline; }}
        .{uid} .bpar-rule-badge {{ font-size:11px; font-weight:600; padding:2px 10px; border-radius:20px; background:#f5f5f7; color:#6e6e73; white-space:nowrap; min-width:62px; text-align:center; }}
        .{uid} .bpar-rule-sev {{ font-size:14px; flex-shrink:0; width:1.4em; text-align:center; }}
        .{uid} .bpar-rule-desc {{ padding:0 18px 8px 48px; font-size:12px; color:#86868b; line-height:1.5; display:none; }}
        .{uid} .bpar-rule-group.open .bpar-rule-desc {{ display:block; }}
        .{uid} .bpar-rule-body {{ display:none; border-top:1px solid #f0f0f5; }}
        .{uid} .bpar-rule-group.open .bpar-rule-body {{ display:block; }}
        .{uid} .bpar-otype {{ border-top:1px solid #f0f0f5; }}
        .{uid} .bpar-otype-hdr {{ display:flex; align-items:center; padding:10px 18px 10px 36px; cursor:pointer; user-select:none; gap:10px; font-size:13px; font-weight:500; color:#6e6e73; transition:background 0.15s; }}
        .{uid} .bpar-otype-hdr:hover {{ background:#fafafa; }}
        .{uid} .bpar-otype-chev {{ width:14px; height:14px; flex-shrink:0; transition:transform 0.25s ease; color:#aeaeb2; }}
        .{uid} .bpar-otype.open .bpar-otype-chev {{ transform:rotate(90deg); }}
        .{uid} .bpar-otype-badge {{ font-size:11px; padding:1px 8px; border-radius:20px; background:#f0f0f5; color:#86868b; }}
        .{uid} .bpar-obj-list {{ display:none; padding:0 18px 8px 60px; }}
        .{uid} .bpar-otype.open .bpar-obj-list {{ display:block; }}
        .{uid} .bpar-obj-item {{ padding:6px 0; font-size:13px; color:#424245; border-bottom:1px solid #f5f5f7; font-family:"SF Mono",SFMono-Regular,Menlo,Consolas,monospace; }}
        .{uid} .bpar-obj-item:last-child {{ border-bottom:none; }}
        .{uid} .bpar-empty {{ text-align:center; padding:40px 20px; color:#86868b; font-size:14px; }}
        .{uid} .bpar-empty svg {{ width:48px; height:48px; stroke:#34c759; margin-bottom:12px; }}
        .{uid} .bpar-empty-title {{ font-size:16px; font-weight:600; color:#1d1d1f; margin-bottom:4px; }}
    </style>
    <div class="{uid}">
        <div class="bpar-header">
            <div class="bpar-header-left">
                <div class="bpar-header-icon">
                    <svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
                </div>
                <div>
                    <h2>BPA Results</h2>
                    <p class="bpar-header-sub">{dataset_name} &mdash; {total_violations} violation{"s" if total_violations != 1 else ""} found</p>
                </div>
            </div>
        </div>
        <div class="bpar-summary" id="{uid}_summary"></div>
        <div class="bpar-tabs" id="{uid}_tabs"></div>
        <div class="bpar-filters" id="{uid}_filters">
            <span class="bpar-filter-label">Filters</span>
            <select class="bpar-select" id="{uid}_fRule"><option value="">All Rules</option></select>
            <div class="bpar-ot-dd" id="{uid}_otDD">
                <div class="bpar-ot-toggle" id="{uid}_otToggle">All Object Types</div>
                <div class="bpar-ot-menu" id="{uid}_otMenu"></div>
            </div>
            <div class="bpar-sev-dd" id="{uid}_sevDD">
                <div class="bpar-sev-toggle" id="{uid}_sevToggle">All Severities</div>
                <div class="bpar-sev-menu" id="{uid}_sevMenu"></div>
            </div>
        </div>
        <div id="{uid}_content"></div>
    </div>
    <script>
    (function() {{
        var U = {json.dumps(uid)};
        var DATA = {data_json};
        var RULE_NAMES = {rule_names_json};
        var OBJ_TYPES = {object_types_json};
        var SEVERITIES = {severities_json};
        var SEV_LABELS = {severity_label_json};
        var OT_ICONS = {obj_type_icons_json};
        var activeTab = null;
        function $(id) {{ return document.getElementById(id); }}
        var summaryEl = $(U+'_summary');
        var totalV = 0;
        Object.keys(DATA).forEach(function(c) {{ totalV += DATA[c].length; }});
        var wc=0, ec=0, ic=0;
        Object.keys(DATA).forEach(function(c) {{
            DATA[c].forEach(function(r) {{
                var sl = (SEV_LABELS[r.severity]||'').toLowerCase();
                if (sl==='warning') wc++; else if (sl==='error') ec++; else if (sl==='info') ic++;
            }});
        }});
        [{{v:totalV,l:'Total Violations',c:''}},{{v:ec,l:'Errors',c:'err'}},{{v:wc,l:'Warnings',c:'warn'}},{{v:ic,l:'Info',c:'info'}}].forEach(function(x) {{
            var d=document.createElement('div'); d.className='bpar-card'+(x.c?' '+x.c:'');
            d.innerHTML='<div class="bpar-card-value">'+x.v+'</div><div class="bpar-card-label">'+x.l+'</div>';
            summaryEl.appendChild(d);
        }});
        var fRule=$(U+'_fRule'), sevDD=$(U+'_sevDD'), sevToggle=$(U+'_sevToggle'), sevMenu=$(U+'_sevMenu'), filterSevVal='';
        var otDD=$(U+'_otDD'), otToggle=$(U+'_otToggle'), otMenu=$(U+'_otMenu'), filterOtVal='';
        RULE_NAMES.forEach(function(r) {{ var o=document.createElement('option'); o.value=r; o.textContent=r; fRule.appendChild(o); }});
        var otAll=document.createElement('div'); otAll.className='bpar-ot-item selected'; otAll.setAttribute('data-value',''); otAll.textContent='All Object Types'; otMenu.appendChild(otAll);
        OBJ_TYPES.forEach(function(t) {{
            var it=document.createElement('div'); it.className='bpar-ot-item'; it.setAttribute('data-value',t);
            var ic3=document.createElement('span'); ic3.className='bpar-ot-icon'; ic3.textContent=OT_ICONS[t]||'';
            var lb2=document.createElement('span'); lb2.className='bpar-ot-label'; lb2.textContent=t;
            it.appendChild(ic3); it.appendChild(lb2); otMenu.appendChild(it);
        }});
        otToggle.addEventListener('click', function(e) {{ e.stopPropagation(); otDD.classList.toggle('open'); sevDD.classList.remove('open'); }});
        otMenu.addEventListener('click', function(e) {{
            var it=e.target.closest('.bpar-ot-item'); if(!it) return;
            var val=it.getAttribute('data-value'); filterOtVal=val;
            otMenu.querySelectorAll('.bpar-ot-item').forEach(function(el) {{ el.classList.remove('selected'); }});
            it.classList.add('selected');
            if(val==='') {{ otToggle.textContent='All Object Types'; }}
            else {{ otToggle.textContent=''; var tI2=document.createElement('span'); tI2.className='bpar-ot-icon'; tI2.textContent=OT_ICONS[val]||''; var tL2=document.createElement('span'); tL2.className='bpar-ot-label'; tL2.textContent=val; otToggle.appendChild(tI2); otToggle.appendChild(tL2); }}
            otDD.classList.remove('open'); render();
        }});
        document.addEventListener('click', function() {{ otDD.classList.remove('open'); }});
        var sa=document.createElement('div'); sa.className='bpar-sev-item selected'; sa.setAttribute('data-value',''); sa.textContent='All Severities'; sevMenu.appendChild(sa);
        SEVERITIES.forEach(function(s) {{
            var it=document.createElement('div'); it.className='bpar-sev-item'; it.setAttribute('data-value',s);
            var ic2=document.createElement('span'); ic2.className='bpar-sev-icon'; ic2.textContent=s;
            var lb=document.createElement('span'); lb.className='bpar-sev-label'; lb.textContent=SEV_LABELS[s]||s;
            it.appendChild(ic2); it.appendChild(lb); sevMenu.appendChild(it);
        }});
        sevToggle.addEventListener('click', function(e) {{ e.stopPropagation(); sevDD.classList.toggle('open'); otDD.classList.remove('open'); }});
        document.addEventListener('click', function() {{ sevDD.classList.remove('open'); }});
        sevMenu.addEventListener('click', function(e) {{
            var it=e.target.closest('.bpar-sev-item'); if(!it) return;
            var val=it.getAttribute('data-value'); filterSevVal=val;
            sevMenu.querySelectorAll('.bpar-sev-item').forEach(function(el) {{ el.classList.remove('selected'); }});
            it.classList.add('selected');
            if(val==='') {{ sevToggle.textContent='All Severities'; }}
            else {{ sevToggle.textContent=''; var tI=document.createElement('span'); tI.className='bpar-sev-icon'; tI.textContent=val; var tL=document.createElement('span'); tL.className='bpar-sev-label'; tL.textContent=SEV_LABELS[val]||val; sevToggle.appendChild(tI); sevToggle.appendChild(tL); }}
            sevDD.classList.remove('open'); render();
        }});
        fRule.addEventListener('change', render);
        var CAT_ICONS = {{
            'Performance':'<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>',
            'Formatting':'<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>',
            'Maintenance':'<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>',
            'DAX Expressions':'<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><polyline points="16 18 22 12 16 6"/><polyline points="8 6 2 12 8 18"/><line x1="14" y1="4" x2="10" y2="20"/></svg>',
            'Error Prevention':'<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>',
            'Naming Conventions':'<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M4 7V4h16v3"/><line x1="9" y1="20" x2="15" y2="20"/><line x1="12" y1="4" x2="12" y2="20"/></svg>'
        }};
        var DEFAULT_ICON='<svg class="bpar-cat-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>';
        var tabsEl=$(U+'_tabs');
        var cats=Object.keys(DATA);
        if(cats.length===0) {{
            $(U+'_content').innerHTML='<div class="bpar-empty"><svg viewBox="0 0 24 24" fill="none" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg><div class="bpar-empty-title">No violations found</div><div>Your semantic model follows all best practice rules.</div></div>';
            return;
        }}
        cats.forEach(function(cat, idx) {{
            var btn=document.createElement('button'); btn.className='bpar-tab'+(idx===0?' active':'');
            var icon=CAT_ICONS[cat]||DEFAULT_ICON;
            btn.innerHTML=icon+'<span class="bpar-tab-text"><strong>'+cat+'</strong><span class="tab-count">'+DATA[cat].length+' violation'+(DATA[cat].length!==1?'s':'')+'</span></span>';
            btn.addEventListener('click', function() {{
                tabsEl.querySelectorAll('.bpar-tab').forEach(function(b) {{ b.classList.remove('active'); }});
                btn.classList.add('active'); activeTab=cat; render();
            }});
            tabsEl.appendChild(btn);
        }});
        activeTab=cats[0];
        var chevSvg='<svg class="bpar-chevron" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="7 4 13 10 7 16"/></svg>';
        var chevSm='<svg class="bpar-otype-chev" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="7 4 13 10 7 16"/></svg>';
        function render() {{
            var ct=$(U+'_content'); ct.innerHTML='';
            if(!activeTab||!DATA[activeTab]) return;
            var fr=fRule.value, ft=filterOtVal, fs=filterSevVal;
            var rows=DATA[activeTab].filter(function(r) {{
                if(fr&&r.rule!==fr) return false; if(ft&&r.objectType!==ft) return false; if(fs&&r.severity!==fs) return false; return true;
            }});
            if(rows.length===0) {{ ct.innerHTML='<div class="bpar-empty">No violations match the current filters.</div>'; return; }}
            var ruleMap={{}};
            rows.forEach(function(r) {{ if(!ruleMap[r.rule]) ruleMap[r.rule]=[]; ruleMap[r.rule].push(r); }});
            Object.keys(ruleMap).sort().forEach(function(rule) {{
                var items=ruleMap[rule]; var first=items[0];
                var g=document.createElement('div'); g.className='bpar-rule-group';
                var hdr=document.createElement('div'); hdr.className='bpar-rule-hdr';
                var titleHtml=first.url?'<a href="'+first.url+'" target="_blank">'+rule+'</a>':rule;
                hdr.innerHTML=chevSvg+'<span class="bpar-rule-title">'+titleHtml+'</span><span class="bpar-rule-badge">'+items.length+' object'+(items.length!==1?'s':'')+'</span><span class="bpar-rule-sev">'+first.severity+'</span>';
                hdr.addEventListener('click', function() {{ g.classList.toggle('open'); }});
                g.appendChild(hdr);
                if(first.description) {{ var desc=document.createElement('div'); desc.className='bpar-rule-desc'; desc.textContent=first.description; g.appendChild(desc); }}
                var body=document.createElement('div'); body.className='bpar-rule-body';
                var typeMap={{}};
                items.forEach(function(it) {{ if(!typeMap[it.objectType]) typeMap[it.objectType]=[]; typeMap[it.objectType].push(it); }});
                Object.keys(typeMap).sort().forEach(function(ot) {{
                    var objs=typeMap[ot]; var otg=document.createElement('div'); otg.className='bpar-otype';
                    var oth=document.createElement('div'); oth.className='bpar-otype-hdr';
                    oth.innerHTML=chevSm+'<span>'+ot+'</span><span class="bpar-otype-badge">'+objs.length+'</span>';
                    oth.addEventListener('click', function(e) {{ e.stopPropagation(); otg.classList.toggle('open'); }});
                    otg.appendChild(oth);
                    var list=document.createElement('div'); list.className='bpar-obj-list';
                    objs.forEach(function(o) {{ var item=document.createElement('div'); item.className='bpar-obj-item'; item.textContent=o.objectName; list.appendChild(item); }});
                    otg.appendChild(list); body.appendChild(otg);
                }});
                g.appendChild(body); ct.appendChild(g);
            }});
        }}
        render();
    }})();
    </script>
    """


def _build_editor_html(rules_for_editor, uid):
    """Build the rule editor HTML with embedded JS for add/edit/delete/save."""

    all_rules_json = json.dumps(rules_for_editor)

    return f"""
    <style>
        .{uid} {{
            font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", Arial, sans-serif;
            color: #1d1d1f; max-width: 100%;
            -webkit-font-smoothing: antialiased;
        }}
        .{uid} .ed-top {{ display:flex; align-items:center; justify-content:space-between; padding:18px 0; border-bottom:1px solid #e8e8ed; margin-bottom:16px; }}
        .{uid} .ed-top h2 {{ margin:0; font-size:20px; font-weight:700; color:#1d1d1f; }}
        .{uid} .ed-top-actions {{ display:flex; gap:8px; align-items:center; }}
        .{uid} .ed-btn {{ padding:6px 14px; border:1px solid #d2d2d7; border-radius:8px; background:#fff; font-size:12px; cursor:pointer; transition:all 0.15s; color:#1d1d1f; font-family:inherit; font-weight:500; }}
        .{uid} .ed-btn:hover {{ border-color:#0071e3; color:#0071e3; }}
        .{uid} .ed-btn.danger {{ color:#ff3b30; }}
        .{uid} .ed-btn.danger:hover {{ border-color:#ff3b30; background:#fff5f5; }}
        .{uid} .ed-btn.primary {{ background:#0071e3; color:#fff; border-color:#0071e3; }}
        .{uid} .ed-btn.primary:hover {{ background:#0077ed; }}
        .{uid} .ed-btn.add {{ background:#fff; color:#34c759; border-color:#34c759; font-weight:600; }}
        .{uid} .ed-btn.add:hover {{ background:#f0fff4; }}
        .{uid} .ed-table {{ width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed; }}
        .{uid} .ed-table thead {{ position:sticky; top:0; z-index:2; }}
        .{uid} .ed-table th {{ text-align:left; padding:10px 12px; font-weight:600; color:#86868b; font-size:11px; text-transform:uppercase; letter-spacing:0.4px; border-bottom:2px solid #e8e8ed; white-space:nowrap; background:#fff; }}
        .{uid} .ed-table td {{ padding:10px 12px; border-bottom:1px solid #f0f0f5; vertical-align:middle; color:#1d1d1f; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; text-align:left; }}
        .{uid} .ed-table tr:hover td {{ background:#fafafa; }}
        .{uid} .ed-act {{ white-space:nowrap; display:flex; gap:6px; }}
        .{uid} .save-status {{ font-size:12px; padding:6px 14px; border-radius:8px; display:none; align-items:center; gap:6px; }}
        .{uid} .save-status.success {{ display:inline-flex; background:#f0fff4; color:#34c759; border:1px solid #34c759; }}
        .{uid} .form-overlay {{ display:none; position:fixed; top:0; left:0; right:0; bottom:0; background:rgba(0,0,0,0.35); backdrop-filter:blur(2px); -webkit-backdrop-filter:blur(2px); z-index:10001; justify-content:center; align-items:center; padding:20px; }}
        .{uid} .form-overlay.open {{ display:flex; }}
        .{uid} .form-panel {{ background:#fff; border-radius:16px; box-shadow:0 12px 48px rgba(0,0,0,0.2); width:100%; max-width:640px; max-height:90vh; overflow-y:auto; padding:28px; }}
        .{uid} .form-panel h3 {{ margin:0 0 20px 0; font-size:18px; font-weight:700; color:#1d1d1f; }}
        .{uid} .fg {{ margin-bottom:16px; }}
        .{uid} .fg label {{ display:block; font-size:12px; font-weight:600; color:#6e6e73; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.4px; }}
        .{uid} .fg .req {{ color:#ff3b30; margin-left:2px; }}
        .{uid} .fg input, .{uid} .fg textarea, .{uid} .fg select {{ width:100%; padding:10px 14px; border:1.5px solid #d2d2d7; border-radius:10px; font-size:14px; font-family:inherit; color:#1d1d1f; box-sizing:border-box; transition:border-color 0.2s, box-shadow 0.2s; }}
        .{uid} .fg input:focus, .{uid} .fg textarea:focus, .{uid} .fg select:focus {{ outline:none; border-color:#0071e3; box-shadow:0 0 0 3px rgba(0,113,227,0.12); }}
        .{uid} .fg textarea {{ min-height:80px; resize:vertical; font-family:"SF Mono",SFMono-Regular,Menlo,Consolas,monospace; font-size:13px; }}
        .{uid} .fg .ferr {{ color:#ff3b30; font-size:11px; margin-top:4px; display:none; font-weight:500; }}
        .{uid} .form-actions {{ display:flex; gap:10px; justify-content:flex-end; margin-top:24px; }}
        .{uid} .ed-back {{ display:inline-flex; align-items:center; gap:6px; padding:8px 16px; border:1.5px solid #d2d2d7; border-radius:10px; background:#fff; font-size:13px; font-weight:500; color:#1d1d1f; cursor:pointer; transition:all 0.2s ease; font-family:inherit; margin-bottom:12px; }}
        .{uid} .ed-back:hover {{ border-color:#0071e3; color:#0071e3; background:#f5f8ff; }}
        .{uid} .ed-back svg {{ width:14px; height:14px; stroke:currentColor; }}
    </style>
    <div class="{uid}">
        <button class="ed-back" id="{uid}_backBtn">
            <svg viewBox="0 0 24 24" fill="none" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="15 18 9 12 15 6"/></svg>
            Back
        </button>
        <div class="ed-top">
            <h2>Rule Editor</h2>
            <div class="ed-top-actions">
                <span class="save-status" id="{uid}_saveStatus"></span>
                <button class="ed-btn add" id="{uid}_addBtn">+ Add Rule</button>
                <button class="ed-btn primary" id="{uid}_saveBtn">
                    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
                    Save Rules
                </button>
            </div>
        </div>
        <div style="overflow-y:auto; max-height:65vh;">
            <table class="ed-table">
                <thead><tr>
                    <th style="width:5%">#</th>
                    <th style="width:22%">Name</th>
                    <th style="width:12%">Category</th>
                    <th style="width:12%">Scope</th>
                    <th style="width:8%">Severity</th>
                    <th style="width:26%">Expression</th>
                    <th style="width:15%">Actions</th>
                </tr></thead>
                <tbody id="{uid}_tbody"></tbody>
            </table>
        </div>
        <div class="form-overlay" id="{uid}_formOverlay">
            <div class="form-panel">
                <h3 id="{uid}_formTitle">Edit Rule</h3>
                <input type="hidden" id="{uid}_formIdx" value="-1"/>
                <div class="fg"><label>Name <span class="req">*</span></label><input type="text" id="{uid}_fName" placeholder="Rule name"/><div class="ferr" id="{uid}_fNameErr">Name is required.</div></div>
                <div class="fg"><label>Category <span class="req">*</span></label><input type="text" id="{uid}_fCat" placeholder="e.g. Performance"/><div class="ferr" id="{uid}_fCatErr">Category is required.</div></div>
                <div class="fg"><label>Scope <span class="req">*</span> <span style="font-weight:400;text-transform:none;letter-spacing:0">(comma-separated)</span></label><input type="text" id="{uid}_fScope" placeholder="e.g. Column, Measure"/><div class="ferr" id="{uid}_fScopeErr">Scope is required.</div></div>
                <div class="fg"><label>Severity <span class="req">*</span></label><select id="{uid}_fSev"><option value="">-- Select --</option><option value="Warning">Warning</option><option value="Error">Error</option><option value="Info">Info</option></select><div class="ferr" id="{uid}_fSevErr">Severity is required.</div></div>
                <div class="fg"><label>Expression <span class="req">*</span></label><textarea id="{uid}_fExpr" placeholder="Python expression..."></textarea><div class="ferr" id="{uid}_fExprErr">Expression is required.</div></div>
                <div class="fg"><label>Description</label><textarea id="{uid}_fDesc" placeholder="Optional description..." style="font-family:inherit;font-size:14px;"></textarea></div>
                <div class="fg"><label>URL</label><input type="text" id="{uid}_fURL" placeholder="Optional reference URL"/></div>
                <div class="form-actions">
                    <button class="ed-btn" id="{uid}_fCancel">Cancel</button>
                    <button class="ed-btn primary" id="{uid}_fSave">Save</button>
                </div>
            </div>
        </div>
    </div>
    <script>
    (function() {{
        var U={json.dumps(uid)};
        var ALL_RULES={all_rules_json};
        function $(id) {{ return document.getElementById(id); }}
        function esc(s) {{ if(!s) return ''; var d=document.createElement('div'); d.textContent=s; return d.innerHTML; }}
        var tbody=$(U+'_tbody'), saveStatus=$(U+'_saveStatus'), formOverlay=$(U+'_formOverlay');
        function renderTable() {{
            tbody.innerHTML='';
            ALL_RULES.forEach(function(rule, idx) {{
                var tr=document.createElement('tr');
                tr.innerHTML='<td style="color:#86868b;font-size:11px;">'+(idx+1)+'</td>'+'<td title="'+esc(rule.Name)+'">'+esc(rule.Name)+'</td>'+'<td>'+esc(rule.Category)+'</td>'+'<td>'+esc(rule.Scope)+'</td>'+'<td>'+esc(rule.Severity)+'</td>'+'<td title="'+esc(rule.Expression)+'">'+esc(rule.Expression).substring(0,45)+(rule.Expression.length>45?'\u2026':'')+'</td>'+'<td class="ed-act"></td>';
                var act=tr.querySelector('.ed-act');
                var eb=document.createElement('button'); eb.className='ed-btn'; eb.textContent='Edit';
                eb.addEventListener('click',(function(i){{return function(){{openForm(i);}}}})(idx));
                var db=document.createElement('button'); db.className='ed-btn danger'; db.textContent='Delete';
                db.addEventListener('click',(function(i){{return function(){{deleteRule(i);}}}})(idx));
                act.appendChild(eb); act.appendChild(db); tbody.appendChild(tr);
            }});
        }}
        function deleteRule(idx) {{
            var rows=tbody.querySelectorAll('tr'); if(!rows[idx]) return;
            var act=rows[idx].querySelector('td:last-child');
            if(act.querySelector('.confirm')) return;
            var w=document.createElement('span'); w.className='confirm'; w.style.cssText='display:inline-flex;gap:6px;align-items:center;';
            var lbl=document.createElement('span'); lbl.textContent='Delete?'; lbl.style.cssText='color:#ff3b30;font-size:11px;font-weight:600;';
            var yb=document.createElement('button'); yb.className='ed-btn danger'; yb.textContent='Yes'; yb.style.cssText='padding:3px 10px;font-size:11px;';
            var nb=document.createElement('button'); nb.className='ed-btn'; nb.textContent='No'; nb.style.cssText='padding:3px 10px;font-size:11px;';
            w.appendChild(lbl); w.appendChild(yb); w.appendChild(nb); act.innerHTML=''; act.appendChild(w);
            yb.addEventListener('click', function() {{ ALL_RULES.splice(idx,1); renderTable(); }});
            nb.addEventListener('click', function() {{ renderTable(); }});
        }}
        function downloadRulesJson() {{
            var out=ALL_RULES.map(function(r) {{
                var sv=r.Scope; if(sv.indexOf(',')!==-1) sv=sv.split(',').map(function(s){{return s.trim();}});
                return {{Category:r.Category,Scope:sv,Severity:r.Severity,Name:r.Name,Expression:r.Expression,Description:r.Description||null,URL:r.URL||null}};
            }});
            var blob=new Blob([JSON.stringify(out,null,4)],{{type:'application/json'}});
            var a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='bpa_rules.json';
            document.body.appendChild(a); a.click(); document.body.removeChild(a);
            return 'bpa_rules.json';
        }}
        function openForm(idx) {{
            var title=$(U+'_formTitle');
            $(U+'_formIdx').value=idx;
            [U+'_fNameErr',U+'_fCatErr',U+'_fScopeErr',U+'_fSevErr',U+'_fExprErr'].forEach(function(id){{$(id).style.display='none';}});
            if(idx>=0) {{
                title.textContent='Edit Rule'; var r=ALL_RULES[idx];
                $(U+'_fName').value=r.Name||''; $(U+'_fCat').value=r.Category||''; $(U+'_fScope').value=r.Scope||''; $(U+'_fSev').value=r.Severity||''; $(U+'_fExpr').value=r.Expression||''; $(U+'_fDesc').value=r.Description||''; $(U+'_fURL').value=r.URL||'';
            }} else {{
                title.textContent='Add New Rule';
                [U+'_fName',U+'_fCat',U+'_fScope',U+'_fExpr',U+'_fDesc',U+'_fURL'].forEach(function(id){{$(id).value='';}});
                $(U+'_fSev').value='';
            }}
            formOverlay.classList.add('open');
        }}
        $(U+'_addBtn').addEventListener('click', function() {{ openForm(-1); }});
        $(U+'_fCancel').addEventListener('click', function() {{ formOverlay.classList.remove('open'); }});
        formOverlay.addEventListener('click', function(e) {{ if(e.target===formOverlay) formOverlay.classList.remove('open'); }});
        $(U+'_fSave').addEventListener('click', function() {{
            var idx=parseInt($(U+'_formIdx').value,10);
            var name=$(U+'_fName').value.trim(), cat=$(U+'_fCat').value.trim(), scope=$(U+'_fScope').value.trim(), sev=$(U+'_fSev').value.trim(), expr=$(U+'_fExpr').value.trim(), desc=$(U+'_fDesc').value.trim(), url=$(U+'_fURL').value.trim();
            var valid=true;
            if(!name){{$(U+'_fNameErr').style.display='block';valid=false;}} else {{$(U+'_fNameErr').style.display='none';}}
            if(!cat){{$(U+'_fCatErr').style.display='block';valid=false;}} else {{$(U+'_fCatErr').style.display='none';}}
            if(!scope){{$(U+'_fScopeErr').style.display='block';valid=false;}} else {{$(U+'_fScopeErr').style.display='none';}}
            if(!sev){{$(U+'_fSevErr').style.display='block';valid=false;}} else {{$(U+'_fSevErr').style.display='none';}}
            if(!expr){{$(U+'_fExprErr').style.display='block';valid=false;}} else {{$(U+'_fExprErr').style.display='none';}}
            if(!valid) return;
            var obj={{Name:name,Category:cat,Scope:scope,Severity:sev,Expression:expr,Description:desc||'',URL:url||''}};
            if(idx>=0) ALL_RULES[idx]=obj; else ALL_RULES.push(obj);
            renderTable(); formOverlay.classList.remove('open');
        }});
        $(U+'_saveBtn').addEventListener('click', function() {{
            var fn=downloadRulesJson();
            saveStatus.textContent='Downloaded '+fn; saveStatus.className='save-status success'; saveStatus.style.display='';
            setTimeout(function(){{saveStatus.className='save-status';saveStatus.style.display='none';}},4000);
        }});
        renderTable();
        // Back button: find hidden widget button and click it
        var backEl = $(U+'_backBtn');
        if (backEl) {{ backEl.addEventListener('click', function() {{
            var btn = document.querySelector('button[title="__ed_back__"]');
            if (btn) {{ btn.click(); return; }}
            var btns = document.querySelectorAll('button');
            for (var i = 0; i < btns.length; i++) {{
                if (btns[i].textContent.trim() === '__ed_back__') {{ btns[i].click(); return; }}
            }}
        }}); }}
    }})();
    </script>
    """


# ==========================================================================
#  Landing page HTML
# ==========================================================================


def _build_landing_html(dataset_name, num_rules):
    """Build the static landing page HTML (buttons are ipywidgets, not HTML)."""

    return f"""
    <div style="
        font-family: -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'SF Pro Text', 'Helvetica Neue', Arial, sans-serif;
        color: #1d1d1f;
        -webkit-font-smoothing: antialiased;
        display: flex; flex-direction: column; align-items: center; justify-content: center;
        padding: 60px 20px 20px; text-align: center;
    ">
        <div style="
            width: 64px; height: 64px;
            background: linear-gradient(135deg, #0071e3 0%, #40a9ff 100%);
            border-radius: 18px;
            display: flex; align-items: center; justify-content: center;
            margin-bottom: 20px;
            box-shadow: 0 4px 20px rgba(0,113,227,0.25);
        ">
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/><polyline points="9 12 11 14 15 10"/></svg>
        </div>
        <h2 style="font-size:26px; font-weight:700; margin:0 0 8px 0; letter-spacing:-0.3px;">Best Practice Analyzer</h2>
        <p style="font-size:15px; color:#6e6e73; margin:0 0 6px 0; max-width:460px; line-height:1.5;">
            Analyze your semantic model against best practice rules to identify performance issues, formatting problems, and more.
        </p>
        <span style="
            font-size:13px; color:#86868b; margin:0 0 20px 0;
            background:#f5f5f7; display:inline-block; padding:4px 14px;
            border-radius:20px; font-weight:500;
        ">{dataset_name}</span>
        <div style="
            display:flex; justify-content:center; gap:20px;
            margin-top:16px; padding-top:16px; border-top:1px solid #e8e8ed;
        ">
            <div style="text-align:center;">
                <div style="font-size:22px; font-weight:700; color:#1d1d1f;">{num_rules}</div>
                <div style="font-size:11px; font-weight:600; color:#86868b; text-transform:uppercase; letter-spacing:0.5px; margin-top:2px;">Rules Loaded</div>
            </div>
        </div>
    </div>
    """


# ==========================================================================
#  Main entry point
# ==========================================================================


def bpa(
    dataset: str | UUID,
    rules: Optional[List[dict]] = None,
    workspace: Optional[str | UUID] = None,
    check_dependencies: bool = True,
    extended: bool = False,
):
    import ipywidgets as widgets

    if rules is None:
        rules = [dict(r) for r in bpa_rules.rules]

    (tom, dep) = get_tom(dataset, workspace, check_dependencies, extended)

    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset, workspace=workspace_id
    )

    if tom.model.Tables.Count == 0:
        print(
            f"{icons.warning} The '{dataset_name}' semantic model within the "
            f"'{workspace_name}' workspace has no tables and therefore there "
            f"are no valid BPA results."
        )
        return

    # Mutable state shared across callbacks
    state = {
        "tom": tom,
        "dep": dep,
        "rules": [dict(r) for r in rules],
        "last_results": None,  # cached (cat_data, rn, ot, sev, total)
        "prev_view": "landing",  # where back button should return to
    }

    # -- Output area --
    output = widgets.Output()

    # -- Buttons --
    run_btn = widgets.Button(
        description="Run BPA",
        button_style="primary",
        icon="play",
        layout=widgets.Layout(
            width="auto",
            padding="10px 32px",
            border_radius="12px",
        ),
    )
    run_btn.style.font_weight = "600"

    edit_btn = widgets.Button(
        description="Edit Rules",
        button_style="",
        icon="pencil",
        layout=widgets.Layout(
            width="auto",
            padding="10px 24px",
            border_radius="12px",
        ),
    )

    import_btn = widgets.Button(
        description="Import Rules",
        button_style="",
        icon="upload",
        layout=widgets.Layout(
            width="auto",
            padding="10px 24px",
            border_radius="12px",
        ),
    )

    # File upload widget (hidden until import clicked)
    file_upload = widgets.FileUpload(
        accept=".json",
        multiple=False,
        layout=widgets.Layout(display="none"),
    )

    status_html = widgets.HTML(value="")

    # -- Navigation helpers --
    def _make_back_btn(callback):
        """Create a fresh Back button wired to *callback*."""
        btn = widgets.Button(
            description="\u2190  Back",
            button_style="",
            layout=widgets.Layout(
                width="auto",
                padding="8px 18px",
                border_radius="10px",
            ),
        )
        btn.on_click(callback)
        return btn

    def _show_subview():
        """Hide landing & action buttons."""
        landing_html.layout.display = "none"
        buttons_box.layout.display = "none"
        import_row.layout.display = "none"

    def _show_landing(b=None):
        """Restore landing page."""
        output.clear_output()
        landing_html.layout.display = None
        buttons_box.layout.display = None
        import_row.layout.display = None
        state["prev_view"] = "landing"

    def _render_violations():
        """Re-render cached violations results."""
        cached = state.get("last_results")
        if not cached:
            _show_landing()
            return
        cat_data, rn, ot, sev, total = cached
        html = _build_results_html(cat_data, rn, ot, sev, total, dataset_name)
        _show_subview()
        state["prev_view"] = "landing"  # back from violations → landing

        back = _make_back_btn(_show_landing)
        edit = widgets.Button(
            description="\u270E  Edit Rules",
            button_style="",
            layout=widgets.Layout(
                width="auto",
                padding="8px 18px",
                border_radius="10px",
            ),
        )
        edit.on_click(_on_edit_from_violations)

        with output:
            output.clear_output(wait=True)
            display(
                widgets.HBox(
                    [back, edit],
                    layout=widgets.Layout(
                        justify_content="space-between", margin="0 0 8px 0"
                    ),
                )
            )
            display(HTML(html))

    def _on_back(b):
        """Context-aware back: editor→violations or editor/violations→landing."""
        prev = state.get("prev_view", "landing")
        if prev == "violations":
            _render_violations()
        else:
            _show_landing()

    # -- Helpers to compute violation data --
    def _compute_results(tom, dep, current_rules):
        violations_df = capture_violations(tom, dep, current_rules)

        sev_icon_map = {}
        for rule in current_rules:
            sev = rule.get("Severity", "")
            if sev in severity_map:
                sev_icon_map[sev] = severity_map[sev]

        vd = violations_df.copy()
        vd["Severity"] = vd["Severity"].replace(sev_icon_map)

        bpa_dict = {
            cat: vd[vd["Category"] == cat].drop("Category", axis=1)
            for cat in vd["Category"].drop_duplicates().values
        }

        all_rule_names = sorted(vd["Rule Name"].dropna().unique().tolist())
        all_object_types = sorted(vd["Object Type"].dropna().unique().tolist())
        all_severities = sorted(vd["Severity"].dropna().unique().tolist())

        categories_data = {}
        for cat, df in bpa_dict.items():
            if df.shape[0] == 0:
                continue
            rows = []
            for _, row in df.iterrows():
                rows.append(
                    {
                        "rule": str(row["Rule Name"]),
                        "objectType": str(row["Object Type"]),
                        "objectName": str(row["Object Name"]),
                        "severity": str(row["Severity"]),
                        "description": (
                            str(row["Description"])
                            if pd.notnull(row["Description"])
                            else ""
                        ),
                        "url": str(row["URL"]) if pd.notnull(row["URL"]) else "",
                    }
                )
            categories_data[cat] = rows

        total_violations = sum(len(v) for v in categories_data.values())
        return (
            categories_data,
            all_rule_names,
            all_object_types,
            all_severities,
            total_violations,
        )

    # -- Button callbacks --
    def on_run_bpa(b):
        with output:
            output.clear_output(wait=True)
            display(
                HTML(
                    '<div style="font-family:-apple-system,BlinkMacSystemFont,sans-serif;'
                    'color:#0071e3;font-size:14px;padding:20px;text-align:center;">'
                    "\u23f3 Evaluating rules against semantic model\u2026</div>"
                )
            )

        try:
            cat_data, rn, ot, sev, total = _compute_results(
                state["tom"], state["dep"], state["rules"]
            )
        except Exception as e:
            with output:
                output.clear_output(wait=True)
                display(
                    HTML(
                        f'<div style="color:#ff3b30;padding:20px;font-family:sans-serif;">'
                        f"\u274c Error running BPA: {e}</div>"
                    )
                )
            return

        state["last_results"] = (cat_data, rn, ot, sev, total)
        _render_violations()

    def _open_editor():
        """Render the rule editor into the output area."""
        rules_for_editor = []
        for rule in state["rules"]:
            scope = rule.get("Scope", "")
            if isinstance(scope, list):
                scope = ", ".join(scope)
            rules_for_editor.append(
                {
                    "Category": rule.get("Category", ""),
                    "Scope": scope,
                    "Severity": rule.get("Severity", ""),
                    "Name": rule.get("Name", ""),
                    "Expression": rule.get("Expression", ""),
                    "Description": rule.get("Description", "") or "",
                    "URL": rule.get("URL", "") or "",
                }
            )
        uid = f"bpaed{id(rules_for_editor)}"
        html = _build_editor_html(rules_for_editor, uid)
        _show_subview()

        # Hidden widget button inside the same output – HTML back button triggers it via JS
        back_trigger = widgets.Button(
            description="__ed_back__",
            tooltip="__ed_back__",
            layout=widgets.Layout(
                width="1px",
                height="1px",
                padding="0",
                margin="0",
                overflow="hidden",
                opacity="0",
            ),
        )
        back_trigger.on_click(_on_back)

        with output:
            output.clear_output(wait=True)
            display(back_trigger)
            display(HTML(html))

    def _on_edit_from_landing(b):
        """Open editor with back → landing."""
        state["prev_view"] = "landing"
        _open_editor()

    def _on_edit_from_violations(b):
        """Open editor with back → violations."""
        state["prev_view"] = "violations"
        _open_editor()

    def on_import_rules(b):
        file_upload.layout.display = "block"
        status_html.value = (
            '<span style="font-size:12px;color:#86868b;font-family:sans-serif;">'
            "Select a .json file\u2026</span>"
        )

    def on_file_uploaded(change):
        file_upload.layout.display = "none"
        uploaded = change.get("new")
        if not uploaded:
            return

        # ipywidgets FileUpload returns a tuple of dicts
        for item in uploaded:
            try:
                content = item["content"].tobytes().decode("utf-8")
                parsed = json.loads(content)
            except Exception as e:
                status_html.value = (
                    f'<span style="color:#ff3b30;font-size:12px;">'
                    f"Invalid JSON: {e}</span>"
                )
                return

            if not isinstance(parsed, list) or len(parsed) == 0:
                status_html.value = (
                    '<span style="color:#ff3b30;font-size:12px;">'
                    "Rules file must be a non-empty JSON array.</span>"
                )
                return

            required_keys = ["Category", "Scope", "Severity", "Name", "Expression"]
            for i, rule in enumerate(parsed):
                for k in required_keys:
                    if k not in rule or not rule[k]:
                        status_html.value = (
                            f'<span style="color:#ff3b30;font-size:12px;">'
                            f"Rule #{i + 1} is missing key: {k}</span>"
                        )
                        return

            state["rules"] = [dict(r) for r in parsed]
            status_html.value = (
                f'<span style="color:#34c759;font-size:12px;">'
                f"\u2705 Imported {len(parsed)} rule{'s' if len(parsed) != 1 else ''}"
                f" from {item.get('name', 'file')}</span>"
            )
            break

    run_btn.on_click(on_run_bpa)
    edit_btn.on_click(_on_edit_from_landing)
    import_btn.on_click(on_import_rules)
    file_upload.observe(on_file_uploaded, names="value")

    # -- Layout --
    landing_html = widgets.HTML(
        value=_build_landing_html(dataset_name, len(state["rules"]))
    )

    buttons_box = widgets.HBox(
        [run_btn, edit_btn, import_btn],
        layout=widgets.Layout(
            justify_content="center",
            gap="12px",
            margin="0 0 10px 0",
        ),
    )

    import_row = widgets.HBox(
        [file_upload, status_html],
        layout=widgets.Layout(
            justify_content="center",
            gap="10px",
            margin="0 0 10px 0",
        ),
    )

    ui = widgets.VBox(
        [landing_html, buttons_box, import_row, output],
        layout=widgets.Layout(max_width="100%"),
    )

    display(ui)
