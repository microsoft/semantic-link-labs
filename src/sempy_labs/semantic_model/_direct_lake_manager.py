import sempy
import json
import uuid
from typing import Optional
from uuid import UUID
import ipywidgets as widgets
from IPython.display import display, HTML
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

        # Apply source (expression) changes
        for src in new_sources:
            expr_name = src.get("expressionName", "")
            if not expr_name:
                continue

            expression = generate_shared_expression(
                item_name=src["itemName"],
                item_type=src["itemType"],
                workspace=src["workspaceName"],
                use_sql_endpoint=src["usesSqlEndpoint"],
            )

            if expr_name in existing_expr_names:
                tom.model.Expressions[expr_name].Expression = expression
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

    print(f"{icons.green_dot} Changes applied successfully.")


# ---------------------------------------------------------------------------
# HTML / CSS / JS template
# ---------------------------------------------------------------------------

_CSS = """
<style>
/* == Reset & base == */
.dlm-root * { box-sizing: border-box; margin: 0; padding: 0; }
.dlm-root {
    font-family: -apple-system, BlinkMacSystemFont, "SF Pro Display",
                 "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: #1d1d1f;
    background: #f5f5f7;
    border-radius: 16px;
    overflow: hidden;
    max-width: 1100px;
    box-shadow: 0 4px 24px rgba(0,0,0,.08);
}

/* == Top bar == */
.dlm-topbar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 18px 28px;
    background: #fff;
    border-bottom: 1px solid #e5e5ea;
}
.dlm-topbar-title {
    font-size: 20px;
    font-weight: 600;
    letter-spacing: -0.3px;
    flex: 1;
}
.dlm-topbar-badge {
    font-size: 11px;
    font-weight: 600;
    padding: 3px 10px;
    border-radius: 12px;
    background: #007aff;
    color: #fff;
}

/* == Navigation pill bar == */
.dlm-nav {
    display: flex;
    gap: 6px;
    padding: 12px 28px;
    background: #fff;
    border-bottom: 1px solid #e5e5ea;
}
.dlm-nav-btn {
    padding: 7px 20px;
    font-size: 13px;
    font-weight: 500;
    border-radius: 980px;
    border: none;
    cursor: pointer;
    background: #f5f5f7;
    color: #6e6e73;
    transition: all .2s ease;
}
.dlm-nav-btn:hover { background: #e8e8ed; }
.dlm-nav-btn.active {
    background: #007aff;
    color: #fff;
}

/* == Page container == */
.dlm-page { display: none; padding: 24px 28px 28px; }
.dlm-page.active { display: block; }

/* == Section header with action button == */
.dlm-section-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 16px;
}
.dlm-section-title {
    font-size: 17px;
    font-weight: 600;
    letter-spacing: -0.2px;
}
.dlm-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-size: 13px;
    font-weight: 500;
    padding: 7px 16px;
    border-radius: 980px;
    border: none;
    cursor: pointer;
    transition: all .15s ease;
}
.dlm-btn-primary { background: #007aff; color: #fff; }
.dlm-btn-primary:hover { background: #0066d6; }
.dlm-btn-danger  { background: #ff3b30; color: #fff; }
.dlm-btn-danger:hover  { background: #d42a20; }
.dlm-btn-ghost   { background: transparent; color: #007aff; }
.dlm-btn-ghost:hover   { background: rgba(0,122,255,.08); }
.dlm-btn-secondary { background: #e8e8ed; color: #1d1d1f; }
.dlm-btn-secondary:hover { background: #dcdce0; }

/* == Card / Table == */
.dlm-card {
    background: #fff;
    border-radius: 14px;
    box-shadow: 0 1px 4px rgba(0,0,0,.06);
    overflow: hidden;
    margin-bottom: 20px;
}
.dlm-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
}
.dlm-table thead th {
    text-align: left;
    padding: 12px 16px;
    font-weight: 600;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: .5px;
    color: #86868b;
    background: #fafafa;
    border-bottom: 1px solid #e5e5ea;
}
.dlm-table tbody td {
    padding: 11px 16px;
    border-bottom: 1px solid #f2f2f7;
    vertical-align: middle;
    text-align: left;
}
.dlm-table tbody tr:last-child td { border-bottom: none; }
.dlm-table tbody tr:hover { background: #f9f9fb; }

/* == Tag / pill helpers == */
.dlm-tag {
    display: inline-block;
    font-size: 11px;
    font-weight: 600;
    padding: 2px 10px;
    border-radius: 10px;
}
.dlm-tag-blue   { background: #e8f1ff; color: #0055d4; }
.dlm-tag-green  { background: #e0f9ed; color: #177a4b; }
.dlm-tag-gray   { background: #f2f2f7; color: #6e6e73; }
.dlm-tag-orange { background: #fff4e6; color: #b35c00; }

/* == Modal (add / edit) == */
.dlm-overlay {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,.35);
    backdrop-filter: blur(4px);
    z-index: 9998;
    justify-content: center;
    align-items: center;
}
.dlm-overlay.visible { display: flex; }
.dlm-modal {
    background: #fff;
    border-radius: 16px;
    width: 460px;
    max-width: 92vw;
    max-height: 90vh;
    display: flex;
    flex-direction: column;
    box-shadow: 0 20px 60px rgba(0,0,0,.18);
    overflow: hidden;
    animation: dlm-pop .2s ease;
}
@keyframes dlm-pop {
    from { transform: scale(.95); opacity: 0; }
    to   { transform: scale(1);   opacity: 1; }
}
.dlm-modal-header {
    padding: 18px 24px;
    font-size: 17px;
    font-weight: 600;
    border-bottom: 1px solid #e5e5ea;
    flex-shrink: 0;
}
.dlm-modal-body { padding: 20px 24px; overflow-y: auto; flex: 1; }
.dlm-modal-footer {
    display: flex;
    justify-content: flex-end;
    gap: 8px;
    padding: 14px 24px;
    border-top: 1px solid #e5e5ea;
    flex-shrink: 0;
}
.dlm-field { margin-bottom: 14px; }
.dlm-field label {
    display: block;
    font-size: 12px;
    font-weight: 600;
    color: #86868b;
    margin-bottom: 5px;
    text-transform: uppercase;
    letter-spacing: .4px;
}
.dlm-field input,
.dlm-field select {
    width: 100%;
    border: 1.5px solid #d2d2d7;
    border-radius: 8px;
    padding: 9px 12px;
    font-size: 14px;
    font-family: inherit;
    outline: none;
    transition: border .15s;
}
.dlm-field input:focus,
.dlm-field select:focus { border-color: #007aff; }

/* == Empty state == */
.dlm-empty {
    text-align: center;
    padding: 48px 24px;
    color: #86868b;
    font-size: 14px;
}
.dlm-empty-icon { font-size: 36px; margin-bottom: 10px; }

/* == Toast == */
.dlm-toast {
    position: fixed;
    bottom: 24px;
    left: 50%;
    transform: translateX(-50%) translateY(80px);
    background: #1d1d1f;
    color: #fff;
    padding: 10px 24px;
    border-radius: 980px;
    font-size: 13px;
    font-weight: 500;
    z-index: 99999;
    opacity: 0;
    transition: all .3s ease;
    pointer-events: none;
}
.dlm-toast.show {
    opacity: 1;
    transform: translateX(-50%) translateY(0);
}
</style>
"""


def _build_html(uid, sources_json, tables_json, dataset_name):
    """Return the full HTML string for the Direct Lake Manager UI."""

    return (
        _CSS
        + """
<div class="dlm-root" id="dlm-"""
        + uid
        + """">

  <!-- Top bar -->
  <div class="dlm-topbar">
    <div class="dlm-topbar-title">Direct Lake Manager</div>
    <span class="dlm-topbar-badge">"""
        + dataset_name
        + """</span>
  </div>

  <!-- Navigation -->
  <div class="dlm-nav">
    <button class="dlm-nav-btn active" data-page="sources" onclick="dlm_"""
        + uid
        + """.nav(this)">Sources</button>
    <button class="dlm-nav-btn" data-page="tables" onclick="dlm_"""
        + uid
        + """.nav(this)">Tables</button>
  </div>

  <!-- ============ SOURCES PAGE ============ -->
  <div class="dlm-page active" id="dlm-"""
        + uid
        + """-page-sources">
    <div class="dlm-section-header">
      <div class="dlm-section-title">Direct Lake Sources</div>
      <button class="dlm-btn dlm-btn-primary" onclick="dlm_"""
        + uid
        + """.openAddSource()">&#43;&ensp;Add Source</button>
    </div>
    <div class="dlm-card">
      <table class="dlm-table" id="dlm-"""
        + uid
        + """-src-table">
        <thead>
          <tr>
            <th>Expression Name</th>
            <th>Item Name</th>
            <th>Item Type</th>
            <th>Workspace</th>
            <th>SQL Endpoint</th>
            <th style="width:120px">Actions</th>
          </tr>
        </thead>
        <tbody id="dlm-"""
        + uid
        + """-src-body"></tbody>
      </table>
      <div class="dlm-empty" id="dlm-"""
        + uid
        + """-src-empty" style="display:none">
        <div class="dlm-empty-icon">&#128194;</div>
        No sources configured.
      </div>
    </div>
  </div>

  <!-- ============ TABLES PAGE ============ -->
  <div class="dlm-page" id="dlm-"""
        + uid
        + """-page-tables">
    <div class="dlm-section-header">
      <div class="dlm-section-title">Direct Lake Tables</div>
    </div>
    <div class="dlm-card">
      <table class="dlm-table" id="dlm-"""
        + uid
        + """-tbl-table">
        <thead>
          <tr>
            <th>Table</th>
            <th>Entity Name</th>
            <th>Schema</th>
            <th>Expression Name</th>
            <th style="width:90px">Actions</th>
          </tr>
        </thead>
        <tbody id="dlm-"""
        + uid
        + """-tbl-body"></tbody>
      </table>
      <div class="dlm-empty" id="dlm-"""
        + uid
        + """-tbl-empty" style="display:none">
        <div class="dlm-empty-icon">&#128203;</div>
        No Direct Lake tables found.
      </div>
    </div>
  </div>
</div>

<!-- ============ ADD / EDIT SOURCE MODAL ============ -->
<div class="dlm-overlay" id="dlm-"""
        + uid
        + """-modal-src">
  <div class="dlm-modal">
    <div class="dlm-modal-header" id="dlm-"""
        + uid
        + """-modal-src-title">Add Source</div>
    <div class="dlm-modal-body">
      <div class="dlm-field">
        <label>Expression Name</label>
        <input id="dlm-"""
        + uid
        + """-src-f-expr" placeholder="e.g. DatabaseQuery" />
      </div>
      <div class="dlm-field">
        <label>Item Name</label>
        <input id="dlm-"""
        + uid
        + """-src-f-name" placeholder="e.g. My Lakehouse" />
      </div>
      <div class="dlm-field">
        <label>Item Type</label>
        <select id="dlm-"""
        + uid
        + """-src-f-type" onchange="dlm_"""
        + uid
        + """.onTypeChange()">
          <option value="Lakehouse">Lakehouse</option>
          <option value="Warehouse">Warehouse</option>
          <option value="MirroredDatabase">MirroredDatabase</option>
          <option value="SQLDatabase">SQLDatabase</option>
          <option value="MirroredAzureDatabricksCatalog">MirroredAzureDatabricksCatalog</option>
        </select>
      </div>
      <div class="dlm-field">
        <label>Workspace Name</label>
        <input id="dlm-"""
        + uid
        + """-src-f-ws" placeholder="Workspace name" />
      </div>
      <div class="dlm-field">
        <label>Uses SQL Endpoint</label>
        <select id="dlm-"""
        + uid
        + """-src-f-sql">
          <option value="true">Yes</option>
          <option value="false">No</option>
        </select>
      </div>
    </div>
    <div class="dlm-modal-footer">
      <button class="dlm-btn dlm-btn-secondary" onclick="dlm_"""
        + uid
        + """.closeModal('src')">Cancel</button>
      <button class="dlm-btn dlm-btn-primary" id="dlm-"""
        + uid
        + """-src-save-btn" onclick="dlm_"""
        + uid
        + """.saveSource()">Save</button>
    </div>
  </div>
</div>

<!-- ============ EDIT TABLE MODAL ============ -->
<div class="dlm-overlay" id="dlm-"""
        + uid
        + """-modal-tbl">
  <div class="dlm-modal">
    <div class="dlm-modal-header">Edit Table Mapping</div>
    <div class="dlm-modal-body">
      <div class="dlm-field">
        <label>Table Name</label>
        <input id="dlm-"""
        + uid
        + """-tbl-f-table" disabled style="background:#f5f5f7;color:#86868b" />
      </div>
      <div class="dlm-field">
        <label>Expression Name</label>
        <select id="dlm-"""
        + uid
        + """-tbl-f-expr">
        </select>
      </div>
      <div class="dlm-field">
        <label>Entity Name</label>
        <input id="dlm-"""
        + uid
        + """-tbl-f-entity" placeholder="Entity name" />
      </div>
      <div class="dlm-field">
        <label>Schema Name</label>
        <input id="dlm-"""
        + uid
        + """-tbl-f-schema" placeholder="Schema name" />
      </div>
    </div>
    <div class="dlm-modal-footer">
      <button class="dlm-btn dlm-btn-secondary" onclick="dlm_"""
        + uid
        + """.closeModal('tbl')">Cancel</button>
      <button class="dlm-btn dlm-btn-primary" onclick="dlm_"""
        + uid
        + """.saveTable()">Save</button>
    </div>
  </div>
</div>

<!-- Toast -->
<div class="dlm-toast" id="dlm-"""
        + uid
        + """-toast"></div>

<script>
(function() {
  var uid = \""""
        + uid
        + """\";
  function el(id) { return document.getElementById(id); }

  /* == State == */
  var sources = """
        + sources_json
        + """;
  var tables  = """
        + tables_json
        + """;
  var editSrcIdx = -1;
  var editTblIdx = -1;

  /* == Escape HTML to prevent XSS == */
  function esc(str) {
    var d = document.createElement("div");
    d.appendChild(document.createTextNode(str));
    return d.innerHTML;
  }

  /* == Toast == */
  function toast(msg) {
    var t = el("dlm-"+uid+"-toast");
    t.textContent = msg;
    t.classList.add("show");
    setTimeout(function() { t.classList.remove("show"); }, 2200);
  }

  /* == Navigation == */
  function nav(btn) {
    var root = el("dlm-"+uid);
    var navBtns = root.querySelectorAll(".dlm-nav-btn");
    var pages   = root.querySelectorAll(".dlm-page");
    for (var i = 0; i < navBtns.length; i++) navBtns[i].classList.remove("active");
    for (var i = 0; i < pages.length; i++)   pages[i].classList.remove("active");
    btn.classList.add("active");
    el("dlm-"+uid+"-page-"+btn.getAttribute("data-page")).classList.add("active");
  }

  /* == Render sources == */
  function renderSources() {
    var body  = el("dlm-"+uid+"-src-body");
    var empty = el("dlm-"+uid+"-src-empty");
    if (!sources.length) {
      body.innerHTML = "";
      empty.style.display = "block";
      return;
    }
    empty.style.display = "none";
    var html = "";
    for (var i = 0; i < sources.length; i++) {
      var s = sources[i];
      var typeCls = s.itemType === "Lakehouse" ? "dlm-tag-blue" : "dlm-tag-orange";
      var sqlCls  = s.usesSqlEndpoint ? "dlm-tag-green" : "dlm-tag-gray";
      var sqlTxt  = s.usesSqlEndpoint ? "Yes" : "No";
      html += '<tr>'
        + '<td>' + esc(s.expressionName || s.ExpressionName || '') + '</td>'
        + '<td>' + esc(s.itemName) + '</td>'
        + '<td><span class="dlm-tag ' + typeCls + '">' + esc(s.itemType) + '</span></td>'
        + '<td>' + esc(s.workspaceName) + '</td>'
        + '<td><span class="dlm-tag ' + sqlCls + '">' + sqlTxt + '</span></td>'
        + '<td>'
        +   '<button class="dlm-btn dlm-btn-ghost" onclick="dlm_' + uid + '.editSource(' + i + ')" title="Edit">&#9998;</button>'
        +   '<button class="dlm-btn dlm-btn-ghost" style="color:#ff3b30" onclick="dlm_' + uid + '.deleteSource(' + i + ')" title="Delete">&#128465;</button>'
        + '</td>'
        + '</tr>';
    }
    body.innerHTML = html;
  }

  /* == Render tables == */
  function renderTables() {
    var body  = el("dlm-"+uid+"-tbl-body");
    var empty = el("dlm-"+uid+"-tbl-empty");
    if (!tables.length) {
      body.innerHTML = "";
      empty.style.display = "block";
      return;
    }
    empty.style.display = "none";
    var html = "";
    for (var i = 0; i < tables.length; i++) {
      var t = tables[i];
      html += '<tr>'
        + '<td style="font-weight:500">' + esc(t.tableName) + '</td>'
        + '<td>' + esc(t.entityName) + '</td>'
        + '<td>' + esc(t.schemaName) + '</td>'
        + '<td>' + esc(t.expressionName) + '</td>'
        + '<td>'
        +   '<button class="dlm-btn dlm-btn-ghost" onclick="dlm_' + uid + '.editTable(' + i + ')" title="Edit">&#9998;</button>'
        + '</td>'
        + '</tr>';
    }
    body.innerHTML = html;
  }

  /* == SQL endpoint source types == */
  var sqlEndpointSourceTypes = ["Lakehouse", "Warehouse"];

  function onTypeChange() {
    var typeVal = el("dlm-"+uid+"-src-f-type").value;
    var sqlEl   = el("dlm-"+uid+"-src-f-sql");
    if (sqlEndpointSourceTypes.indexOf(typeVal) === -1) {
      sqlEl.value = "false";
      sqlEl.disabled = true;
      sqlEl.style.opacity = "0.5";
    } else {
      sqlEl.disabled = false;
      sqlEl.style.opacity = "1";
      if (typeVal === "Warehouse") {
        sqlEl.value = "true";
      }
    }
  }

  /* == Source modal == */
  function openAddSource() {
    editSrcIdx = -1;
    el("dlm-"+uid+"-modal-src-title").textContent = "Add Source";
    el("dlm-"+uid+"-src-f-expr").value  = "";
    el("dlm-"+uid+"-src-f-expr").disabled = false;
    el("dlm-"+uid+"-src-f-expr").style.background = "";
    el("dlm-"+uid+"-src-f-expr").style.color = "";
    el("dlm-"+uid+"-src-f-name").value  = "";
    el("dlm-"+uid+"-src-f-type").value  = "Lakehouse";
    el("dlm-"+uid+"-src-f-ws").value    = "";
    el("dlm-"+uid+"-src-f-sql").value   = "true";
    onTypeChange();
    el("dlm-"+uid+"-modal-src").classList.add("visible");
  }

  function editSource(idx) {
    editSrcIdx = idx;
    var s = sources[idx];
    el("dlm-"+uid+"-modal-src-title").textContent = "Edit Source";
    el("dlm-"+uid+"-src-f-expr").value  = s.expressionName;
    el("dlm-"+uid+"-src-f-expr").disabled = true;
    el("dlm-"+uid+"-src-f-expr").style.background = "#f5f5f7";
    el("dlm-"+uid+"-src-f-expr").style.color = "#86868b";
    el("dlm-"+uid+"-src-f-name").value  = s.itemName;
    el("dlm-"+uid+"-src-f-type").value  = s.itemType;
    el("dlm-"+uid+"-src-f-ws").value    = s.workspaceName;
    el("dlm-"+uid+"-src-f-sql").value   = s.usesSqlEndpoint ? "true" : "false";
    onTypeChange();
    el("dlm-"+uid+"-modal-src").classList.add("visible");
  }

  function saveSource() {
    var exprName = el("dlm-"+uid+"-src-f-expr").value.trim();
    var name = el("dlm-"+uid+"-src-f-name").value.trim();
    var type = el("dlm-"+uid+"-src-f-type").value;
    var ws   = el("dlm-"+uid+"-src-f-ws").value.trim();
    var sql  = el("dlm-"+uid+"-src-f-sql").value === "true";
    if (!exprName) { toast("Expression name is required"); return; }
    if (!name) { toast("Item name is required"); return; }
    if (!ws)   { toast("Workspace is required"); return; }
    var obj = {
      expressionName: exprName,
      itemId: editSrcIdx >= 0 ? sources[editSrcIdx].itemId : "",
      itemName: name,
      itemType: type,
      workspaceId: editSrcIdx >= 0 ? sources[editSrcIdx].workspaceId : "",
      workspaceName: ws,
      usesSqlEndpoint: sql
    };
    if (editSrcIdx >= 0) {
      sources[editSrcIdx] = obj;
      toast("Source updated");
    } else {
      sources.push(obj);
      toast("Source added");
    }
    closeModal("src");
    renderSources();
    pushState();
  }

  function deleteSource(idx) {
    if (!confirm("Remove this source?")) return;
    sources.splice(idx, 1);
    renderSources();
    toast("Source removed");
    pushState();
  }

  /* == Table modal == */
  function editTable(idx) {
    editTblIdx = idx;
    var t = tables[idx];
    el("dlm-"+uid+"-tbl-f-table").value  = t.tableName;
    /* Populate expression dropdown from sources */
    var exprSel = el("dlm-"+uid+"-tbl-f-expr");
    var seen = {};
    exprSel.innerHTML = "";
    for (var j = 0; j < sources.length; j++) {
      var en = sources[j].expressionName || sources[j].ExpressionName || "";
      if (en && !seen[en]) {
        seen[en] = true;
        var opt = document.createElement("option");
        opt.value = en;
        opt.textContent = en;
        exprSel.appendChild(opt);
      }
    }
    exprSel.value = t.expressionName;
    el("dlm-"+uid+"-tbl-f-entity").value = t.entityName;
    el("dlm-"+uid+"-tbl-f-schema").value = t.schemaName;
    el("dlm-"+uid+"-modal-tbl").classList.add("visible");
  }

  function saveTable() {
    var expr   = el("dlm-"+uid+"-tbl-f-expr").value.trim();
    var entity = el("dlm-"+uid+"-tbl-f-entity").value.trim();
    var schema = el("dlm-"+uid+"-tbl-f-schema").value.trim();
    if (!expr)   { toast("Expression name is required"); return; }
    if (!entity) { toast("Entity name is required"); return; }
    tables[editTblIdx].expressionName = expr;
    tables[editTblIdx].entityName     = entity;
    tables[editTblIdx].schemaName     = schema;
    closeModal("tbl");
    renderTables();
    toast("Table mapping updated");
    pushState();
  }

  /* == Close any modal == */
  function closeModal(type) {
    el("dlm-"+uid+"-modal-"+type).classList.remove("visible");
  }

  /* == Push state to ipywidgets Textarea bridge == */
  var _bridgeEl = null;
  function _findBridge() {
    if (_bridgeEl) return _bridgeEl;
    var sel = '.dlm-bridge-' + uid + ' textarea';
    var docs = [document];
    try { if (parent && parent.document !== document) docs.push(parent.document); } catch(e) {}
    try { if (top && top.document !== document && top.document !== parent.document) docs.push(top.document); } catch(e) {}
    for (var i = 0; i < docs.length; i++) {
      _bridgeEl = docs[i].querySelector(sel);
      if (_bridgeEl) return _bridgeEl;
    }
    return null;
  }
  function pushState() {
    var payload = JSON.stringify({ sources: sources, tables: tables });
    var br = _findBridge();
    if (br) {
      br.value = payload;
      br.dispatchEvent(new Event('input', { bubbles: true }));
    }
  }

  /* == Initial render == */
  renderSources();
  renderTables();

  /* Expose API globally so onclick handlers work */
  window["dlm_" + uid] = {
    nav: nav,
    openAddSource: openAddSource,
    editSource: editSource,
    onTypeChange: onTypeChange,
    deleteSource: deleteSource,
    saveSource: saveSource,
    editTable: editTable,
    saveTable: saveTable,
    closeModal: closeModal,
    getSources: function() { return sources; },
    getTables:  function() { return tables; }
  };
})();
</script>
"""
    )


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

    source_types = [
        "Lakehouse",
        "Warehouse",
        "MirroredDatabase",
        "SQLDatabase",
        "MirroredAzureDatabricksCatalog",
    ]
    sql_endpoint_source_types = ["Lakehouse", "Warehouse"]

    # Unique id so multiple widgets can coexist
    uid = uuid.uuid4().hex[:10]

    sources_json = json.dumps(sources)
    tables_json = json.dumps(tables)

    # Register callback for Apply Changes
    _dlm_callbacks[uid] = {
        "dataset_id": str(dataset_id),
        "workspace_id": str(workspace_id),
    }

    html_content = _build_html(uid, sources_json, tables_json, dataset_name)

    # State bridge: hidden Textarea that JS writes to on every edit
    initial_state = json.dumps({"sources": sources, "tables": tables})
    state_bridge = widgets.Textarea(
        value=initial_state,
        layout=widgets.Layout(
            visibility="hidden",
            height="1px",
            width="1px",
            overflow="hidden",
            margin="0",
            padding="0",
        ),
    )
    state_bridge.add_class(f"dlm-bridge-{uid}")

    # Status output widget for apply feedback
    status_output = widgets.Output()

    # Apply Changes button (Python widget - reliable comm protocol)
    apply_btn = widgets.Button(
        description="\u2713 Apply Changes",
        button_style="primary",
        layout=widgets.Layout(margin="8px 0 0 0"),
    )

    def _on_apply(_):
        val = state_bridge.value
        if not val:
            with status_output:
                status_output.clear_output(wait=True)
                print(f"{icons.warning} No changes to apply.")
            return
        with status_output:
            status_output.clear_output(wait=True)
            _apply_changes(uid, val)

    apply_btn.on_click(_on_apply)

    display(HTML(html_content))
    display(widgets.VBox([state_bridge, apply_btn, status_output]))
