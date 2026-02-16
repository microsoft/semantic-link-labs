import json
from IPython.display import display, HTML, Javascript
from sempy_labs.tom import connect_semantic_model
from typing import Optional
from uuid import UUID


def perspective_editor(dataset: str | UUID, workspace: Optional[str | UUID] = None):

    # -----------------------------
    # LOAD MODEL METADATA
    # -----------------------------
    with connect_semantic_model(dataset=dataset, workspace=workspace, readonly=True) as tom:

        metadata = {}
        for table in tom.model.Tables:
            metadata[table.Name] = {
                "columns": [c.Name for c in tom.all_columns() if c.Parent == table],
                "measures": [m.Name for m in table.Measures],
                "hierarchies": [h.Name for h in table.Hierarchies]
            }

        perspectives = [p.Name for p in tom.model.Perspectives]

    # -----------------------------
    # HTML UI
    # -----------------------------
    html = f"""
    <style>
    body {{
        font-family: -apple-system, BlinkMacSystemFont, "SF Pro Text", sans-serif;
        background: #f5f5f7;
    }}

    .card {{
        width: 900px;
        margin: 30px auto;
        background: rgba(255,255,255,0.85);
        backdrop-filter: blur(20px);
        border-radius: 18px;
        box-shadow: 0 20px 40px rgba(0,0,0,0.08);
        padding: 20px;
    }}

    .header {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
    }}

    .title {{
        font-size: 20px;
        font-weight: 600;
    }}

    select, input {{
        padding: 8px 12px;
        border-radius: 10px;
        border: none;
        background: #f2f2f4;
        font-size: 14px;
    }}

    button {{
        border: none;
        border-radius: 12px;
        padding: 8px 14px;
        font-weight: 500;
        cursor: pointer;
    }}

    .primary {{
        background: #007aff;
        color: white;
    }}

    .danger {{
        background: #ff3b30;
        color: white;
    }}

    .secondary {{
        background: #e5e5ea;
    }}

    .tree-group {{
        margin-bottom: 8px;
    }}

    .tree-header {{
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 6px;
        border-radius: 8px;
        cursor: pointer;
    }}

    .tree-header:hover {{
        background: #f2f2f4;
    }}

    .children {{
        margin-left: 24px;
        display: none;
        flex-direction: column;
        gap: 4px;
    }}

    .expanded .children {{
        display: flex;
    }}

    .child-row {{
        display: flex;
        align-items: center;
        gap: 8px;
        padding: 4px;
    }}

    .icon {{
        width: 18px;
        text-align: center;
        opacity: 0.7;
    }}
    </style>

    <div class="card">
        <div class="header">
            <div class="title">Edit Perspective</div>
            <div>
                <select id="perspectiveSelect">
                    {''.join([f'<option>{p}</option>' for p in perspectives])}
                </select>
                <button class="secondary" onclick="createPerspective()">+</button>
            </div>
        </div>

        <div id="treeContainer"></div>

        <div style="margin-top:20px; display:flex; justify-content:flex-end; gap:10px;">
            <button class="danger" onclick="deletePerspective()">Delete</button>
            <button class="primary" onclick="savePerspective()">Save</button>
        </div>
    </div>

    <script>
    const metadata = {json.dumps(metadata)};

    function buildTree() {{
        const container = document.getElementById("treeContainer");
        container.innerHTML = "";

        Object.keys(metadata).forEach(table => {{
            const group = document.createElement("div");
            group.className = "tree-group";

            const header = document.createElement("div");
            header.className = "tree-header";

            const tableCheckbox = document.createElement("input");
            tableCheckbox.type = "checkbox";

            const label = document.createElement("div");
            label.innerHTML = "📦 " + table;

            header.appendChild(tableCheckbox);
            header.appendChild(label);
            group.appendChild(header);

            const children = document.createElement("div");
            children.className = "children";

            const childBoxes = [];

            ["columns","measures","hierarchies"].forEach(type => {{
                metadata[table][type].forEach(obj => {{

                    const row = document.createElement("div");
                    row.className = "child-row";

                    const cb = document.createElement("input");
                    cb.type = "checkbox";
                    cb.dataset.table = table;
                    cb.dataset.type = type;
                    cb.dataset.name = obj;

                    const iconMap = {{
                        columns: "◻︎",
                        measures: "∑",
                        hierarchies: "≡"
                    }};

                    const icon = document.createElement("div");
                    icon.className = "icon";
                    icon.innerHTML = iconMap[type];

                    const lbl = document.createElement("div");
                    lbl.innerHTML = obj;

                    row.appendChild(cb);
                    row.appendChild(icon);
                    row.appendChild(lbl);

                    children.appendChild(row);
                    childBoxes.push(cb);

                    cb.addEventListener("change", () => {{
                        const total = childBoxes.length;
                        const checked = childBoxes.filter(c => c.checked).length;

                        if (checked === 0) {{
                            tableCheckbox.checked = false;
                            tableCheckbox.indeterminate = false;
                        }}
                        else if (checked === total) {{
                            tableCheckbox.checked = true;
                            tableCheckbox.indeterminate = false;
                        }}
                        else {{
                            tableCheckbox.checked = false;
                            tableCheckbox.indeterminate = true;
                        }}
                    }});
                }});
            }});

            tableCheckbox.addEventListener("change", () => {{
                childBoxes.forEach(cb => cb.checked = tableCheckbox.checked);
                tableCheckbox.indeterminate = false;
            }});

            header.onclick = (e) => {{
                if (e.target !== tableCheckbox)
                    group.classList.toggle("expanded");
            }};

            group.appendChild(children);
            container.appendChild(group);
        }});
    }}

    function savePerspective() {{

    const perspective = document.getElementById("perspectiveSelect").value;
    const selected = [];

    document.querySelectorAll(".tree-group").forEach(group => {{

        const tableName = group.querySelector(".tree-header div:nth-child(2)").innerText.replace("📦 ", "");
        const tableCheckbox = group.querySelector(".tree-header input");
        const childCheckboxes = group.querySelectorAll(".child-row input");

        const totalChildren = childCheckboxes.length;
        const checkedChildren = Array.from(childCheckboxes).filter(cb => cb.checked);

        // ✅ Case 1: Entire table selected
        if (tableCheckbox.checked && checkedChildren.length === totalChildren) {{

            selected.push({{
                table: tableName,
                type: "table"
            }});

        }}
        // ✅ Case 2: Partial selection
        else if (checkedChildren.length > 0) {{

            checkedChildren.forEach(cb => {{
                selected.push({{
                    table: cb.dataset.table,
                    type: cb.dataset.type,
                    name: cb.dataset.name
                }});
            }});
        }}

        // ❌ Case 3: Nothing selected → ignore
    }});

    window.save_to_python(JSON.stringify({{
        action: "save",
        perspective: perspective,
        selected: selected
    }}));
}}

    function createPerspective() {{
        const name = prompt("Perspective name:");
        if (!name) return;

        window.save_to_python(JSON.stringify({{
            action: "create",
            perspective: name
        }}));
    }}

    function deletePerspective() {{
        const perspective = document.getElementById("perspectiveSelect").value;

        if (!confirm("Delete perspective?")) return;

        window.save_to_python(JSON.stringify({{
            action: "delete",
            perspective: perspective
        }}));
    }}

    buildTree();
    </script>
    """

    display(HTML(html))

    # -----------------------------
    # PYTHON CALLBACK
    # -----------------------------
    def save_to_python(payload):
        data = json.loads(payload)

        with connect_semantic_model(dataset=dataset, workspace=workspace, readonly=False) as tom:

            perspective = data['perspective']
            if data["action"] == "create":
                tom.add_perspective(perspective)
            elif data["action"] == "delete":
                p = tom.model.Perspectives[perspective]
                tom.remove_object(object=p)
            elif data["action"] == "save":
                p = tom.model.Perspectives.Find(perspective)
                if not p:
                    p = tom.add_perspective(perspective)
                # Clear existing
                for t in p.PerspectiveTables:
                    t.Clear()
                    for c in t.PerspectiveColumns:
                        c.Clear()
                    for m in t.PerspectiveMeasures:
                        m.Clear()
                    for h in t.PerspectiveHierarchies:
                        h.Clear()
                selected = data.get('selected', [])
                table_list = [t.get('table') for t in selected if t.get('type') == 'table']
                for t in table_list:
                    p.PerspectiveTables.Add(tom.model.Tables[t])
                for o in selected:
                    name = o.get('name')
                    type = o.get('type')
                    table = o.get('table')
                    if type == 'columns':
                        object = tom.model.Tables[table].Columns[name]
                        tom.add_to_perspective(object=object, perspective_name=perspective)
                    elif type == 'measures':
                        object = tom.model.Tables[table].Measures[name]
                        tom.add_to_perspective(object=object, perspective_name=perspective)
                    elif type == 'hierarchies':
                        object = tom.model.Tables[table].Hierarchies[name]
                        tom.add_to_perspective(object=object, perspective_name=perspective)

    display(Javascript(f"window.save_to_python = {save_to_python};"))