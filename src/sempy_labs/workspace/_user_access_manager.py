"""Interactive, permission-aware workspace access review."""

from typing import Optional
from urllib.parse import quote, urlsplit
from uuid import UUID

from sempy._utils._log import log
from sempy_labs._helper_functions import _base_api, resolve_workspace_name_and_id
from sempy_labs._ui_components import (
    DARK_THEME_VARS,
    ICONS,
    LIGHT_THEME_VARS,
    SEARCH_SELECT_CSS,
    SEARCH_SELECT_JS,
    fullscreen_css,
    fullscreen_setup_js,
    render_attribution_html,
    scoped_attribution_css,
    scoped_header_css,
)


def _uuid(value):
    return str(UUID(str(value)))


def _pages(request, client):
    """Fetch every page with status checking and restrict continuation origins."""
    seen = set()
    while request:
        if request in seen:
            raise ValueError("Repeated continuation link.")
        seen.add(request)
        data = _base_api(request=request, client=client).json()
        if not isinstance(data, dict) or not isinstance(data.get("value"), list):
            raise ValueError("Invalid paginated response; results are incomplete.")
        yield from data["value"]
        link = data.get("@odata.nextLink" if client == "graph" else "continuationUri")
        if not link:
            if client != "graph" and data.get("continuationToken"):
                raise ValueError("Missing continuation URI; results are incomplete.")
            break
        parts = urlsplit(link)
        host = "graph.microsoft.com" if client == "graph" else "api.fabric.microsoft.com"
        if (
            parts.scheme != "https"
            or parts.netloc.lower() != host
            or parts.fragment
            or not parts.path.startswith("/v1.0/" if client == "graph" else "/v1/")
        ):
            raise ValueError("Invalid continuation URI; results are incomplete.")
        request = (
            parts.path.removeprefix("/v1.0/") + ("?" + parts.query if parts.query else "")
            if client == "graph"
            else link
        )


def _principal(kind, identity):
    identity = identity.strip()
    if not identity:
        raise ValueError("Enter a user UPN/object ID or group object ID.")
    if kind == "User":
        if identity in (".", "..") or "/" in identity or "\\" in identity:
            raise ValueError("Invalid user UPN or object ID.")
        path = "users/" + quote(identity, safe="")
    elif kind == "Group":
        path = "groups/" + _uuid(identity)
    else:
        raise ValueError("Only users and groups are supported.")
    obj = _base_api(request=path, client="graph").json()
    return {
        "id": _uuid(obj["id"]),
        "type": kind,
        "name": obj.get("displayName") or obj.get("userPrincipalName") or obj["id"],
    }


def _review(kind, identity):
    """Return visible workspace assignments, recording each inaccessible scope."""
    principal = _principal(kind, identity)
    warnings = []
    groups = {}
    members = []
    try:
        directory = "users" if kind == "User" else "groups"
        for obj in _pages(f"{directory}/{principal['id']}/transitiveMemberOf", "graph"):
            if obj.get("@odata.type") == "#microsoft.graph.group" and obj.get("id"):
                groups[_uuid(obj["id"])] = obj.get("displayName") or obj["id"]
    except Exception as exc:
        groups = {}
        warnings.append(f"Group resolution failed; inherited access is incomplete: {exc}")
    if kind == "Group":
        try:
            members = [
                {
                    "id": obj.get("id") or "",
                    "name": obj.get("displayName") or "(unavailable)",
                    "type": obj.get("@odata.type", "").removeprefix("#microsoft.graph."),
                    "upn": obj.get("userPrincipalName") or "",
                }
                for obj in _pages(f"groups/{principal['id']}/transitiveMembers", "graph")
            ]
        except Exception as exc:
            members = []
            warnings.append(f"Group members could not be fully loaded: {exc}")

    workspaces = []
    reviewed_workspace_ids = []
    rows = []
    try:
        workspaces = [
            {"id": _uuid(w["id"]), "name": w.get("displayName") or w["id"]}
            for w in _pages("/v1/workspaces", "fabric_sp")
        ]
    except Exception as exc:
        warnings.append(f"Workspace inventory failed; review is incomplete: {exc}")

    for ws in workspaces:
        try:
            assignments = list(_pages(f"/v1/workspaces/{ws['id']}/roleAssignments", "fabric_sp"))
            workspace_rows = []
            for assignment in assignments:
                p = assignment.get("principal") or {}
                try:
                    pid = _uuid(p.get("id"))
                except (ValueError, TypeError, AttributeError):
                    continue
                direct = pid == principal["id"] and p.get("type") == kind
                inherited = p.get("type") == "Group" and pid in groups
                if not direct and not inherited:
                    continue
                try:
                    assignment_id = _uuid(assignment["id"])
                except (ValueError, TypeError, KeyError, AttributeError):
                    assignment_id = ""
                workspace_rows.append({
                    "workspace_id": ws["id"], "workspace": ws["name"],
                    "assignment_id": assignment_id, "principal_id": pid,
                    "role": assignment.get("role") or "(unknown)",
                    "path": "Direct" if direct else f"Via group: {p.get('displayName') or groups[pid]} ({pid})",
                    "direct": direct,
                })
            rows.extend(workspace_rows)
            reviewed_workspace_ids.append(ws["id"])
        except Exception as exc:
            warnings.append(f"Workspace {ws['name']} ({ws['id']}) could not be reviewed: {exc}")
    return {"principal": principal, "workspaces": workspaces, "rows": rows,
            "members": members, "warnings": warnings,
            "reviewed_workspace_ids": reviewed_workspace_ids}


def _mutate(review, action, workspace_id, assignment_id="", role=""):
    """Only mutate the selected principal's direct assignment from the last review."""
    if not review:
        raise ValueError("Review a principal before making changes.")
    ws = _uuid(workspace_id)
    if ws not in review["reviewed_workspace_ids"]:
        raise ValueError("Workspace assignments were not successfully reviewed.")
    principal = review["principal"]
    if action in ("grant", "update") and role not in ("Admin", "Member", "Contributor", "Viewer"):
        raise ValueError("Invalid workspace role.")
    base = f"/v1/workspaces/{ws}/roleAssignments"
    if action == "grant":
        if any(r["workspace_id"] == ws and r["direct"] for r in review["rows"]):
            raise ValueError("A direct assignment already exists; update it instead.")
        _base_api(request=base, method="post", client="fabric_sp", status_codes=[200, 201],
                  payload={"principal": {"id": principal["id"], "type": principal["type"]}, "role": role})
    elif action in ("update", "remove"):
        aid = _uuid(assignment_id)
        matches = [r for r in review["rows"] if r["workspace_id"] == ws
                   and r["assignment_id"] == aid and r["direct"]
                   and r["principal_id"] == principal["id"]]
        if len(matches) != 1:
            raise ValueError("Only the selected principal's direct assignment can be changed.")
        kwargs = {"request": f"{base}/{aid}", "client": "fabric_sp"}
        if action == "update":
            _base_api(**kwargs, method="patch", payload={"role": role}, status_codes=[200])
        else:
            _base_api(**kwargs, method="delete", status_codes=[200, 204])
    else:
        raise ValueError("Unknown action.")


_CSS = """
.slls-uam { __LIGHT__ width:100%; max-width:1100px; margin:auto; padding:20px; border:1px solid var(--ui-border); border-radius:12px; background:var(--ui-bg); color:var(--ui-text); font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Helvetica Neue",Helvetica,Arial,sans-serif; -webkit-font-smoothing:antialiased; }
.slls-uam.slls-uam-dark { __DARK__ }
.slls-uam * { box-sizing:border-box; }
.slls-uam .head { display:flex; align-items:center; gap:10px; }
.slls-uam .heading { flex:1; }
.slls-uam h2 { margin:0; font-size:22px; }
.slls-uam .note { color:var(--ui-text-secondary); font-size:13px; line-height:1.5; }
.slls-uam .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:14px 0; }
.slls-uam input,.slls-uam select,.slls-uam button { font:inherit; border:1px solid var(--ui-border-strong); border-radius:8px; background:var(--ui-bg); color:var(--ui-text); padding:7px; }
.slls-uam button { cursor:pointer; }
.slls-uam button:disabled { opacity:.5; cursor:not-allowed; }
.slls-uam .picker { min-width:220px; }
.slls-uam .scroll { overflow:auto; max-height:360px; }
.slls-uam table { border-collapse:collapse; width:100%; font-size:13px; }
.slls-uam th,.slls-uam td { border-bottom:1px solid var(--ui-border); text-align:left; padding:8px; }
.slls-uam .error { color:var(--ui-danger-text); background:var(--ui-danger-bg); padding:8px; white-space:pre-wrap; }
""".replace("__LIGHT__", LIGHT_THEME_VARS).replace("__DARK__", DARK_THEME_VARS)
_CSS += scoped_header_css(".slls-uam") + scoped_attribution_css(".slls-uam")
_CSS += SEARCH_SELECT_CSS + fullscreen_css(".slls-uam", "slls-uam-fullscreen")

_JS = r"""
__FULLSCREEN__
__SEARCH__
function render({model,el}) {
 const root=document.createElement("div"); root.className="slls-uam"; el.appendChild(root);
 root.innerHTML=`<div class="head"><div class="heading"><h2>User Access Manager</h2><div class="note">Workspace access review</div></div><button class="sl-theme-btn" data-theme title="Toggle theme"></button><button class="sl-theme-btn" data-fullscreen title="Full screen"></button></div>
 <p class="note">Only caller-visible workspaces are scanned, not the tenant. Listing assignments requires Member or higher workspace permissions; Graph requires a configured service principal and directory read permissions, including Member.Read.Hidden for hidden membership. Rate limiting or denied requests can leave individual workspaces unreviewed. Results can be incomplete. EntireTenant, other unsupported principal types, apps, items, tenant settings, grant date/by and full membership paths are not included. Other access paths may remain after removal.</p>
 <div class="controls"><label>Principal <select data-kind><option>User</option><option>Group</option></select></label><input data-identity placeholder="User UPN / object ID or group object ID" aria-label="Principal identity"><button data-review>Review</button></div>
 <div data-status role="status"></div><div data-result></div>__ATTR__`;
 const q=s=>root.querySelector(s), state={review:null,busy:false,selected:""};
 let picker=null;
 function theme(){root.classList.toggle("slls-uam-dark",!!model.get("dark_mode"));q("[data-theme]").innerHTML=model.get("dark_mode")?`__SUN__`:`__MOON__`;}
 q("[data-theme]").onclick=()=>{model.set("dark_mode",!model.get("dark_mode"));model.save_changes();};model.on("change:dark_mode",theme);theme();
 sllsSetupFullscreen(root,q("[data-fullscreen]"),"slls-uam-fullscreen",`__FS__`,`__FS_EXIT__`);
 function dispatch(action,extra={}){if(state.busy)return;state.busy=true;draw();model.set("pending_action",{action,...extra});model.set("run",model.get("run")+1);model.save_changes();}
 function text(tag,value){const e=document.createElement(tag);e.textContent=value||"";return e;}
 function draw(){
   const data=model.get("data")||{}, target=q("[data-result]");target.replaceChildren();q("[data-review]").disabled=state.busy;
   const status=model.get("status")||{};q("[data-status]").textContent=status.message||"";q("[data-status]").className=status.error?"error":"note";
   if(!data.principal){state.selected="";if(picker){picker.setDisabled(true);picker.setOptions([],"");}return;}
   target.appendChild(text("h3",`${data.principal.name} (${data.principal.type}, ${data.principal.id})`));
   if(data.warnings.length){const w=text("div",data.warnings.join("\n"));w.className="error";target.appendChild(w);}
   const tools=document.createElement("div");tools.className="controls";tools.innerHTML='<span class="picker" data-picker></span><label>Role <select data-role><option>Viewer</option><option>Contributor</option><option>Member</option><option>Admin</option></select></label><button data-grant>Grant direct access</button>';target.appendChild(tools);
   if(!picker)picker=createSearchSelect({placeholder:"Select workspace…",searchPlaceholder:"Filter workspaces…",ariaLabel:"Workspace",onChange:o=>{state.selected=o.value;}});
   const reviewed=data.reviewed_workspace_ids||[];
   if(!reviewed.includes(state.selected))state.selected=reviewed.includes(model.get("workspace_id"))?model.get("workspace_id"):"";
   picker.setOptions(data.workspaces.filter(w=>reviewed.includes(w.id)).map(w=>({value:w.id,label:`${w.name} (${w.id})`})),state.selected);picker.setDisabled(state.busy);tools.querySelector("[data-picker]").appendChild(picker.el);
   tools.querySelector("[data-grant]").disabled=state.busy;
   tools.querySelector("[data-grant]").onclick=()=>{const ws=data.workspaces.find(w=>w.id===state.selected),role=tools.querySelector("[data-role]").value;if(!ws){alert("Select a workspace.");return;}const impact=data.principal.type==="Group"?" This changes access for the group and its members.":"";if(confirm(`Grant ${data.principal.name} (${data.principal.id}) direct ${role} access to ${ws.name} (${ws.id})?${impact}`))dispatch("grant",{workspace_id:ws.id,role});};
   const table=document.createElement("table");table.innerHTML="<thead><tr><th>Workspace</th><th>Access through</th><th>Role</th><th>Actions</th></tr></thead>";const body=document.createElement("tbody");table.appendChild(body);
   for(const row of data.rows){const tr=document.createElement("tr");tr.append(text("td",`${row.workspace} (${row.workspace_id})`),text("td",row.path),text("td",row.role));const cell=document.createElement("td");
    if(row.direct&&row.assignment_id){for(const action of ["update","remove"]){const b=text("button",action==="update"?"Change role":"Remove");b.disabled=state.busy;b.onclick=()=>{const role=tools.querySelector("[data-role]").value;const detail=action==="update"?`Change role to ${role}`:"Remove direct access";const impact=data.principal.type==="Group"?" This changes access for the group and its members.":"";if(confirm(`${detail} for ${data.principal.name} (${data.principal.id}) in ${row.workspace} (${row.workspace_id})? Other access paths may remain.${impact}`))dispatch(action,{workspace_id:row.workspace_id,assignment_id:row.assignment_id,role});};cell.appendChild(b);}}else cell.textContent=row.direct?"Assignment ID unavailable; read-only":"Inherited / read-only";tr.appendChild(cell);body.appendChild(tr);}
   const scroll=document.createElement("div");scroll.className="scroll";scroll.appendChild(table);target.appendChild(scroll);if(!data.rows.length)target.appendChild(text("p","No matching visible assignments found; this is not proof of no access."));
   if(data.principal.type==="Group"){target.appendChild(text("h3","Transitive group members (nested; details may be restricted)"));const list=document.createElement("ul");for(const m of data.members)list.appendChild(text("li",`${m.name} — ${m.type} — ${m.upn} (${m.id})`));target.appendChild(list);}
 }
 q("[data-review]").onclick=()=>dispatch("review",{kind:q("[data-kind]").value,identity:q("[data-identity]").value});
 model.on("change:data",draw);model.on("change:status",()=>{state.busy=false;draw();});draw();
}
export default { render };
"""
for key, value in {
    "__FULLSCREEN__": fullscreen_setup_js(), "__SEARCH__": SEARCH_SELECT_JS,
    "__ATTR__": render_attribution_html(), "__SUN__": ICONS["sun"],
    "__MOON__": ICONS["moon"], "__FS__": ICONS["fullscreen"],
    "__FS_EXIT__": ICONS["fullscreen_exit"],
}.items():
    _JS = _JS.replace(key, value)


@log
def user_access_manager(workspace: Optional[str | UUID] = None, dark_mode: bool = False) -> None:
    """
    Display an interactive workspace access review and administration tool.

    Reviews caller-visible workspaces only; access to role assignments depends on
    workspace permissions. Graph resolution requires configured service principal
    authentication and appropriate Graph application permissions, including
    Member.Read.Hidden for hidden membership. Listing workspace assignments
    requires Member or higher access. Rate limits and failures are reported per
    workspace. Results can be incomplete. EntireTenant assignments, other principal
    types, apps, items, tenant settings, grant dates and grantors are not supported.
    Group inheritance is read-only unless
    the group itself is selected as the principal. Removing one assignment does
    not necessarily remove effective access.

    Uses `workspace role assignments <https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspace-role-assignments>`_
    and `transitive membership <https://learn.microsoft.com/graph/api/user-list-transitivememberof>`_.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        Workspace name or ID, defaulting to the attached lakehouse workspace or
        notebook workspace. It does not expand the caller's visibility.
    dark_mode : bool, default=False
        Whether to initially use the dark theme.
    """
    try:
        import anywidget
        import traitlets
    except ImportError as exc:
        raise ImportError("user_access_manager requires anywidget; install it with: pip install anywidget") from exc
    from IPython.display import display

    _, workspace_id = resolve_workspace_name_and_id(workspace)

    class _Widget(anywidget.AnyWidget):
        _esm = _JS
        _css = _CSS
        data = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)

    widget = _Widget(dark_mode=dark_mode, workspace_id=str(workspace_id))
    review = None

    def on_run(_change):
        nonlocal review
        action = dict(widget.pending_action)
        try:
            if action.get("action") == "review":
                review = None
                widget.data = {}
                review = _review(action.get("kind"), action.get("identity", ""))
                widget.data = review
                widget.status = {"message": "Review complete. Check warnings and visibility limits.", "error": False, "run": widget.run}
            else:
                _mutate(review, action.get("action"), action.get("workspace_id"),
                        action.get("assignment_id", ""), action.get("role", ""))
                review = None
                widget.data = {}
                widget.status = {"message": "Change submitted. Review again before another change; other access paths may remain.", "error": False, "run": widget.run}
        except Exception as exc:
            review = None
            widget.data = {}
            widget.status = {"message": f"Operation failed; review again. {exc}", "error": True, "run": widget.run}

    widget.observe(on_run, names=["run"])
    display(widget)
