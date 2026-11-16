"""Interactive, permission-aware workspace access review."""

from contextlib import contextmanager
from typing import Optional
from urllib.parse import quote, urlsplit
from uuid import UUID

from sempy._utils._log import log
import sempy_labs._authentication as auth
from sempy_labs._helper_functions import _base_api
from sempy_labs._ui_components import (
    DARK_THEME_VARS,
    ICONS,
    LIGHT_THEME_VARS,
    TABLE_COLUMN_RESIZE_JS,
    fullscreen_css,
    fullscreen_setup_js,
    render_attribution_html,
    scoped_attribution_css,
    scoped_header_css,
    scoped_table_column_resize_css,
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


def _graph_available(graph_credential=None):
    # Fabric notebooks cannot issue Graph tokens for the signed-in user, so Graph
    # calls need either an explicit credential or a service principal context.
    return graph_credential is not None or auth.token_provider.get() is not None


@contextmanager
def _graph_auth(graph_credential=None):
    """Temporarily route Graph calls through the given credential."""
    if graph_credential is None:
        yield
        return
    token = auth.token_provider.set(graph_credential)
    try:
        yield
    finally:
        auth.token_provider.reset(token)


def _graph_credential_from_key_vault(key_vault_uri, key_vault_tenant_id, key_vault_client_id, key_vault_client_secret):
    values = (key_vault_uri, key_vault_tenant_id, key_vault_client_id, key_vault_client_secret)
    if not any(values):
        return None
    if not all(values):
        raise ValueError(
            "Provide key_vault_uri, key_vault_tenant_id, key_vault_client_id and key_vault_client_secret together."
        )
    return auth.ServicePrincipalTokenProvider.from_azure_key_vault(
        key_vault_uri=key_vault_uri,
        key_vault_tenant_id=key_vault_tenant_id,
        key_vault_client_id=key_vault_client_id,
        key_vault_client_secret=key_vault_client_secret,
    )


def _local_principal(kind, identity):
    """Build a principal without Graph; a user UPN is resolved from role assignments."""
    identity = identity.strip()
    if not identity:
        raise ValueError("Enter a user UPN/object ID or group object ID.")
    if kind == "User":
        try:
            return {"id": _uuid(identity), "type": kind, "name": identity}, ""
        except ValueError:
            return {"id": "", "type": kind, "name": identity}, identity.lower()
    if kind == "Group":
        return {"id": _uuid(identity), "type": kind, "name": identity}, ""
    raise ValueError("Only users and groups are supported.")


def _review(kind, identity, graph_credential=None):
    """Return visible workspace assignments, recording each inaccessible scope."""
    graph = _graph_available(graph_credential)
    upn = ""
    warnings = []
    groups = {}
    members = []
    info = ""
    if graph:
        # Only Graph calls use the credential; Fabric calls stay on the caller's identity.
        with _graph_auth(graph_credential):
            principal = _principal(kind, identity)
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
    else:
        principal, upn = _local_principal(kind, identity)
        # Informational, not a warning: Graph is optional.
        info = (
            "Microsoft Graph is not connected (optional), so only direct assignments are shown. "
            "To include group-inherited access and group members, connect a service principal "
            "under Microsoft Graph authentication."
        )

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
                direct = p.get("type") == kind and (
                    pid == principal["id"]
                    or (upn and ((p.get("userDetails") or {}).get("userPrincipalName") or "").lower() == upn)
                )
                if direct and not principal["id"]:
                    principal["id"] = pid
                    principal["name"] = p.get("displayName") or principal["name"]
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
    review = {"principal": principal, "workspaces": workspaces, "rows": rows,
              "members": members, "warnings": warnings, "info": info,
              "reviewed_workspace_ids": reviewed_workspace_ids, "graph": graph}
    review["summary"] = _summary(review, len(groups))
    return review


_ROLE_RANK = {"Viewer": 1, "Contributor": 2, "Member": 3, "Admin": 4}


def _summary(review, group_count):
    """Count access per workspace, using the highest role across all access paths."""
    effective = {}
    for row in review["rows"]:
        current = effective.get(row["workspace_id"])
        if current is None or _ROLE_RANK.get(row["role"], 0) > _ROLE_RANK.get(current, 0):
            effective[row["workspace_id"]] = row["role"]
    return {
        "workspaces_with_access": len(effective),
        "direct": sum(1 for row in review["rows"] if row["direct"]),
        "inherited": sum(1 for row in review["rows"] if not row["direct"]),
        "roles": {role: sum(1 for r in effective.values() if r == role) for role in ("Admin", "Member", "Contributor", "Viewer")},
        "reviewed": len(review["reviewed_workspace_ids"]),
        "visible": len(review["workspaces"]),
        "groups": group_count if review["graph"] else None,
        "members": len(review["members"]),
        "warnings": len(review["warnings"]),
    }


_ROLES = ("Viewer", "Contributor", "Member", "Admin")


def _mutate(review, action, workspace_id, assignment_id, role="", dry_run=False):
    """Change or remove only the reviewed principal's direct assignment from the last review."""
    if not review:
        raise ValueError("Review a principal before making changes.")
    ws = _uuid(workspace_id)
    if ws not in review["reviewed_workspace_ids"]:
        raise ValueError("Workspace assignments were not successfully reviewed.")
    if action not in ("update", "remove"):
        raise ValueError("Unknown action.")
    if action == "update" and role not in _ROLES:
        raise ValueError("Invalid workspace role.")
    aid = _uuid(assignment_id)
    principal = review["principal"]
    matches = [r for r in review["rows"] if r["workspace_id"] == ws
               and r["assignment_id"] == aid and r["direct"]
               and r["principal_id"] == principal["id"]]
    if len(matches) != 1:
        raise ValueError("Only the selected principal's direct assignment can be changed.")
    if dry_run:
        return
    kwargs = {"request": f"/v1/workspaces/{ws}/roleAssignments/{aid}", "client": "fabric_sp"}
    if action == "update":
        _base_api(**kwargs, method="patch", payload={"role": role}, status_codes=[200])
    else:
        _base_api(**kwargs, method="delete", status_codes=[200, 204])


def _apply(review, changes):
    """Validate every pending change before applying any; return one message per failed change."""
    if not changes:
        raise ValueError("There are no pending changes to save.")
    # Fabric reuses the principal ID as the role assignment ID in every workspace,
    # so an assignment is only unique together with its workspace.
    seen = set()
    for change in changes:
        _mutate(review, change.get("action"), change.get("workspace_id"),
                change.get("assignment_id"), change.get("role", ""), dry_run=True)
        key = (_uuid(change["workspace_id"]), _uuid(change["assignment_id"]))
        if key in seen:
            raise ValueError("Each assignment can only have one pending change.")
        seen.add(key)
    names = {r["workspace_id"]: r["workspace"] for r in review["rows"]}
    failures = []
    for change in changes:
        try:
            _mutate(review, change.get("action"), change.get("workspace_id"),
                    change.get("assignment_id"), change.get("role", ""))
        except Exception as exc:
            failures.append(f"{names.get(_uuid(change['workspace_id']), change.get('workspace_id'))}: {exc}")
    return failures


_CSS = """
.slls-uam { __LIGHT__ position:relative; box-sizing:border-box; font-size:13px; line-height:1.4; width:100%; max-width:1100px; margin:auto; padding:20px; border:1px solid var(--ui-border); border-radius:12px; background:var(--ui-bg); color:var(--ui-text); font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Helvetica Neue",Helvetica,Arial,sans-serif; -webkit-font-smoothing:antialiased; }
.slls-uam.slls-uam-dark { __DARK__ }
.slls-uam * { box-sizing:border-box; }
.slls-uam .note { color:var(--ui-text-secondary); font-size:13px; line-height:1.5; }
.slls-uam [data-status] { white-space:pre-wrap; margin:6px 0; }
.slls-uam .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:14px 0; }
.slls-uam .controls > label { display:inline-flex; align-items:center; gap:8px; color:var(--ui-text-secondary); }
.slls-uam [data-identity] { width:320px; max-width:100%; }
.slls-uam [data-result] > h3 { margin:16px 0 4px; font-size:16px; font-weight:600; }
.slls-uam input,.slls-uam select,.slls-uam button { font:inherit; font-size:13px; line-height:normal; height:30px; box-sizing:border-box; border:1px solid var(--ui-border-strong); border-radius:8px; background:var(--ui-bg); color:var(--ui-text); padding:0 10px; }
.slls-uam button { display:inline-flex; align-items:center; justify-content:center; gap:6px; }
.slls-uam button { cursor:pointer; }
.slls-uam button:disabled { opacity:.5; cursor:not-allowed; }
.slls-uam .scroll { overflow:auto; max-height:420px; border:1px solid var(--ui-border); border-radius:8px; }
.slls-uam table { border-collapse:collapse; width:100%; font-size:13px; }
.slls-uam th,.slls-uam td { border-bottom:1px solid var(--ui-border); text-align:left; padding:8px; }
.slls-uam .error { color:var(--ui-danger-text); background:var(--ui-danger-bg); padding:8px; border-radius:8px; white-space:pre-wrap; }
.slls-uam .graph { margin:14px 0 0; border:1px solid var(--ui-border); border-radius:8px; background:var(--ui-bg-secondary); padding:10px 14px; }
.slls-uam .graph summary { cursor:pointer; font-size:13px; font-weight:600; display:flex; align-items:center; gap:8px; }
.slls-uam .graph .optional { font-weight:400; color:var(--ui-text-tertiary); }
.slls-uam .graph-logo { display:inline-flex; }
.slls-uam .graph-logo svg { display:block; width:18px; height:18px; }
.slls-uam .kv { display:grid; grid-template-columns:repeat(auto-fit,minmax(220px,1fr)); gap:10px; }
.slls-uam .kv label { display:flex; flex-direction:column; gap:4px; font-size:12px; color:var(--ui-text-secondary); }
.slls-uam .pill { font-size:11.5px; font-weight:500; padding:2px 8px; border-radius:999px; background:var(--ui-bg-tertiary); color:var(--ui-text-secondary); }
.slls-uam .pill.on { background:var(--ui-accent-soft); color:var(--ui-accent); }
.slls-uam .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:10px; margin:12px 0 6px; }
.slls-uam .card { padding:12px 14px; border:1px solid var(--ui-border); border-radius:8px; background:var(--ui-bg-secondary); }
.slls-uam .card-label { font-size:12px; color:var(--ui-text-secondary); }
.slls-uam .card-value { font-size:22px; font-weight:600; margin-top:2px; font-variant-numeric:tabular-nums; }
.slls-uam .rows th { position:sticky; top:0; z-index:1; background:var(--ui-bg-secondary); font-weight:600; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
.slls-uam .rows td { overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.slls-uam .rows td.id { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; font-size:12px; color:var(--ui-text-secondary); }
.slls-uam .rows td.actions { white-space:nowrap; color:var(--ui-text-tertiary); }
.slls-uam .rows tr.pending-remove td:not(.actions) { text-decoration:line-through; color:var(--ui-text-tertiary); }
.slls-uam .rows .new-role { color:var(--ui-pending); font-weight:600; }
.slls-uam .icon-btn { display:inline-flex; align-items:center; justify-content:center; width:28px; height:28px; padding:0; margin-right:4px; border-radius:7px; background:transparent; border-color:transparent; color:var(--ui-text-secondary); transition:background 120ms ease, color 120ms ease; }
.slls-uam .icon-btn:hover:not(:disabled) { background:var(--ui-bg-hover); color:var(--ui-text); }
.slls-uam .icon-btn.danger:hover:not(:disabled) { background:var(--ui-danger-bg); color:var(--ui-danger-text); }
.slls-uam .icon-btn svg { width:15px; height:15px; display:block; }
.slls-uam .dot { display:inline-block; width:8px; height:8px; margin-right:7px; border-radius:50%; background:var(--ui-pending); vertical-align:middle; flex-shrink:0; }
.slls-uam .sort { all:unset; display:inline-flex; align-items:center; gap:4px; max-width:100%; cursor:pointer; color:inherit; }
.slls-uam .sort:hover { color:var(--ui-accent); }
.slls-uam .sort:focus-visible { outline:2px solid var(--ui-accent); outline-offset:2px; border-radius:3px; }
.slls-uam th[aria-sort="ascending"] .sort,.slls-uam th[aria-sort="descending"] .sort { color:var(--ui-accent); }
.slls-uam .sort-icon svg { display:block; width:12px; height:12px; }
.slls-uam .filterbar { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin:14px 0 8px; }
.slls-uam .filterbar .spacer { flex:1; }
.slls-uam .search { display:flex; align-items:center; gap:6px; flex:0 1 340px; height:30px; box-sizing:border-box; padding:0 10px; border:1px solid var(--ui-border-strong); border-radius:8px; background:var(--ui-bg); color:var(--ui-text-tertiary); }
.slls-uam .search svg { width:14px; height:14px; flex-shrink:0; }
.slls-uam .search input { border:none; height:100%; padding:0; width:100%; outline:none; background:transparent; }
.slls-uam .pending-btn { display:inline-flex; align-items:center; }
.slls-uam .icon-btn.bar-btn { width:30px; height:30px; margin:0; border-color:var(--ui-border-strong); }
.slls-uam .icon-btn.bar-btn svg { width:16px; height:16px; }
.slls-uam .icon-btn.bar-btn.primary { background:var(--ui-accent); border-color:var(--ui-accent); color:var(--ui-on-accent); }
.slls-uam .icon-btn.bar-btn.primary:hover:not(:disabled) { background:var(--ui-accent-hover); color:var(--ui-on-accent); }
.slls-uam button.primary { background:var(--ui-accent); border-color:var(--ui-accent); color:var(--ui-on-accent); }
.slls-uam button.primary.danger { background:var(--ui-danger); border-color:var(--ui-danger); }
.slls-uam .overlay { position:absolute; inset:0; z-index:140; display:flex; align-items:center; justify-content:center; padding:24px; border-radius:12px; background:color-mix(in srgb, var(--ui-bg) 72%, transparent); backdrop-filter:blur(8px); }
.slls-uam .dialog { width:min(520px,100%); max-height:100%; overflow:auto; padding:20px; border:1px solid var(--ui-border-strong); border-radius:12px; background:var(--ui-bg); box-shadow:var(--ui-shadow-lg); }
.slls-uam .dialog h3 { margin:0 0 8px; font-size:16px; }
.slls-uam .dialog p { margin:0 0 10px; font-size:13px; line-height:1.5; color:var(--ui-text-secondary); overflow-wrap:anywhere; }
.slls-uam .dialog label { display:flex; flex-direction:column; gap:4px; font-size:12px; color:var(--ui-text-secondary); }
.slls-uam .dialog-actions { display:flex; justify-content:flex-end; gap:8px; margin-top:18px; }
.slls-uam .info h4 { margin:12px 0 4px; font-size:13px; }
.slls-uam .info p { margin:0; }
.slls-uam .pending-list { list-style:none; margin:0; padding:0; border:1px solid var(--ui-border); border-radius:8px; }
.slls-uam .pending-list li { display:flex; align-items:center; gap:4px; padding:6px 6px 6px 12px; border-bottom:1px solid var(--ui-border); font-size:13px; }
.slls-uam .pending-list li:last-child { border-bottom:none; }
.slls-uam .pending-list .what { flex:1; overflow-wrap:anywhere; }
""".replace("__LIGHT__", LIGHT_THEME_VARS).replace("__DARK__", DARK_THEME_VARS)
_CSS += scoped_header_css(".slls-uam") + scoped_attribution_css(".slls-uam")
_CSS += fullscreen_css(".slls-uam", "slls-uam-fullscreen") + scoped_table_column_resize_css(".slls-uam")

_JS = r"""
__FULLSCREEN__
__RESIZE__
const INFO_HTML=`<p>Review which Fabric workspaces a user or group can access, see whether access is direct or inherited through a group, and change or remove the principal's direct role assignments.</p>
<h4>Scope</h4><p>Only caller-visible workspaces are scanned, not the tenant. Listing role assignments requires Member or higher workspace permissions. Rate limiting or denied requests can leave individual workspaces unreviewed, so results can be incomplete.</p>
<h4>Microsoft Graph (optional)</h4><p>Connecting a service principal through Azure Key Vault adds group-inherited access and group members. It is used only for Microsoft Graph lookups and needs directory read permissions, including Member.Read.Hidden for hidden membership. Without it, only direct assignments are shown. Workspace listing and changes always run as you.</p>
<h4>Making changes</h4><p>Role changes and removals are queued as pending changes, marked with an orange dot, and applied together when you click Save. Only the reviewed principal's direct assignments can be changed; group-inherited access is read-only. Other access paths may remain after removal.</p>
<h4>Not included</h4><p>EntireTenant, other unsupported principal types, apps, items, tenant settings, grant date/by and full membership paths are not included.</p>`;
function render({model,el}) {
 const root=document.createElement("div"); root.className="slls-uam"; el.appendChild(root);
 root.innerHTML=`<div class="sl-header"><span class="sl-title-icon">__ICON__</span><div class="sl-titlewrap"><div class="sl-title">User Access Manager</div><div class="sl-subtitle">Workspace access review</div></div><div class="sl-head-spacer"></div><button class="sl-theme-btn" data-info title="About this tool" aria-label="About this tool">__INFO__</button><button class="sl-theme-btn" data-fullscreen title="Full screen"></button><button class="sl-theme-btn" data-theme title="Toggle theme"></button></div>
 <details class="graph"><summary><span class="graph-logo">__GRAPH__</span>Microsoft Graph authentication <span class="optional">(optional)</span> <span class="pill" data-graph-pill></span></summary>
 <p class="note">Optional. Without it, the review still works but only shows direct assignments. To include group-inherited access and group members, enter the Azure Key Vault holding a service principal's secrets; they are used only for Microsoft Graph lookups. Workspace listing and changes still run as you.</p>
 <div class="kv"><label>Key Vault URI<input data-kv-uri placeholder="https://myvault.vault.azure.net/"></label><label>Tenant ID secret name<input data-kv-tenant></label><label>Client ID secret name<input data-kv-client></label><label>Client secret secret name<input data-kv-secret></label></div>
 <div class="controls"><button data-graph-connect>Connect</button><button data-graph-disconnect>Disconnect</button></div></details>
 <div class="controls"><label>Principal <select data-kind><option>User</option><option>Group</option></select></label><input data-identity placeholder="User UPN / object ID or group object ID" aria-label="Principal identity"><button data-review>Review</button></div>
 <div data-status role="status"></div><div data-result></div>__ATTR__`;
 const q=s=>root.querySelector(s), state={busy:false,filter:"",sort:{key:"workspace",dir:1},widths:new Map(),pending:new Map(),refreshTable:null};
 const kvFields={key_vault_uri:"[data-kv-uri]",key_vault_tenant_id:"[data-kv-tenant]",key_vault_client_id:"[data-kv-client]",key_vault_client_secret:"[data-kv-secret]"};
 function theme(){root.classList.toggle("slls-uam-dark",!!model.get("dark_mode"));q("[data-theme]").innerHTML=model.get("dark_mode")?`__SUN__`:`__MOON__`;}
 q("[data-theme]").onclick=()=>{model.set("dark_mode",!model.get("dark_mode"));model.save_changes();};model.on("change:dark_mode",theme);theme();
 sllsSetupFullscreen(root,q("[data-fullscreen]"),"slls-uam-fullscreen",`__FS__`,`__FS_EXIT__`);
 function fillGraphConfig(){const cfg=model.get("graph_config")||{};for(const [k,s] of Object.entries(kvFields))q(s).value=cfg[k]||"";}
 model.on("change:graph_config",fillGraphConfig);fillGraphConfig();
 function dispatch(action,extra={}){if(state.busy)return;state.busy=true;draw();model.set("pending_action",{action,...extra});model.set("run",model.get("run")+1);model.save_changes();}
 function text(tag,value){const e=document.createElement(tag);e.textContent=value||"";return e;}
 function iconButton(svg,label,cls=""){const b=document.createElement("button");b.type="button";b.className=("icon-btn "+cls).trim();b.innerHTML=svg;b.title=label;b.setAttribute("aria-label",label);return b;}
 function dot(){const d=text("span","");d.className="dot";d.title="Pending change";return d;}
 const plural=(n,word)=>`${n} ${word}${n===1?"":"s"}`;
 const ROLES=["Viewer","Contributor","Member","Admin"],RANK={Viewer:1,Contributor:2,Member:3,Admin:4};
 // In-widget dialog; native browser popups are blocked or jarring in notebook hosts.
 const overlay=document.createElement("div");overlay.className="overlay";overlay.style.display="none";
 const dialog=document.createElement("div");dialog.className="dialog";dialog.setAttribute("role","dialog");dialog.setAttribute("aria-modal","true");
 const dlgTitle=text("h3",""),dlgMessage=text("p",""),dlgBody=document.createElement("div"),dlgRoleLabel=text("label","Role"),dlgRole=document.createElement("select");
 for(const r of ROLES){const o=text("option",r);o.value=r;dlgRole.appendChild(o);}dlgRoleLabel.appendChild(dlgRole);
 const dlgActions=document.createElement("div");dlgActions.className="dialog-actions";
 const dlgCancel=text("button","Cancel"),dlgConfirm=text("button","Confirm");dlgCancel.type=dlgConfirm.type="button";
 dlgActions.append(dlgCancel,dlgConfirm);dialog.append(dlgTitle,dlgMessage,dlgBody,dlgRoleLabel,dlgActions);overlay.appendChild(dialog);root.appendChild(overlay);
 let dlgOnConfirm=null,dlgCurrentRole=null;
 function closeDialog(){overlay.style.display="none";dlgOnConfirm=null;}
 function ask({title,message="",body=null,confirmLabel=null,cancelLabel="Cancel",danger=false,role=null,currentRole=null,onConfirm=null}){
   dlgTitle.textContent=title;dlgMessage.textContent=message;dlgMessage.style.display=message?"":"none";
   dlgBody.replaceChildren(...(body?[body]:[]));dlgBody.style.display=body?"":"none";
   dlgCancel.textContent=cancelLabel;dlgConfirm.textContent=confirmLabel||"";dlgConfirm.style.display=confirmLabel?"":"none";
   dlgConfirm.className=danger?"primary danger":"primary";dlgRoleLabel.style.display=role?"":"none";
   dlgCurrentRole=currentRole;if(role)dlgRole.value=role;dlgOnConfirm=onConfirm;syncDialog();overlay.style.display="flex";
 }
 function syncDialog(){dlgConfirm.disabled=!!dlgCurrentRole&&dlgRole.value===dlgCurrentRole;}
 dlgRole.onchange=syncDialog;dlgCancel.onclick=closeDialog;
 overlay.onclick=e=>{if(e.target===overlay)closeDialog();};
 overlay.addEventListener("keydown",e=>{if(e.key==="Escape")closeDialog();});
 dlgConfirm.onclick=()=>{const fn=dlgOnConfirm,role=dlgRole.value;closeDialog();if(fn)fn(role);};
 q("[data-info]").onclick=()=>{const info=document.createElement("div");info.className="info";info.innerHTML=INFO_HTML;ask({title:"About User Access Manager",body:info,cancelLabel:"Close"});};
 // Pending changes are kept client-side until Save; a new review discards them.
 function describe(p){return p.action==="remove"?`Remove direct ${p.from} access`:`Change role ${p.from} → ${p.role}`;}
 // Fabric reuses the principal ID as the assignment ID in every workspace, so key by both.
 const keyOf=r=>`${r.workspace_id}|${r.assignment_id}`;
 function setPending(row,change){if(change)state.pending.set(keyOf(row),{workspace_id:row.workspace_id,assignment_id:row.assignment_id,workspace:row.workspace,from:row.role,...change});else state.pending.delete(keyOf(row));if(state.refreshTable)state.refreshTable();}
 function save(){dispatch("save",{changes:[...state.pending.values()].map(({action,workspace_id,assignment_id,role})=>({action,workspace_id,assignment_id,role:role||""}))});}
 function showPending(){
   const data=model.get("data")||{},list=document.createElement("ul");list.className="pending-list";
   for(const p of state.pending.values()){const li=document.createElement("li");const what=text("span",`${p.workspace} — ${describe(p)}`);what.className="what";
     const undo=iconButton(`__UNDO__`,"Undo this change");undo.onclick=()=>{state.pending.delete(keyOf(p));if(state.refreshTable)state.refreshTable();if(state.pending.size)showPending();else closeDialog();};
     li.append(dot(),what,undo);list.appendChild(li);}
   const impact=data.principal&&data.principal.type==="Group"?" These changes affect the group and its members.":"";
   ask({title:"Pending changes",message:`Saving applies all ${plural(state.pending.size,"change")} at once. Other access paths may remain.${impact}`,body:list,cancelLabel:"Close",confirmLabel:"Save changes",onConfirm:save});
 }
 const COLUMNS=[{key:"workspace",label:"Workspace"},{key:"workspace_id",label:"Workspace ID",cls:"id"},{key:"path",label:"Access through"},{key:"role",label:"Role"},{key:"actions",label:"Actions",cls:"actions"}];
 function sortedRows(rows){
   const f=state.filter.trim().toLowerCase(),{key,dir}=state.sort;
   const out=f?rows.filter(r=>[r.workspace,r.workspace_id,r.path,r.role].some(v=>String(v||"").toLowerCase().includes(f))):rows.slice();
   const val=r=>key==="role"?(RANK[r.role]||0):String(r[key]||"").toLowerCase();
   return out.sort((a,b)=>{const x=val(a),y=val(b);return (x<y?-1:x>y?1:0)*dir||String(a.workspace).localeCompare(String(b.workspace));});
 }
 function draw(){
   const data=model.get("data")||{}, target=q("[data-result]");target.replaceChildren();q("[data-review]").disabled=state.busy;state.refreshTable=null;
   const status=model.get("status")||{};q("[data-status]").textContent=status.message||"";q("[data-status]").className=status.error?"error":"note";
   const graph=model.get("graph_status")||{}, pill=q("[data-graph-pill]");
   pill.textContent=graph.connected?(graph.source==="key_vault"?"Connected via Key Vault":"Connected"):"Not connected · direct assignments only";pill.className=graph.connected?"pill on":"pill";
   q("[data-graph-connect]").disabled=state.busy;q("[data-graph-disconnect]").disabled=state.busy||graph.source!=="key_vault";
   if(!data.principal)return;
   target.appendChild(text("h3",`${data.principal.name} (${data.principal.type}, ${data.principal.id})`));
   const s=data.summary, isGroup=data.principal.type==="Group";
   if(s){
    const items=[["Workspaces with access",s.workspaces_with_access],["Direct assignments",s.direct],["Via groups",s.inherited],
     ["Admin",s.roles.Admin],["Member",s.roles.Member],["Contributor",s.roles.Contributor],["Viewer",s.roles.Viewer],
     ["Workspaces reviewed",`${s.reviewed} / ${s.visible}`],[isGroup?"Parent groups":"Group memberships",s.groups==null?"n/a":s.groups]];
    if(isGroup)items.push(["Transitive members",s.members]);
    const cards=document.createElement("div");cards.className="cards";
    for(const [label,value] of items){const c=document.createElement("div");c.className="card";const l=text("div",label);l.className="card-label";const v=text("div",String(value));v.className="card-value";c.append(l,v);cards.appendChild(c);}
    target.appendChild(cards);
    const roleNote=text("div","Role counts use the highest role per workspace across all access paths.");roleNote.className="note";target.appendChild(roleNote);
   }
   if(data.info){const i=text("div",data.info);i.className="note";target.appendChild(i);}
   if(data.warnings.length){const w=text("div",data.warnings.join("\n"));w.className="error";target.appendChild(w);}
   const who=`${data.principal.name} (${data.principal.id})`, impact=isGroup?" This changes access for the group and its members.":"";
   const filterbar=document.createElement("div");filterbar.className="filterbar";
   const search=document.createElement("span");search.className="search";search.innerHTML=`__SEARCH_ICON__`;
   const filter=document.createElement("input");filter.type="search";filter.placeholder="Filter by workspace, ID, access or role…";filter.setAttribute("aria-label","Filter assignments");filter.value=state.filter;
   search.appendChild(filter);const count=text("span","");count.className="note";const spacer=text("span","");spacer.className="spacer";
   const pendingBtn=document.createElement("button");pendingBtn.type="button";pendingBtn.className="pending-btn";pendingBtn.title="Show pending changes";pendingBtn.onclick=showPending;
   const discardBtn=iconButton(`__ERASER__`,"Discard all pending changes","bar-btn");discardBtn.onclick=()=>{state.pending.clear();state.refreshTable();};
   const saveBtn=iconButton(`__SAVE__`,"Save all pending changes","bar-btn primary");saveBtn.onclick=save;
   filterbar.append(search,count,spacer,pendingBtn,discardBtn,saveBtn);target.appendChild(filterbar);
   const table=document.createElement("table");table.className="rows";const thead=document.createElement("thead"),headRow=document.createElement("tr");
   for(const col of COLUMNS){const th=document.createElement("th");
     if(col.key==="actions"){th.textContent=col.label;}
     else{const b=document.createElement("button");b.type="button";b.className="sort";b.appendChild(text("span",col.label));const icon=document.createElement("span");icon.className="sort-icon";b.appendChild(icon);
       b.onclick=()=>{state.sort=state.sort.key===col.key?{key:col.key,dir:-state.sort.dir}:{key:col.key,dir:1};renderBody();};th.appendChild(b);th._icon=icon;}
     th._key=col.key;headRow.appendChild(th);}
   thead.appendChild(headRow);const body=document.createElement("tbody");table.append(thead,body);
   function actionsCell(row){const cell=document.createElement("td");cell.className="actions";
     if(!(row.direct&&row.assignment_id)){cell.textContent=row.direct?"Assignment ID unavailable; read-only":"Inherited / read-only";return cell;}
     const p=state.pending.get(keyOf(row)),where=`${row.workspace} (${row.workspace_id})`;
     const change=iconButton(`__PENCIL__`,"Change role");change.disabled=state.busy||(p&&p.action==="remove");
     change.onclick=()=>{const effective=p&&p.role?p.role:row.role;
       ask({title:"Change role",message:`Choose a new direct role for ${who} in ${where}. Current role: ${row.role}. The change stays pending until you click Save.${impact}`,confirmLabel:"Apply",role:effective,currentRole:effective,
         onConfirm:role=>setPending(row,role===row.role?null:{action:"update",role})});};
     const remove=iconButton(`__TRASH__`,"Remove direct access","danger");remove.disabled=state.busy||(p&&p.action==="remove");
     remove.onclick=()=>setPending(row,{action:"remove",role:""});
     cell.append(change,remove);
     if(p){const undo=iconButton(`__UNDO__`,"Undo pending change");undo.disabled=state.busy;undo.onclick=()=>setPending(row,null);cell.appendChild(undo);}
     return cell;}
   function renderBody(){
     for(const th of headRow.children){if(!th._icon)continue;const active=state.sort.key===th._key;
       th.setAttribute("aria-sort",active?(state.sort.dir>0?"ascending":"descending"):"none");th._icon.innerHTML=active?(state.sort.dir>0?`__SORT_ASC__`:`__SORT_DESC__`):"";}
     const rows=sortedRows(data.rows);body.replaceChildren();
     for(const row of rows){const tr=document.createElement("tr"),p=state.pending.get(keyOf(row));
       if(p)tr.className=p.action==="remove"?"pending-remove":"pending-update";
       for(const col of COLUMNS.slice(0,4)){const td=text("td",p&&col.key==="workspace"?"":row[col.key]);if(col.cls)td.className=col.cls;td.title=String(row[col.key]||"");tr.appendChild(td);}
       if(p){tr.children[0].append(dot(),text("span",row.workspace));tr.children[0].title=`${row.workspace} — pending: ${describe(p)}`;}
       if(p&&p.action==="update"){const roleCell=tr.children[3];roleCell.textContent=`${row.role} → `;roleCell.title=`${row.role} → ${p.role} (pending)`;const nr=text("span",p.role);nr.className="new-role";roleCell.appendChild(nr);}
       tr.appendChild(actionsCell(row));body.appendChild(tr);}
     count.textContent=rows.length===data.rows.length?plural(rows.length,"assignment"):`${rows.length} of ${data.rows.length} assignments`;
     const n=state.pending.size;pendingBtn.replaceChildren(...(n?[dot()]:[]),text("span",n?plural(n,"pending change"):"No pending changes"));
     pendingBtn.disabled=state.busy||!n;discardBtn.disabled=state.busy||!n;saveBtn.disabled=state.busy||!n;
   }
   state.refreshTable=renderBody;
   filter.oninput=()=>{state.filter=filter.value;renderBody();};
   renderBody();
   const scroll=document.createElement("div");scroll.className="scroll";scroll.appendChild(table);target.appendChild(scroll);
   // Seed starting widths instead of measuring content: long IDs/names would otherwise starve the Role column,
   // truncating pending "old → new" roles. Columns the user resizes keep their saved widths.
   const avail=(scroll.clientWidth||0)-8;
   if(avail>0){const fixed={workspace_id:290,role:200,actions:120},rest=Math.max(280,avail-290-200-120),ws=Math.round(rest*0.55),flex={workspace:ws,path:rest-ws};
     for(const th of headRow.children)th.dataset.columnWidth=fixed[th._key]||flex[th._key];}
   sllsInstallColumnResizers(table,{widths:state.widths,key:"uam-rows",minWidth:72});
   if(!data.rows.length)target.appendChild(text("p","No matching visible assignments found; this is not proof of no access."));
   if(isGroup){target.appendChild(text("h3","Transitive group members (nested; details may be restricted)"));const list=document.createElement("ul");for(const m of data.members)list.appendChild(text("li",`${m.name} — ${m.type} — ${m.upn} (${m.id})`));target.appendChild(list);}
 }
 function review(){dispatch("review",{kind:q("[data-kind]").value,identity:q("[data-identity]").value});}
 q("[data-review]").onclick=()=>{
   if(!state.pending.size){review();return;}
   ask({title:"Discard pending changes?",message:`Reviewing again discards ${plural(state.pending.size,"pending change")}.`,confirmLabel:"Discard and review",danger:true,onConfirm:review});};
 q("[data-graph-connect]").onclick=()=>{const cfg={};for(const [k,s] of Object.entries(kvFields))cfg[k]=q(s).value.trim();dispatch("graph_connect",cfg);};
 q("[data-graph-disconnect]").onclick=()=>dispatch("graph_disconnect");
 model.on("change:data",()=>{state.pending.clear();draw();});model.on("change:graph_status",draw);model.on("change:status",()=>{state.busy=false;draw();});draw();
}
export default { render };
"""
for key, value in {
    "__FULLSCREEN__": fullscreen_setup_js(), "__RESIZE__": TABLE_COLUMN_RESIZE_JS,
    "__ATTR__": render_attribution_html(), "__SUN__": ICONS["sun"],
    "__MOON__": ICONS["moon"], "__FS__": ICONS["fullscreen"],
    "__FS_EXIT__": ICONS["fullscreen_exit"], "__ICON__": ICONS["user_access_manager"],
    "__INFO__": ICONS["info"], "__GRAPH__": ICONS["microsoft_graph"],
    "__PENCIL__": ICONS["pencil"], "__TRASH__": ICONS["trash"], "__UNDO__": ICONS["undo"],
    "__ERASER__": ICONS["eraser"], "__SAVE__": ICONS["save"],
    "__SEARCH_ICON__": ICONS["search"], "__SORT_ASC__": ICONS["sort_asc"], "__SORT_DESC__": ICONS["sort_desc"],
}.items():
    _JS = _JS.replace(key, value)


@log
def user_access_manager(
    dark_mode: bool = False,
    key_vault_uri: Optional[str] = None,
    key_vault_tenant_id: Optional[str] = None,
    key_vault_client_id: Optional[str] = None,
    key_vault_client_secret: Optional[str] = None,
) -> None:
    """
    Display an interactive workspace access review and administration tool.

    Reviews which caller-visible workspaces a user or group can access, directly
    or through group membership, and lets you change or remove the principal's
    direct role assignments. Changes are queued as pending changes and applied
    together on Save. Listing workspace assignments requires Member or higher
    access. Rate limits and failures are reported per workspace, so results can
    be incomplete. EntireTenant assignments, other principal types, apps, items,
    tenant settings, grant dates and grantors are not supported. Group
    inheritance is read-only unless the group itself is selected as the
    principal. Removing one assignment does not necessarily remove effective
    access.

    Microsoft Graph is optional. Connecting it (via the ``key_vault_*``
    parameters, the tool's "Microsoft Graph authentication" panel or an enclosing
    ``service_principal_authentication`` context) adds group-inherited access and
    group members, and requires Graph application permissions, including
    Member.Read.Hidden for hidden membership. Without it, only direct assignments
    are shown (users matched by UPN or object ID, groups by object ID).

    Uses `workspace role assignments <https://learn.microsoft.com/rest/api/fabric/core/workspaces/list-workspace-role-assignments>`_
    and `transitive membership <https://learn.microsoft.com/graph/api/user-list-transitivememberof>`_.

    Parameters
    ----------
    dark_mode : bool, default=False
        Whether to initially use the dark theme.
    key_vault_uri : str, default=None
        Azure Key Vault URI holding the service principal secrets used only for
        Microsoft Graph calls. Workspace listing and role changes still run as the
        signed-in user. These values can also be entered (or changed) in the
        tool's "Microsoft Graph authentication" panel.
    key_vault_tenant_id : str, default=None
        Name of the secret in the Key Vault with the Tenant ID.
    key_vault_client_id : str, default=None
        Name of the secret in the Key Vault with the Service Principal Client ID.
    key_vault_client_secret : str, default=None
        Name of the secret in the Key Vault with the Service Principal Client Secret.
    """
    try:
        import anywidget
        import traitlets
    except ImportError as exc:
        raise ImportError("user_access_manager requires anywidget; install it with: pip install anywidget") from exc
    from IPython.display import display

    kv_keys = ("key_vault_uri", "key_vault_tenant_id", "key_vault_client_id", "key_vault_client_secret")
    graph_config = dict(zip(kv_keys, (key_vault_uri, key_vault_tenant_id, key_vault_client_id, key_vault_client_secret)))
    graph_credential = _graph_credential_from_key_vault(**graph_config)

    def graph_status():
        if graph_credential is not None:
            return {"connected": True, "source": "key_vault"}
        return {"connected": _graph_available(), "source": "context" if _graph_available() else ""}

    class _Widget(anywidget.AnyWidget):
        _esm = _JS
        _css = _CSS
        data = traitlets.Dict().tag(sync=True)
        status = traitlets.Dict().tag(sync=True)
        pending_action = traitlets.Dict().tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        # Holds Key Vault URI and secret *names* only; secret values never reach the browser.
        graph_config = traitlets.Dict().tag(sync=True)
        graph_status = traitlets.Dict().tag(sync=True)

    widget = _Widget(
        dark_mode=dark_mode,
        graph_config={k: v or "" for k, v in graph_config.items()},
        graph_status=graph_status(),
    )
    review = None
    last_request = None

    def on_graph(action):
        nonlocal graph_credential
        try:
            if action.get("action") == "graph_connect":
                config = {k: (action.get(k) or "").strip() for k in kv_keys}
                credential = _graph_credential_from_key_vault(**config)
                if credential is None:
                    raise ValueError("Enter the Key Vault URI and the three secret names.")
                credential.get_token("graph")
                graph_credential = credential
                widget.graph_config = config
                message = "Connected to Microsoft Graph via Key Vault. Review again to include group access."
            else:
                graph_credential = None
                message = "Disconnected from Microsoft Graph."
            widget.graph_status = graph_status()
            widget.status = {"message": message, "error": False, "run": widget.run}
        except Exception as exc:
            widget.status = {"message": f"Microsoft Graph connection failed: {exc}", "error": True, "run": widget.run}

    def refresh():
        nonlocal review
        review = None
        review = _review(*last_request, graph_credential)
        widget.data = review

    def on_save(changes):
        nonlocal review
        try:
            failures = _apply(review, changes)
        except Exception as exc:
            # Validation failed before anything was applied; keep the review and pending changes.
            widget.status = {"message": f"No changes were saved. {exc}", "error": True, "run": widget.run}
            return
        saved = len(changes) - len(failures)
        try:
            refresh()
        except Exception as exc:
            review = None
            widget.data = {}
            failures.append(f"Refreshing the review failed; review again: {exc}")
        message = f"Saved {saved} of {len(changes)} change{'' if len(changes) == 1 else 's'}. Other access paths may remain."
        if failures:
            message += "\nFailed:\n" + "\n".join(failures)
        widget.status = {"message": message, "error": bool(failures), "run": widget.run}

    def on_run(_change):
        nonlocal review, last_request
        action = dict(widget.pending_action)
        name = action.get("action")
        if name in ("graph_connect", "graph_disconnect"):
            on_graph(action)
            return
        if name == "save":
            on_save(list(action.get("changes") or []))
            return
        try:
            if name != "review":
                raise ValueError("Unknown action.")
            review = None
            widget.data = {}
            last_request = (action.get("kind"), action.get("identity", ""))
            refresh()
            widget.status = {"message": "Review complete. Check warnings and visibility limits.", "error": False, "run": widget.run}
        except Exception as exc:
            review = None
            widget.data = {}
            widget.status = {"message": f"Operation failed; review again. {exc}", "error": True, "run": widget.run}

    widget.observe(on_run, names=["run"])
    display(widget)
