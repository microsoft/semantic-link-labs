from unittest.mock import Mock
import shutil
import subprocess
import sys
import types

import pytest

from sempy_labs.workspace import _user_access_manager as manager


USER = "11111111-1111-1111-1111-111111111111"
GROUP = "22222222-2222-2222-2222-222222222222"
WS = "33333333-3333-3333-3333-333333333333"
WS2 = "44444444-4444-4444-4444-444444444444"
DIRECT = "55555555-5555-5555-5555-555555555555"
INHERITED = "66666666-6666-6666-6666-666666666666"


def response(data):
    return Mock(json=Mock(return_value=data))


def test_graph_pages_validate_and_check_every_request(monkeypatch):
    api = Mock(side_effect=[
        response({"value": [{"id": 1}], "@odata.nextLink": "https://graph.microsoft.com/v1.0/users?$skiptoken=next"}),
        RuntimeError("429 throttled"),
    ])
    monkeypatch.setattr(manager, "_base_api", api)
    with pytest.raises(RuntimeError, match="429"):
        list(manager._pages("users", "graph"))
    assert api.call_args_list[1].kwargs["request"] == "users?$skiptoken=next"
    api.reset_mock(side_effect=True)
    api.return_value = response({"value": [], "@odata.nextLink": "https://evil.example/v1.0/users"})
    with pytest.raises(ValueError, match="continuation"):
        list(manager._pages("users", "graph"))
    assert api.call_count == 1


def test_fabric_page_failure_never_returns_complete_inventory(monkeypatch):
    api = Mock(side_effect=[
        response({"value": [{"id": WS}], "continuationToken": "token",
                  "continuationUri": "https://api.fabric.microsoft.com/v1/workspaces?continuationToken=token"}),
        RuntimeError("429 throttled"),
    ])
    monkeypatch.setattr(manager, "_base_api", api)
    with pytest.raises(RuntimeError, match="429"):
        list(manager._pages("/v1/workspaces", "fabric_sp"))
    assert api.call_count == 2


def test_principal_quotes_upn_and_rejects_bad_group(monkeypatch):
    api = Mock(return_value=response({"id": USER, "displayName": "User"}))
    monkeypatch.setattr(manager, "_base_api", api)
    manager._principal("User", "name+tag#EXT#@tenant.example")
    assert api.call_args.kwargs["request"] == "users/name%2Btag%23EXT%23%40tenant.example"
    with pytest.raises(ValueError):
        manager._principal("Group", "group/name")
    for bad in ("..", "name/other", "name\\other"):
        with pytest.raises(ValueError, match="Invalid user"):
            manager._principal("User", bad)


def sample_review(monkeypatch, fail_groups=False):
    monkeypatch.setattr(manager, "_graph_available", lambda *_: True)
    monkeypatch.setattr(manager, "_principal", lambda kind, identity: {"id": USER, "name": "User", "type": "User"})

    def pages(request, client):
        if client == "graph":
            if fail_groups:
                raise RuntimeError("Graph denied")
            return iter([{"id": GROUP, "displayName": "Nested group", "@odata.type": "#microsoft.graph.group"}])
        if request == "/v1/workspaces":
            return iter([{"id": WS, "displayName": "Sales"}, {"id": WS2, "displayName": "Denied"}])
        if WS2 in request:
            raise RuntimeError("403 denied")
        return iter([
            {"id": DIRECT, "principal": {"id": USER, "type": "User"}, "role": "Viewer"},
            {"id": INHERITED, "principal": {"id": GROUP, "type": "Group"}, "role": "Member"},
        ])

    monkeypatch.setattr(manager, "_pages", pages)
    return manager._review("User", "user@example.com")


def test_review_direct_nested_group_and_per_workspace_failure(monkeypatch):
    review = sample_review(monkeypatch)
    assert [row["direct"] for row in review["rows"]] == [True, False]
    assert GROUP in review["rows"][1]["path"]
    assert review["reviewed_workspace_ids"] == [WS]
    assert "403 denied" in review["warnings"][0]
    summary = review["summary"]
    assert (summary["workspaces_with_access"], summary["direct"], summary["inherited"]) == (1, 1, 1)
    assert summary["roles"] == {"Admin": 0, "Member": 1, "Contributor": 0, "Viewer": 0}
    assert (summary["reviewed"], summary["visible"], summary["groups"]) == (1, 2, 1)


def test_review_group_resolution_failure_is_explicit(monkeypatch):
    review = sample_review(monkeypatch, fail_groups=True)
    assert len(review["rows"]) == 1
    assert any("inherited access is incomplete" in warning for warning in review["warnings"])


def test_group_members_failure_does_not_show_partial_results(monkeypatch):
    monkeypatch.setattr(manager, "_graph_available", lambda *_: True)
    monkeypatch.setattr(manager, "_principal", lambda *_: {"id": GROUP, "name": "Group", "type": "Group"})

    def pages(request, client):
        if request.endswith("transitiveMembers"):
            yield {"id": USER}
            raise RuntimeError("page 2 failed")
        return
        yield

    monkeypatch.setattr(manager, "_pages", pages)
    result = manager._review("Group", GROUP)
    assert result["members"] == []
    assert "page 2 failed" in result["warnings"][0]


def test_review_without_service_principal_skips_graph_and_matches_upn(monkeypatch):
    token = manager.auth.token_provider.set(None)
    requests = []

    def pages(request, client):
        requests.append(client)
        if request == "/v1/workspaces":
            return iter([{"id": WS, "displayName": "Sales"}])
        return iter([
            {"id": DIRECT, "role": "Viewer", "principal": {
                "id": USER, "type": "User", "displayName": "Jane",
                "userDetails": {"userPrincipalName": "Jane@Example.com"}}},
            {"id": INHERITED, "role": "Member", "principal": {"id": GROUP, "type": "Group"}},
        ])

    monkeypatch.setattr(manager, "_pages", pages)
    try:
        review = manager._review("User", "jane@example.com")
    finally:
        manager.auth.token_provider.reset(token)
    assert "graph" not in requests
    assert review["principal"] == {"id": USER, "type": "User", "name": "Jane"}
    assert [row["assignment_id"] for row in review["rows"]] == [DIRECT]
    assert "service principal" in review["info"] and review["warnings"] == []


def test_key_vault_credential_is_scoped_to_graph_calls(monkeypatch):
    credential = object()
    seen = []
    monkeypatch.setattr(manager, "_principal", lambda kind, identity: (
        seen.append(("principal", manager.auth.token_provider.get())) or {"id": USER, "name": "User", "type": "User"}))

    def pages(request, client):
        seen.append((client, manager.auth.token_provider.get()))
        return iter([{"id": WS, "displayName": "Sales"}] if request == "/v1/workspaces" else [])

    monkeypatch.setattr(manager, "_pages", pages)
    review = manager._review("User", USER, credential)
    assert seen == [("principal", credential), ("graph", credential), ("fabric_sp", None), ("fabric_sp", None)]
    assert review["warnings"] == []
    assert manager.auth.token_provider.get() is None

    with pytest.raises(ValueError, match="together"):
        manager._graph_credential_from_key_vault("https://kv.vault.azure.net/", "tenant", None, None)
    assert manager._graph_credential_from_key_vault(None, None, None, None) is None


def test_mutations_require_direct_reviewed_assignment(monkeypatch):
    review = sample_review(monkeypatch)
    api = Mock()
    monkeypatch.setattr(manager, "_base_api", api)
    for action in ("update", "remove"):
        with pytest.raises(ValueError, match="direct assignment"):
            manager._mutate(review, action, WS, INHERITED, "Admin")
    with pytest.raises(ValueError, match="not successfully reviewed"):
        manager._mutate(review, "update", WS2, DIRECT, "Admin")
    with pytest.raises(ValueError, match="Invalid workspace role"):
        manager._mutate(review, "update", WS, DIRECT, "Owner")
    with pytest.raises(ValueError, match="Unknown action"):
        manager._mutate(review, "grant", WS, DIRECT, "Viewer")
    manager._mutate(review, "remove", WS, DIRECT, dry_run=True)
    api.assert_not_called()
    manager._mutate(review, "update", WS, DIRECT, "Member")
    assert api.call_args.kwargs["request"].endswith("/" + DIRECT)
    assert api.call_args.kwargs["payload"] == {"role": "Member"}
    manager._mutate(review, "remove", WS, DIRECT)
    assert api.call_args.kwargs["request"].endswith("/" + DIRECT)


def test_apply_validates_every_change_before_applying_any(monkeypatch):
    review = sample_review(monkeypatch)
    api = Mock()
    monkeypatch.setattr(manager, "_base_api", api)
    good = {"action": "update", "workspace_id": WS, "assignment_id": DIRECT, "role": "Member"}
    with pytest.raises(ValueError, match="direct assignment"):
        manager._apply(review, [good, {"action": "remove", "workspace_id": WS, "assignment_id": INHERITED}])
    with pytest.raises(ValueError, match="one pending change"):
        manager._apply(review, [good, {**good, "action": "remove"}])
    with pytest.raises(ValueError, match="no pending changes"):
        manager._apply(review, [])
    api.assert_not_called()
    # Fabric reuses the principal ID as the assignment ID in every workspace.
    review["reviewed_workspace_ids"].append(WS2)
    review["rows"].append({**review["rows"][0], "workspace_id": WS2, "workspace": "Denied"})
    assert manager._apply(review, [good, {**good, "workspace_id": WS2}]) == []
    assert [c.kwargs["request"] for c in api.call_args_list] == [
        f"/v1/workspaces/{WS}/roleAssignments/{DIRECT}", f"/v1/workspaces/{WS2}/roleAssignments/{DIRECT}"]
    api.side_effect = [None, RuntimeError("403 denied")]
    assert manager._apply(review, [good, {**good, "workspace_id": WS2}]) == ["Denied: 403 denied"]


def test_selected_group_direct_assignment_can_be_changed(monkeypatch):
    review = sample_review(monkeypatch)
    review["principal"] = {"id": GROUP, "name": "Group", "type": "Group"}
    review["rows"][1]["direct"] = True
    api = Mock()
    monkeypatch.setattr(manager, "_base_api", api)
    manager._mutate(review, "remove", WS, INHERITED)
    assert api.call_args.kwargs["request"].endswith("/" + INHERITED)
    assert "group and its members" in manager._JS


def test_widget_discloses_limits_and_confirms_mutations():
    js = manager._JS
    assert "Member.Read.Hidden" in js and "EntireTenant" in js
    assert "caller-visible" in js and "Other access paths may remain" in js
    assert "confirm(" not in js and "alert(" not in js
    assert "ask({" in js and 'row.direct&&row.assignment_id' in js
    assert "__FULLSCREEN__" not in js and "__RESIZE__" not in js and "data-grant" not in js


def test_widget_queues_changes_until_save():
    if not shutil.which("node"):
        pytest.skip("Node.js is required for the widget JavaScript test")
    dom = r"""
const assert = require("node:assert/strict");
class Element {
  constructor(tag) {
    this.tag=tag; this.children=[]; this.parent=null; this.attrs={}; this.listeners={};
    this.style={}; this.value=""; this.className="";
    const classes=new Set();
    this.classList={add:c=>classes.add(c),remove:c=>classes.delete(c),
      contains:c=>classes.has(c),toggle:(c,on)=>{if(on)classes.add(c);else classes.delete(c);}};
  }
  set innerHTML(html) {
    this.children=[];
    for (const match of html.matchAll(/<(button|input|div|span|select)\b[^>]*\b(data-[\w-]+)/g)) {
      const child=new Element(match[1]); child.attrs[match[2]]="";
      if(match[2]==="data-role")child.value="Viewer";
      this.appendChild(child);
    }
  }
  setAttribute(name,value){this.attrs[name]=value;}
  appendChild(child){
    if(child.parent)child.parent.children=child.parent.children.filter(c=>c!==child);
    child.parent=this;this.children.push(child);return child;
  }
  append(...children){children.forEach(child=>this.appendChild(child));}
  replaceChildren(...children){this.children.forEach(c=>c.parent=null);this.children=[];this.append(...children);}
  removeChild(child){this.children=this.children.filter(c=>c!==child);child.parent=null;}
  get firstChild(){return this.children[0]||null;}
  querySelector(selector){return this.querySelectorAll(selector)[0]||null;}
  querySelectorAll(selector){
    const found=[];
    const matches=el=>selector.startsWith("[")?selector.slice(1,-1) in el.attrs:
      selector.startsWith(".")&&el.className.split(" ").includes(selector.slice(1));
    const walk=el=>{for(const child of el.children){if(matches(child))found.push(child);walk(child);}};
    walk(this);return found;
  }
  addEventListener(name,fn){this.listeners[name]=fn;}
  fire(name){if(this.listeners[name])this.listeners[name]({stopPropagation(){},preventDefault(){}});}
  getBoundingClientRect(){return {bottom:0};}
  focus(){}
  scrollIntoView(){}
}
global.document={createElement:tag=>new Element(tag),addEventListener(){}};
global.window={innerHeight:1000};
global.confirm=()=>{throw new Error("Native confirm must not be used");};
global.alert=()=>{throw new Error("Native alert must not be used");};
"""
    scenario = r"""
const values={dark_mode:false,run:0,data:{},status:{}};
const listeners={};
const model={get:key=>values[key],set:(key,value)=>{values[key]=value;},
  on:(event,fn)=>{listeners[event]=fn;},save_changes(){}};
const host=new Element("host");
render({model,el:host});
const byTag=(el,tag)=>{const out=[];const walk=e=>{for(const c of e.children){if(c.tag===tag)out.push(c);walk(c);}};walk(el);return out;};
const cells=()=>byTag(host,"td").map(td=>td.textContent);
const btn=title=>byTag(host,"button").find(b=>b.title===title);
const overlay=host.querySelector(".overlay"),dlgButtons=()=>host.querySelector(".dialog-actions").children;
host.querySelector("[data-info]").onclick();
assert.equal(overlay.style.display,"flex");assert.ok(host.querySelector(".info"));
dlgButtons()[0].onclick();assert.equal(overlay.style.display,"none");
const data={principal:{id:"p",name:"Person",type:"User"},warnings:[],members:[],workspaces:[],reviewed_workspace_ids:["one","two"],
  rows:[{workspace_id:"one",workspace:"Sales",assignment_id:"a1",role:"Viewer",path:"Direct",direct:true},
    {workspace_id:"two",workspace:"Finance",assignment_id:"a2",role:"Admin",path:"Via group: G (g)",direct:false},
    {workspace_id:"three",workspace:"Zeta",assignment_id:"a1",role:"Viewer",path:"Direct",direct:true}],
  summary:{workspaces_with_access:2,direct:1,inherited:1,roles:{Admin:1,Member:0,Contributor:0,Viewer:1},
    reviewed:2,visible:2,groups:null,members:0,warnings:0}};
values.data=data;listeners["change:data"]();
assert.equal(host.querySelectorAll(".card").length,9);
assert.equal(host.querySelectorAll(".card-value")[8].textContent,"n/a");
host.querySelector("[data-kv-uri]").value=" https://kv.vault.azure.net/ ";
host.querySelector("[data-graph-connect]").onclick();
assert.equal(values.pending_action.key_vault_uri,"https://kv.vault.azure.net/");
listeners["change:status"]();
assert.equal(cells()[0],"Finance");
const filter=host.querySelector(".search").children.find(c=>c.tag==="input");
filter.value="sales";filter.oninput();
assert.equal(cells()[0],"Sales");assert.equal(cells().length,5);
filter.value="";filter.oninput();
host.querySelectorAll(".sort")[3].onclick();assert.equal(cells()[0],"Sales");
host.querySelectorAll(".sort")[3].onclick();assert.equal(cells()[0],"Finance");
const saveBtn=()=>btn("Save all pending changes");
assert.equal(saveBtn().disabled,true);
const run=values.run;
btn("Change role").onclick();
const roleSelect=host.querySelector(".dialog").children.find(c=>c.tag==="label").children.find(c=>c.tag==="select");
assert.equal(roleSelect.value,"Viewer");assert.equal(dlgButtons()[1].disabled,true);
roleSelect.value="Member";roleSelect.onchange();dlgButtons()[1].onclick();
assert.equal(values.run,run);
assert.equal(host.querySelectorAll(".new-role").length,1);
assert.equal(host.querySelectorAll(".new-role")[0].textContent,"Member");
assert.equal(host.querySelectorAll(".dot").length,2);
assert.equal(saveBtn().disabled,false);
btn("Undo pending change").onclick();
assert.equal(host.querySelectorAll(".new-role").length,0);assert.equal(saveBtn().disabled,true);
btn("Remove direct access").onclick();
assert.equal(values.run,run);assert.equal(host.querySelectorAll(".pending-remove").length,1);
host.querySelector(".pending-btn").onclick();
assert.equal(byTag(host.querySelector(".pending-list"),"li").length,1);
dlgButtons()[0].onclick();
saveBtn().onclick();
assert.deepEqual(values.pending_action,{action:"save",changes:[{action:"remove",workspace_id:"one",assignment_id:"a1",role:""}]});
listeners["change:status"]();
host.querySelector("[data-review]").onclick();
assert.equal(overlay.style.display,"flex");assert.equal(values.pending_action.action,"save");
dlgButtons()[1].onclick();
assert.equal(values.pending_action.action,"review");
values.data={...data};listeners["change:data"]();listeners["change:status"]();
assert.equal(host.querySelectorAll(".pending-remove").length,0);
"""
    js = manager._JS.replace("export default { render };", "")
    result = subprocess.run(
        ["node"], input=dom + js + scenario, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def show_widget(monkeypatch, **kwargs):
    class Trait:
        def __init__(self, *args):
            pass

        def tag(self, **kwargs):
            return None

    class Widget:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

        def observe(self, fn, names):
            self.callback = fn

    display = Mock()
    ipython = types.ModuleType("IPython")
    ipython.__path__ = []
    ipython_display = types.ModuleType("IPython.display")
    ipython_display.display = display
    monkeypatch.setitem(sys.modules, "IPython", ipython)
    monkeypatch.setitem(sys.modules, "IPython.display", ipython_display)
    monkeypatch.setitem(sys.modules, "anywidget", types.SimpleNamespace(AnyWidget=Widget))
    monkeypatch.setitem(sys.modules, "traitlets", types.SimpleNamespace(
        Dict=Trait, Int=Trait, Bool=Trait, Unicode=Trait
    ))
    func = getattr(manager.user_access_manager, "__wrapped__", manager.user_access_manager)
    func(**kwargs)
    return display.call_args.args[0]


def test_graph_panel_connects_and_disconnects(monkeypatch):
    credential = Mock()
    factory = Mock(side_effect=[None, credential, RuntimeError("bad secret")])
    monkeypatch.setattr(manager, "_graph_credential_from_key_vault", factory)
    loader = Mock(return_value={"principal": {}})
    monkeypatch.setattr(manager, "_review", loader)
    widget = show_widget(monkeypatch)
    assert widget.graph_status == {"connected": False, "source": ""}

    config = {"key_vault_uri": "https://kv.vault.azure.net/", "key_vault_tenant_id": "t",
              "key_vault_client_id": "c", "key_vault_client_secret": "s"}
    widget.pending_action = {"action": "review", "kind": "User", "identity": USER}
    widget.run = 1
    widget.callback(None)
    widget.pending_action = {"action": "graph_connect", **config}
    widget.run = 2
    widget.callback(None)
    credential.get_token.assert_called_once_with("graph")
    assert widget.graph_status == {"connected": True, "source": "key_vault"}
    assert widget.graph_config == config and widget.data == {"principal": {}}

    widget.pending_action = {"action": "review", "kind": "User", "identity": USER}
    widget.run = 3
    widget.callback(None)
    assert loader.call_args.args[2] is credential

    widget.pending_action = {"action": "graph_connect", **config}
    widget.run = 4
    widget.callback(None)
    assert widget.status["error"] and "bad secret" in widget.status["message"]
    assert widget.graph_status["connected"]

    widget.pending_action = {"action": "graph_disconnect"}
    widget.run = 5
    widget.callback(None)
    assert widget.graph_status == {"connected": False, "source": ""}


def test_save_applies_pending_changes_then_refreshes(monkeypatch):
    review = {"principal": {"id": USER, "type": "User"}, "rows": []}
    refreshed = {**review, "rows": [{"workspace_id": WS}]}
    loader = Mock(side_effect=[review, refreshed, RuntimeError("Graph denied")])
    monkeypatch.setattr(manager, "_review", loader)
    apply = Mock(side_effect=[ValueError("stale review"), ["Sales: 403 denied"], []])
    monkeypatch.setattr(manager, "_apply", apply)
    widget = show_widget(monkeypatch)
    widget.pending_action = {"action": "review", "kind": "User", "identity": USER}
    widget.run = 1
    widget.callback(None)
    assert widget.data == review

    change = {"action": "remove", "workspace_id": WS, "assignment_id": DIRECT}
    widget.pending_action = {"action": "save", "changes": [change]}
    widget.run = 2
    widget.callback(None)
    assert widget.data == review and loader.call_count == 1
    assert widget.status["error"] and "No changes were saved" in widget.status["message"]

    widget.run = 3
    widget.callback(None)
    assert apply.call_args.args == (review, [change])
    assert loader.call_args.args == ("User", USER, None) and widget.data == refreshed
    assert widget.status["error"] and "Saved 0 of 1" in widget.status["message"]
    assert "Sales: 403 denied" in widget.status["message"]

    widget.run = 4
    widget.callback(None)
    assert widget.data == {} and "Refreshing the review failed" in widget.status["message"]
