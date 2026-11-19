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


def test_review_group_resolution_failure_is_explicit(monkeypatch):
    review = sample_review(monkeypatch, fail_groups=True)
    assert len(review["rows"]) == 1
    assert any("inherited access is incomplete" in warning for warning in review["warnings"])


def test_group_members_failure_does_not_show_partial_results(monkeypatch):
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


def test_mutations_require_direct_reviewed_assignment(monkeypatch):
    review = sample_review(monkeypatch)
    api = Mock()
    monkeypatch.setattr(manager, "_base_api", api)
    for action in ("update", "remove"):
        with pytest.raises(ValueError, match="direct assignment"):
            manager._mutate(review, action, WS, INHERITED, "Admin")
    with pytest.raises(ValueError, match="not successfully reviewed"):
        manager._mutate(review, "grant", WS2, role="Admin")
    with pytest.raises(ValueError, match="Invalid workspace role"):
        manager._mutate(review, "update", WS, DIRECT, "Owner")
    with pytest.raises(ValueError, match="already exists"):
        manager._mutate(review, "grant", WS, role="Viewer")
    api.assert_not_called()
    manager._mutate(review, "update", WS, DIRECT, "Member")
    assert api.call_args.kwargs["request"].endswith("/" + DIRECT)
    assert api.call_args.kwargs["payload"] == {"role": "Member"}
    manager._mutate(review, "remove", WS, DIRECT)
    assert api.call_args.kwargs["request"].endswith("/" + DIRECT)


def test_grant_uses_principal_id_and_type(monkeypatch):
    review = sample_review(monkeypatch)
    review["rows"] = []
    api = Mock()
    monkeypatch.setattr(manager, "_base_api", api)
    manager._mutate(review, "grant", WS, role="Viewer")
    assert api.call_args.kwargs["payload"] == {
        "principal": {"id": USER, "type": "User"}, "role": "Viewer"
    }


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
    assert 'confirm(' in js and 'row.direct&&row.assignment_id' in js
    assert "__FULLSCREEN__" not in js and "__SEARCH__" not in js


def test_workspace_picker_is_reused_and_selection_survives_redraws():
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
global.confirm=()=>true;
global.alert=()=>{throw new Error("Unexpected alert");};
"""
    scenario = r"""
const values={workspace_id:"one",dark_mode:false,run:0,data:{},status:{}};
const listeners={};
const model={get:key=>values[key],set:(key,value)=>{values[key]=value;},
  on:(event,fn)=>{listeners[event]=fn;},save_changes(){}};
const host=new Element("host");
render({model,el:host});
const data={principal:{id:"p",name:"Person",type:"User"},warnings:[],rows:[],members:[],
  workspaces:[{id:"one",name:"One"},{id:"two",name:"Two"}],reviewed_workspace_ids:["one","two"]};
values.data=data;listeners["change:data"]();
assert.equal(__sllsSsOpen.size,1);
let picker=host.querySelector(".slls-ss");
assert.ok(picker);
assert.equal(host.querySelector(".slls-ss-value").textContent,"One (one)");
let options=host.querySelectorAll(".slls-ss-opt");
options[1].fire("click");
for(let i=0;i<20;i++)listeners["change:status"]();
assert.equal(__sllsSsOpen.size,1);
assert.equal(host.querySelector(".slls-ss"),picker);
assert.equal(host.querySelector(".slls-ss-value").textContent,"Two (two)");
host.querySelector("[data-grant]").onclick();
assert.equal(values.pending_action.workspace_id,"two");
values.data={};listeners["change:data"]();
values.data=data;listeners["change:data"]();listeners["change:status"]();
assert.equal(__sllsSsOpen.size,1);
assert.equal(host.querySelector(".slls-ss"),picker);
assert.equal(host.querySelector(".slls-ss-value").textContent,"One (one)");
host.querySelectorAll(".slls-ss-opt")[1].fire("click");
data.reviewed_workspace_ids=["one"];
listeners["change:data"]();
assert.equal(host.querySelector(".slls-ss-value").textContent,"One (one)");
host.querySelector("[data-grant]").onclick();
assert.equal(values.pending_action.workspace_id,"one");
"""
    js = manager._JS.replace("export default { render };", "")
    result = subprocess.run(
        ["node"], input=dom + js + scenario, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_callback_clears_stale_state_after_failure_and_mutation(monkeypatch):
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
    monkeypatch.setattr(manager, "resolve_workspace_name_and_id", lambda _: ("Workspace", WS))
    review = {"principal": {"id": USER, "type": "User"}, "rows": []}
    loader = Mock(side_effect=[review, RuntimeError("Graph denied"), review])
    monkeypatch.setattr(manager, "_review", loader)
    mutation = Mock()
    monkeypatch.setattr(manager, "_mutate", mutation)
    func = getattr(manager.user_access_manager, "__wrapped__", manager.user_access_manager)
    func(workspace=WS)
    widget = display.call_args.args[0]
    widget.pending_action = {"action": "review", "kind": "User", "identity": USER}
    widget.run = 1
    widget.callback(None)
    assert widget.data == review
    widget.run = 2
    widget.callback(None)
    assert widget.data == {} and widget.status["error"]
    widget.run = 3
    widget.callback(None)
    widget.pending_action = {"action": "remove", "workspace_id": WS, "assignment_id": DIRECT}
    widget.run = 4
    widget.callback(None)
    mutation.assert_called_once()
    assert widget.data == {} and not widget.status["error"]
