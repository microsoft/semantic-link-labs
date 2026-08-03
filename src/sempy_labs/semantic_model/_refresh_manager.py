# flake8: noqa: E501
import threading
import time
import warnings
from typing import Any, Optional
from uuid import UUID
from sempy._utils._log import log
from sempy_labs._ui_components import (
    DARK_THEME_VARS as _UI_DARK_VARS,
    ICONS as _UI_ICONS,
    LIGHT_THEME_VARS as _UI_LIGHT_VARS,
    scoped_button_press_css as _ui_scoped_button_press_css,
)

_WIDGET_CSS = """
.slls-rm { __LIGHT_THEME__
    --rm-danger:#d92d20; --rm-danger-soft:rgba(217,45,32,.1);
    --rm-success:#16803c; --rm-success-soft:rgba(22,128,60,.12);
    width:100%; max-width:1180px; min-height:560px; margin:auto; overflow:hidden;
    border:1px solid var(--ui-border); border-radius:12px; background:var(--ui-bg);
    color:var(--ui-text); box-shadow:var(--ui-shadow-sm);
    font-family:-apple-system,BlinkMacSystemFont,"SF Pro Text","Helvetica Neue",Helvetica,Arial,sans-serif;
    -webkit-font-smoothing:antialiased;
}
.slls-rm.slls-rm-dark { __DARK_THEME__ --rm-danger:#ff6961; --rm-success:#5bd477; }
@media (prefers-color-scheme:dark) { .slls-rm.slls-rm-auto { __DARK_THEME__ --rm-danger:#ff6961; --rm-success:#5bd477; } }
.slls-rm * { box-sizing:border-box; }
.slls-rm:fullscreen,.slls-rm:-webkit-full-screen { width:100vw;height:100vh;max-width:none;border:0;border-radius:0;box-shadow:none; }
.slls-rm.slls-rm-fs { position:fixed;inset:0;z-index:2147483000;width:100vw;height:100vh;max-width:none;margin:0;border:0;border-radius:0;box-shadow:none; }
.slls-rm.slls-rm-fs .slls-rm-body,.slls-rm:fullscreen .slls-rm-body { height:calc(100vh - 69px);overflow:auto; }
.slls-rm button,.slls-rm input,.slls-rm select { font:inherit; }
.slls-rm-header { display:flex;align-items:center;gap:12px;padding:16px 20px;border-bottom:1px solid var(--ui-border); }
.slls-rm-mark { width:36px;height:36px;border-radius:8px;display:flex;align-items:center;justify-content:center;background:var(--ui-bg-secondary);color:var(--ui-accent); }
.slls-rm-mark svg { width:19px;height:19px; }
.slls-rm-heading { min-width:0;margin-right:auto; }
.slls-rm-title-row { display:flex;align-items:center;gap:8px; }
.slls-rm-title { margin:0;font-size:20px;line-height:1.2;font-weight:650;letter-spacing:0; }
.slls-rm-title-row .slls-rm-iconbtn { width:28px;height:28px; }
.slls-rm-subtitle { margin-top:3px;font-size:12.5px;color:var(--ui-text-secondary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap; }
.slls-rm-iconbtn { width:34px;height:34px;padding:0;border:1px solid var(--ui-border-strong);border-radius:8px;background:var(--ui-bg);color:var(--ui-text-secondary);display:inline-flex;align-items:center;justify-content:center;cursor:pointer; }
.slls-rm-iconbtn:hover { background:var(--ui-bg-secondary);color:var(--ui-text); }
.slls-rm-iconbtn:disabled { opacity:.42;cursor:not-allowed; }
.slls-rm-iconbtn svg { width:16px;height:16px; }
.slls-rm-body { padding:20px; }
.slls-rm-grid { display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:16px;align-items:start; }
.slls-rm-card,.slls-rm-panel { border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg-tertiary); }
.slls-rm-card { padding:16px; }
.slls-rm-card-head { display:flex;align-items:center;gap:8px;min-height:34px;margin-bottom:12px; }
.slls-rm-card-title { margin:0;margin-right:auto;font-size:14px;font-weight:650; }
.slls-rm-input,.slls-rm-select { width:100%;min-height:36px;padding:7px 10px;border:1px solid var(--ui-border-strong);border-radius:8px;background:var(--ui-bg);color:var(--ui-text);outline:none; }
.slls-rm-input:focus,.slls-rm-select:focus { border-color:var(--ui-accent);box-shadow:0 0 0 3px var(--ui-accent-soft); }
.slls-rm-search { position:relative;margin-bottom:10px; }
.slls-rm-search span { position:absolute;left:9px;top:7px;color:var(--ui-text-tertiary);width:16px; }
.slls-rm-search .slls-rm-input { min-height:30px;height:30px;padding:4px 10px 4px 32px; }
.slls-rm-model { display:flex;align-items:center;gap:9px;min-height:38px;padding:8px 10px;border:1px solid var(--ui-border);border-radius:8px;margin-bottom:10px;background:var(--ui-bg);font-size:13.5px;font-weight:600; }
.slls-rm input[type=checkbox] { width:16px;height:16px;accent-color:var(--ui-accent);flex:0 0 auto; }
.slls-rm-tree { height:350px;overflow:auto;border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg); }
.slls-rm-tree.disabled { opacity:.48;pointer-events:none; }
.slls-rm-table { border-bottom:1px solid var(--ui-border); }
.slls-rm-table:last-child { border-bottom:0; }
.slls-rm-table-row,.slls-rm-partition { display:flex;align-items:center;gap:8px;min-height:38px;padding:6px 10px; }
.slls-rm-table-row:hover,.slls-rm-partition:hover { background:var(--ui-bg-secondary); }
.slls-rm-caret { width:22px;height:22px;padding:0;border:0;background:transparent;color:var(--ui-text-secondary);display:flex;align-items:center;justify-content:center;cursor:pointer;transition:transform 120ms ease; }
.slls-rm-caret.open { transform:rotate(90deg); }
.slls-rm-caret svg { width:9px;height:11px; }
.slls-rm-object-icon { color:var(--ui-text-secondary);display:inline-flex; }
.slls-rm-name { min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:13.5px; }
.slls-rm-count,.slls-rm-muted { color:var(--ui-text-secondary);font-size:11.5px; }
.slls-rm-count { margin-left:auto;white-space:nowrap; }
.slls-rm-partitions { display:none;padding-bottom:5px; }
.slls-rm-partitions.open { display:block; }
.slls-rm-partition { padding-left:48px;min-height:34px; }
.slls-rm-partition .slls-rm-muted:last-child { margin-left:auto;white-space:nowrap; }
.slls-rm-pill,.slls-rm-badge { padding:2px 7px;border-radius:999px;background:var(--ui-bg-secondary);color:var(--ui-text-secondary);font-size:10.5px;white-space:nowrap; }
.slls-rm-empty { padding:28px 16px;text-align:center;color:var(--ui-text-secondary);font-size:13px; }
.slls-rm-fields { display:flex;flex-direction:column;gap:12px; }
.slls-rm-field label { display:block;margin:0 0 5px;color:var(--ui-text-secondary);font-size:10.5px;font-weight:700;text-transform:uppercase;letter-spacing:.04em; }
.slls-rm-field p { margin:5px 0 0;color:var(--ui-text-secondary);font-size:11.5px;line-height:1.35; }
.slls-rm-options .slls-rm-input,.slls-rm-options .slls-rm-select { min-height:32px;height:32px;padding:4px 9px;font-size:12px; }
.slls-rm-two { display:grid;grid-template-columns:1fr 1fr;gap:10px; }
.slls-rm-option { display:flex;align-items:center;gap:8px;font-size:13px; }
.slls-rm-toggle-row { display:flex;align-items:center;justify-content:space-between;gap:12px;font-size:13px; }
.slls-rm-toggle-help { margin:-7px 0 0;color:var(--ui-text-secondary);font-size:11.5px;line-height:1.35; }
.slls-rm-picker-wrap { min-height:430px;display:flex;align-items:center;justify-content:center;padding:24px; }
.slls-rm-picker { width:100%;max-width:820px;border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg-tertiary);padding:24px 28px;box-shadow:var(--ui-shadow-sm); }
.slls-rm-picker-top { display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:20px; }
.slls-rm-picker-tools { display:flex;align-items:center;gap:8px; }
.slls-rm-picker-title { font-size:17px;font-weight:650; }
.slls-rm-picker-sub { margin-top:3px;color:var(--ui-text-secondary);font-size:12.5px; }
.slls-rm-picker-grid { display:grid;grid-template-columns:1fr 1fr;gap:20px; }
.slls-rm-picker-actions { display:flex;justify-content:flex-end;margin-top:24px; }
.slls-rm-btn.primary.slls-rm-picker-connect { flex:none;width:auto;min-width:96px; }
.slls-rm-combo { position:relative; }
.slls-rm-combo-list { display:none;position:absolute;top:calc(100% + 4px);left:0;right:0;z-index:30;max-height:220px;overflow:auto;padding:4px;border:1px solid var(--ui-border-strong);border-radius:8px;background:var(--ui-bg);box-shadow:var(--ui-shadow-md); }
.slls-rm-combo.open .slls-rm-combo-list { display:block; }
.slls-rm-combo-item { padding:8px 10px;border-radius:6px;font-size:13px;cursor:pointer;overflow:hidden;text-overflow:ellipsis;white-space:nowrap; }
.slls-rm-combo-item:hover,.slls-rm-combo-item.active { background:var(--ui-accent-soft);color:var(--ui-accent); }
.slls-rm-combo-empty { padding:8px 10px;color:var(--ui-text-tertiary);font-size:12px; }
.slls-rm-actions { display:flex;gap:8px;margin-top:14px; }
.slls-rm-btn { min-height:38px;border:1px solid var(--ui-border-strong);border-radius:8px;padding:8px 14px;background:var(--ui-bg);color:var(--ui-text);font-weight:600;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;gap:7px; }
.slls-rm-btn:hover { background:var(--ui-bg-secondary); }
.slls-rm-btn:disabled { opacity:.42;cursor:not-allowed; }
.slls-rm-btn.primary { flex:1;color:#fff;background:var(--ui-accent);border-color:var(--ui-accent); }
.slls-rm-btn.danger { color:var(--rm-danger);border-color:var(--rm-danger); }
.slls-rm-btn svg { width:15px;height:15px; }
.slls-rm-spin svg { animation:slls-rm-spin 900ms linear infinite; }
@keyframes slls-rm-spin { to { transform:rotate(360deg); } }
.slls-rm-wide { grid-column:1/-1; }
.slls-rm-panel { overflow:hidden; }
.slls-rm-panel-head { display:flex;align-items:center;gap:9px;width:100%;padding:14px 16px;border:0;background:transparent;color:var(--ui-text);text-align:left;cursor:pointer;font-weight:650; }
.slls-rm-panel-toggle { min-width:0;flex:1;display:flex;align-items:center;gap:9px;padding:0;border:0;background:transparent;color:var(--ui-text);text-align:left;cursor:pointer;font-weight:650; }
.slls-rm-panel-head .slls-rm-caret { pointer-events:none; }
.slls-rm-panel-body { display:none;padding:0 16px 16px; }
.slls-rm-panel.open .slls-rm-panel-body { display:block; }
.slls-rm-panel.open>.slls-rm-panel-head .slls-rm-caret { transform:rotate(90deg); }
.slls-rm-status { display:flex;flex-direction:column;gap:14px;padding:16px;border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg); }
.slls-rm-status-head { display:flex;align-items:center;gap:12px; }
.slls-rm-status-icon { color:var(--ui-accent);width:20px;flex:0 0 auto; }
.slls-rm-status.success .slls-rm-status-icon,.slls-rm-status.success strong { color:var(--rm-success); }
.slls-rm-status.error .slls-rm-status-icon,.slls-rm-status.error strong { color:var(--rm-danger); }
.slls-rm-status strong { display:block;font-size:15px;margin-bottom:3px; }
.slls-rm-status-meta,.slls-rm-status-message { color:var(--ui-text-secondary);font-size:12px;white-space:pre-wrap; }
.slls-rm-status-message { padding:10px 12px;border:1px solid var(--ui-border);border-radius:7px;background:var(--ui-bg-tertiary); }
.slls-rm-status.error .slls-rm-status-message { color:var(--rm-danger);border-color:var(--rm-danger);background:var(--rm-danger-soft); }
.slls-rm-status-facts { display:flex;flex-wrap:wrap;gap:10px; }
.slls-rm-status-fact { min-width:130px;padding:8px 10px;border:1px solid var(--ui-border);border-radius:7px;background:var(--ui-bg-tertiary); }
.slls-rm-status-fact span { display:block;color:var(--ui-text-secondary);font-size:10px;font-weight:700;text-transform:uppercase; }
.slls-rm-status-fact b { display:block;margin-top:3px;font-size:13px;font-weight:600; }
.slls-rm-gantt { padding:16px;border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg); }
.slls-rm-gantt-head { display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px; }
.slls-rm-gantt-head strong { font-size:15px; }
.slls-rm-gantt-legend { display:flex;align-items:center;gap:12px;color:var(--ui-text-secondary);font-size:11px; }
.slls-rm-gantt-key { display:inline-flex;align-items:center;gap:5px; }
.slls-rm-gantt-swatch { width:11px;height:11px;border-radius:3px; }
.slls-rm-gantt-rows { display:flex;flex-direction:column;gap:5px; }
.slls-rm-gantt-row { display:grid;grid-template-columns:minmax(120px,190px) minmax(0,1fr);align-items:center;gap:12px; }
.slls-rm-gantt-label { overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px; }
.slls-rm-gantt-track { position:relative;height:20px;overflow:hidden;border-radius:4px;background:var(--ui-bg-secondary); }
.slls-rm-gantt-bar { position:absolute;top:2px;height:16px;min-width:2px;border-radius:3px; }
.slls-rm-gantt-axis { display:grid;grid-template-columns:minmax(120px,190px) minmax(0,1fr);gap:12px;margin-top:5px;color:var(--ui-text-secondary);font-size:10.5px; }
.slls-rm-gantt-ticks { display:flex;justify-content:space-between; }
.slls-rm-tablewrap { overflow:auto;max-height:300px;border:1px solid var(--ui-border);border-radius:8px; }
.slls-rm-data { width:100%;border-collapse:collapse;font-size:12px; }
.slls-rm-data th { position:sticky;top:0;background:var(--ui-bg-secondary);color:var(--ui-text-secondary);text-align:left;font-weight:650; }
.slls-rm-data th,.slls-rm-data td { padding:8px 10px;border-bottom:1px solid var(--ui-border);white-space:nowrap; }
.slls-rm-data tbody tr { cursor:pointer; }
.slls-rm-data tbody tr:hover { background:var(--ui-bg-secondary); }
.slls-rm-badge.Completed { background:var(--rm-success-soft);color:var(--rm-success); }
.slls-rm-badge.Failed { background:var(--rm-danger-soft);color:var(--rm-danger); }
.slls-rm-days { display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px; }
.slls-rm-day { border:1px solid var(--ui-border-strong);border-radius:7px;padding:6px 9px;background:var(--ui-bg);color:var(--ui-text-secondary);cursor:pointer; }
.slls-rm-day.active { background:var(--ui-accent);border-color:var(--ui-accent);color:#fff; }
.slls-rm-schedule-grid { display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px; }
.slls-rm-schedule-state { display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:14px;padding:10px 12px;border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg-tertiary); }
.slls-rm-schedule-state strong { display:block;font-size:13px; }
.slls-rm-schedule-state>div span { display:block;margin-top:2px;color:var(--ui-text-secondary);font-size:11px; }
.slls-rm-switch { position:relative;width:44px;height:24px;flex:0 0 auto;padding:0;border:0;border-radius:999px;background:var(--ui-border-strong);cursor:pointer;transition:background 120ms ease; }
.slls-rm-switch[aria-checked=true] { background:var(--ui-accent); }
.slls-rm-switch:disabled { opacity:.42;cursor:not-allowed; }
.slls-rm-switch:focus-visible { outline:0;box-shadow:0 0 0 3px var(--ui-accent-soft); }
.slls-rm-switch-knob { position:absolute;top:2px;left:2px;width:20px;height:20px;border-radius:50%;background:#fff;box-shadow:var(--ui-shadow-sm);transition:transform 120ms ease; }
.slls-rm-switch[aria-checked=true] .slls-rm-switch-knob { transform:translateX(20px); }
.slls-rm-times { display:flex;flex-direction:column;gap:6px; }
.slls-rm-time-row { display:flex;align-items:center;gap:6px; }
.slls-rm-time-row .slls-rm-select { max-width:150px; }
.slls-rm-add-time { align-self:flex-start;min-height:34px;margin-top:2px; }
.slls-rm-schedule-actions { align-items:center;flex-wrap:wrap; }
.slls-rm-btn.primary.slls-rm-save-schedule { flex:none;width:auto; }
.slls-rm-schedule-help { color:var(--ui-text-secondary);font-size:11px; }
.slls-rm-overlay { display:none;position:fixed;inset:0;z-index:10000;padding:24px;background:rgba(0,0,0,.48);align-items:center;justify-content:center; }
.slls-rm-overlay.show { display:flex; }
.slls-rm-modal { width:min(1000px,100%);max-height:calc(100vh - 28px);overflow:auto;border:1px solid var(--ui-border);border-radius:8px;background:var(--ui-bg);color:var(--ui-text);box-shadow:var(--ui-shadow-lg); }
.slls-rm-modal-head { position:sticky;top:0;z-index:2;display:flex;align-items:flex-start;gap:12px;padding:18px 14px;border-bottom:1px solid var(--ui-border);background:var(--ui-bg); }
.slls-rm-modal-head h2 { margin:0;font-size:16px; }
.slls-rm-modal-head code { display:block;margin-top:5px;color:var(--ui-accent);font-size:10.5px; }
.slls-rm-modal-head .slls-rm-iconbtn { margin-left:auto; }
.slls-rm-modal-body { padding:16px 14px 18px; }
.slls-rm-summary { display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:20px 32px; }
.slls-rm-summary span,.slls-rm-detail-title { display:block;color:var(--ui-text-secondary);font-size:10.5px;font-weight:700;text-transform:uppercase; }
.slls-rm-summary b { display:block;margin-top:6px;font-size:13px;font-weight:500;overflow-wrap:anywhere; }
.slls-rm-detail-section { margin-top:24px; }
.slls-rm-detail-title { margin-bottom:7px; }
.slls-rm-detail-section .slls-rm-tablewrap { max-height:none; }
.slls-rm-detail-section .slls-rm-data tbody tr { cursor:default; }
.slls-rm-detail-section .slls-rm-data td { height:46px; }
.slls-rm-detail-error { color:var(--rm-danger);max-width:300px;overflow:hidden;text-overflow:ellipsis; }
.slls-rm-attribution { padding-top:15px;text-align:right;color:var(--ui-text-tertiary);font-size:11px; }
.slls-rm-attribution a { color:inherit;text-decoration:none; }
.slls-rm-attribution a:hover { font-weight:700; }
@media (max-width:800px) { .slls-rm-grid,.slls-rm-picker-grid { grid-template-columns:1fr; } .slls-rm-wide { grid-column:auto; } .slls-rm-tree { height:300px; } .slls-rm-schedule-grid { grid-template-columns:1fr; } .slls-rm-summary { grid-template-columns:repeat(2,minmax(0,1fr)); } }
""".replace("__LIGHT_THEME__", _UI_LIGHT_VARS).replace("__DARK_THEME__", _UI_DARK_VARS)
_WIDGET_CSS += _ui_scoped_button_press_css(".slls-rm")


_WIDGET_JS = r"""
function render({ model, el }) {
    const root = document.createElement("div"); root.className = "slls-rm"; el.appendChild(root);
    const I={refresh:`__I_REFRESH__`,sun:`__I_SUN__`,moon:`__I_MOON__`,search:`__I_SEARCH__`,caret:`__I_CARET__`,table:`__I_TABLE__`,calculatedTable:`__I_CALCULATED_TABLE__`,calculationGroup:`__I_CALCULATION_GROUP__`,fieldParameter:`__I_FIELD_PARAMETER__`,partition:`__I_PARTITION__`,history:`__I_HISTORY__`,calendar:`__I_CALENDAR__`,close:`__I_CLOSE__`,check:`__I_CHECK__`,save:`__I_SAVE__`,plus:`__I_PLUS__`,fullscreen:`__I_FULLSCREEN__`,fullscreenExit:`__I_FULLSCREEN_EXIT__`,swap:`__I_SWAP__`,expandRows:`__I_EXPAND_ROWS__`,collapseRows:`__I_COLLAPSE_ROWS__`};
    const hints={Full:"Reload and recalculate data for the selected objects.",Automatic:"Let the engine decide what needs processing.",Calculate:"Recalculate formulas, hierarchies and relationships only.",DataOnly:"Reload data without recalculating dependents.",ClearValues:"Flush data from the selected objects.",Defragment:"Defragment table data while keeping it available."};
    const weekdays=[["Monday","Mon"],["Tuesday","Tue"],["Wednesday","Wed"],["Thursday","Thu"],["Friday","Fri"],["Saturday","Sat"],["Sunday","Sun"]];
    const timeOptions=Array.from({length:48},(_,index)=>`${String(Math.floor(index/2)).padStart(2,"0")}:${index%2?"30":"00"}`);
    const s={full:true,tables:new Set(),parts:new Set(),expanded:new Set(),search:"",history:false,schedule:false,type:"Full",commit:"Transactional",parallel:10,retry:0,policy:false,visualize:false,detail:false,draft:null,pickWs:"",pickDs:"",pickerOpen:false};
    const esc=v=>String(v??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"})[c]);
    const pkey=(t,p)=>`${t}\u0000${p}`;
    const dispatch=(action,payload={})=>{model.set("pending_action",{action,...payload});model.set("run",Number(model.get("run")||0)+1);model.save_changes();};
    let savedPosition=null;
    const scrollContainers=()=>{const nodes=[],seen=new Set(),body=root.querySelector(".slls-rm-body");if(body&&body.scrollHeight>body.clientHeight){nodes.push({node:body,key:"body"});seen.add(body);}let node=root;while(node){if(node instanceof Element&&node.scrollHeight>node.clientHeight&&!seen.has(node)){nodes.push({node,key:null});seen.add(node);}node=node.parentNode||(node.host||null);}const page=document.scrollingElement;if(page&&!seen.has(page))nodes.push({node:page,key:null});return nodes;};
    const keepPosition=()=>{savedPosition=scrollContainers().map(({node,key})=>({node,key,left:node.scrollLeft,top:node.scrollTop}));};
    const restorePosition=()=>{if(!savedPosition)return;const positions=savedPosition;savedPosition=null,restore=()=>positions.forEach(({node,key,left,top})=>{const target=key==="body"?root.querySelector(".slls-rm-body"):node;if(target)target.scrollTo(left,top);});restore();requestAnimationFrame(()=>{restore();requestAnimationFrame(restore);});};
    root.addEventListener("mousedown",event=>{const panelHead=event.target.closest(".slls-rm-panel-head");if(panelHead&&!event.target.closest("[data-reload-history],[data-reload-schedule]"))event.preventDefault();},true);
    root.addEventListener("click",event=>{const panelHead=event.target.closest(".slls-rm-panel-head"),reload=event.target.closest("[data-reload-history],[data-reload-schedule]");if(!panelHead||reload)return;const toggle=panelHead.querySelector("[data-panel]");if(!toggle)return;event.preventDefault();event.stopPropagation();const open=toggle.dataset.panel==="history"?(s.history=!s.history):(s.schedule=!s.schedule);panelHead.closest(".slls-rm-panel").classList.toggle("open",open);if(toggle.dataset.panel==="history"&&open&&!model.get("history_loaded"))dispatch("load_history");if(toggle.dataset.panel==="schedule"&&open&&!model.get("schedule_loaded"))dispatch("load_schedule");},true);
    root.addEventListener("click",event=>{if(event.target.closest("[data-reload-history]")){event.preventDefault();event.stopPropagation();dispatch("load_history");}else if(event.target.closest("[data-reload-schedule]")){event.preventDefault();event.stopPropagation();s.draft=null;dispatch("load_schedule");}});
    const optionDescriptions={type:"Controls how the selected model objects are processed.",commit:"Determines whether refresh changes are committed together or in separate batches.",parallel:"Sets the maximum number of refresh operations that can run concurrently.",retry:"Sets how many times a failed refresh operation is retried.",policy:"Applies the model's incremental refresh policy when processing eligible tables."};
    const annotateOptionDescriptions=()=>Object.entries(optionDescriptions).forEach(([option,description])=>{const control=root.querySelector(`[data-option="${option}"]`),field=control&&control.closest(".slls-rm-field,.slls-rm-toggle-row");if(field)field.title=description;});
    new MutationObserver(annotateOptionDescriptions).observe(root,{childList:true,subtree:true});
    const theme=()=>{root.classList.remove("slls-rm-dark","slls-rm-auto");const dark=model.get("dark_mode");if(dark===true)root.classList.add("slls-rm-dark");else if(dark==null)root.classList.add("slls-rm-auto");};
    const date=v=>{if(!v)return"Never refreshed";const d=new Date(v);return Number.isNaN(d.getTime())?"Never refreshed":d.toLocaleString();};
    const duration=(a,b)=>{a=Date.parse(a);b=Date.parse(b);if(!a||!b||b<a)return"";const x=Math.round((b-a)/1000),h=Math.floor(x/3600),m=Math.floor(x%3600/60);return`${h?`${h}h `:""}${m?`${m}m `:""}${x%60}s`;};
    const objects=()=>s.full?[]:[...[...s.tables].map(table=>({table})),...[...s.parts].map(v=>{const[table,partition]=v.split("\u0000");return{table,partition};})];
    let fsMode=false;
    function setFullscreen(on){fsMode=on;root.classList.toggle("slls-rm-fs",on);try{if(on){const request=root.requestFullscreen||root.webkitRequestFullscreen;if(request){const promise=request.call(root);if(promise&&promise.catch)promise.catch(()=>{});}}else{const exit=document.exitFullscreen||document.webkitExitFullscreen;if(exit&&(document.fullscreenElement||document.webkitFullscreenElement)){const promise=exit.call(document);if(promise&&promise.catch)promise.catch(()=>{});}}}catch(_error){}draw();}
    function onFullscreenChange(){if(!(document.fullscreenElement||document.webkitFullscreenElement)&&fsMode){fsMode=false;root.classList.remove("slls-rm-fs");draw();}}
    document.addEventListener("fullscreenchange",onFullscreenChange);document.addEventListener("webkitfullscreenchange",onFullscreenChange);document.addEventListener("keydown",event=>{if(event.key==="Escape"&&fsMode)setFullscreen(false);});
    function tree(){const all=model.get("objects")||[],q=s.search.trim().toLowerCase();const rows=all.map(t=>{if(!q||t.name.toLowerCase().includes(q))return t;const partitions=(t.partitions||[]).filter(p=>p.name.toLowerCase().includes(q));return partitions.length?{...t,partitions}:null;}).filter(Boolean);if(!rows.length)return`<div class="slls-rm-empty">${all.length?"No tables or partitions match your search.":"Loading tables and partitions..."}</div>`;return rows.map(t=>{const open=s.expanded.has(t.name)||!!q,checked=s.tables.has(t.name),partial=[...s.parts].some(v=>v.startsWith(`${t.name}\u0000`)),parts=t.partitions||[],tableIcon=t.kind==="calculation_group"?I.calculationGroup:t.kind==="calculated_table"?I.calculatedTable:t.kind==="field_parameter"?I.fieldParameter:I.table,tableKind={calculation_group:"Calculation Group",calculated_table:"Calculated Table",field_parameter:"Field Parameter"}[t.kind]||"Table";return`<div class="slls-rm-table"><div class="slls-rm-table-row"><button class="slls-rm-caret ${open?"open":""}" data-expand="${esc(t.name)}">${I.caret}</button><input type="checkbox" data-table="${esc(t.name)}" ${checked?"checked":""} ${partial?"data-partial=1":""}><span class="slls-rm-object-icon" title="${tableKind}" aria-label="${tableKind}">${tableIcon}</span><span class="slls-rm-name">${esc(t.name)}</span><span class="slls-rm-count">${parts.length} partition${parts.length===1?"":"s"}</span></div><div class="slls-rm-partitions ${open?"open":""}">${parts.map(p=>`<label class="slls-rm-partition"><input type="checkbox" data-partition="${esc(p.name)}" data-owner="${esc(t.name)}" ${checked||s.parts.has(pkey(t.name,p.name))?"checked":""} ${checked?"disabled":""}><span class="slls-rm-object-icon">${I.partition}</span><span class="slls-rm-name">${esc(p.name)}</span>${p.mode?`<span class="slls-rm-pill">${esc(p.mode)}</span>`:""}<span class="slls-rm-muted">${esc(date(p.refreshedTime))}</span></label>`).join("")}</div></div>`;}).join("");}
    function status(){const x=model.get("refresh_status")||{},busy=!!model.get("busy");if(!busy&&!x.message&&!x.status)return"";const value=x.status||(busy?"Unknown":""),kind=x.kind||(value==="Completed"?"success":value==="Failed"?"error":""),title=value==="Completed"?"Refresh completed":value==="Failed"?"Refresh failed":value==="Cancelled"?"Refresh cancelled":"Refresh in progress...",elapsed=duration(x.startTime,x.endTime),targets=x.objects||[],meta=x.startTime?`Started ${date(x.startTime)}${x.endTime?` · ended ${date(x.endTime)}`:""}`:"",facts=[["Refresh type",x.refreshType],["Duration",elapsed]].filter(([,v])=>v),rows=targets.map(o=>`<tr><td>${esc(o.table||"-")}</td><td>${esc(o.partition||"-")}</td><td>${esc(o.status||"-")}</td></tr>`).join("");return`<div class="slls-rm-status ${kind}"><div class="slls-rm-status-head"><span class="slls-rm-status-icon ${busy?"slls-rm-spin":""}">${value==="Completed"?I.check:value==="Failed"?I.close:I.refresh}</span><div><strong>${esc(title)}</strong>${meta?`<div class="slls-rm-status-meta">${esc(meta)}</div>`:""}</div></div>${facts.length?`<div class="slls-rm-status-facts">${facts.map(([label,v])=>`<div class="slls-rm-status-fact"><span>${label}</span><b>${esc(v)}</b></div>`).join("")}</div>`:""}${x.message?`<div class="slls-rm-status-message">${esc(x.message)}</div>`:""}${rows?`<div class="slls-rm-tablewrap"><table class="slls-rm-data"><thead><tr><th>Table</th><th>Partition</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table></div>`:""}</div>`;}
    const formatMs=value=>value>=1000?`${(value/1000).toFixed(1)}s`:`${Math.round(value)}ms`;
    function gantt(){const events=model.get("gantt_events")||[];if(!events.length)return"";const starts=events.map(event=>Date.parse(event.startTime)).filter(Number.isFinite),ends=events.map(event=>Date.parse(event.endTime)).filter(Number.isFinite),minimum=Math.min(...starts),maximum=Math.max(...ends),span=Math.max(maximum-minimum,1),groups=new Map();events.forEach(event=>{const rows=groups.get(event.objectName)||[];rows.push(event);groups.set(event.objectName,rows);});const rows=[...groups.entries()].sort(([,left],[,right])=>Date.parse(left[0].startTime)-Date.parse(right[0].startTime)).map(([name,items])=>`<div class="slls-rm-gantt-row"><span class="slls-rm-gantt-label" title="${esc(name)}">${esc(name)}</span><div class="slls-rm-gantt-track">${items.map(event=>{const start=Date.parse(event.startTime),end=Date.parse(event.endTime),left=Math.max(0,(start-minimum)/span*100),width=Math.max(.3,(end-start)/span*100),color=event.eventSubclass==="ExecuteSql"?"#0070c0":"#ffc000";return`<span class="slls-rm-gantt-bar" style="left:${left}%;width:${Math.min(width,100-left)}%;background:${color}" title="${esc(name)} · ${esc(event.eventSubclass==="ExecuteSql"?"Execute SQL":"Process")} · ${esc(formatMs(event.durationMs||0))} · CPU ${esc(formatMs(event.cpuTimeMs||0))}"></span>`;}).join("")}</div></div>`).join("");return`<div class="slls-rm-gantt"><div class="slls-rm-gantt-head"><strong>Refresh timeline</strong><div class="slls-rm-gantt-legend"><span class="slls-rm-gantt-key"><i class="slls-rm-gantt-swatch" style="background:#ffc000"></i>Process</span><span class="slls-rm-gantt-key"><i class="slls-rm-gantt-swatch" style="background:#0070c0"></i>Execute SQL</span><span>Total ${esc(formatMs(span))}</span></div></div><div class="slls-rm-gantt-rows">${rows}</div><div class="slls-rm-gantt-axis"><span>Elapsed</span><div class="slls-rm-gantt-ticks"><span>0ms</span><span>${esc(formatMs(span/2))}</span><span>${esc(formatMs(span))}</span></div></div></div>`;}
    function history(){const rows=model.get("history")||[],loading=!!model.get("history_loading");return`<div class="slls-rm-panel ${s.history?"open":""}"><div class="slls-rm-panel-head"><button class="slls-rm-panel-toggle" data-panel="history"><span class="slls-rm-caret">${I.caret}</span>${I.history}<span>Refresh history</span></button>${rows.length?`<span class="slls-rm-badge">${rows.length}</span>`:""}<button class="slls-rm-iconbtn ${loading?"slls-rm-spin":""}" data-reload-history title="Reload refresh history" ${loading?"disabled":""}>${I.refresh}</button></div><div class="slls-rm-panel-body">${rows.length?`<div class="slls-rm-tablewrap"><table class="slls-rm-data"><thead><tr><th>Type</th><th>Start</th><th>End</th><th>Duration</th><th>Status</th><th>Refresh ID</th></tr></thead><tbody>${rows.map(r=>`<tr data-detail="${esc(r.requestId)}"><td>${esc(r.refreshType)}</td><td>${esc(date(r.startTime))}</td><td>${esc(r.endTime?date(r.endTime):"-")}</td><td>${esc(duration(r.startTime,r.endTime)||"-")}</td><td><span class="slls-rm-badge ${esc(r.status)}">${esc(r.status)}</span></td><td><code>${esc(r.requestId)}</code></td></tr>`).join("")}</tbody></table></div>`:`<div class="slls-rm-empty">${model.get("history_loaded")?"No refresh history for this model yet.":"Open this panel to load refresh history."}</div>`}</div></div>`;}
    function schedule(){const current=model.get("schedule")||{},loading=!!model.get("schedule_loading");if(!s.draft&&current.exists)s.draft=JSON.parse(JSON.stringify(current));const d=s.draft||{days:[],times:[],localTimeZoneId:"UTC",notifyOption:"NoNotification",enabled:false},timeRows=(d.times||[]).map((time,index)=>`<div class="slls-rm-time-row"><select class="slls-rm-select" data-time-index="${index}" aria-label="Refresh time ${index+1}">${timeOptions.filter(option=>option===time||!d.times.includes(option)).map(option=>`<option value="${option}" ${option===time?"selected":""}>${option}</option>`).join("")}</select><button class="slls-rm-iconbtn" data-remove-time="${index}" title="Remove time" aria-label="Remove time ${esc(time)}">${I.close}</button></div>`).join("");return`<div class="slls-rm-panel ${s.schedule?"open":""}"><div class="slls-rm-panel-head"><button class="slls-rm-panel-toggle" data-panel="schedule"><span class="slls-rm-caret">${I.caret}</span>${I.calendar}<span>Refresh schedule</span>${current.exists?`<span class="slls-rm-badge ${current.enabled?"Completed":""}">${current.enabled?"Enabled":"Disabled"}</span>`:""}</button>${s.schedule?`<button class="slls-rm-iconbtn ${loading?"slls-rm-spin":""}" data-reload-schedule title="Reload refresh schedule" aria-label="Reload refresh schedule" ${loading?"disabled":""}>${I.refresh}</button>`:""}</div><div class="slls-rm-panel-body">${loading&&!model.get("schedule_loaded")?`<div class="slls-rm-empty">Loading refresh schedule...</div>`:model.get("schedule_loaded")&&!current.exists?`<div class="slls-rm-empty">${esc(current.message||"This model has no refresh schedule.")}</div>`:`<div class="slls-rm-schedule-state"><div><strong>Scheduled refresh</strong><span>${d.enabled?"Power BI refreshes this model on the schedule below.":"The schedule is saved but turned off."}</span></div><button class="slls-rm-switch" type="button" role="switch" aria-checked="${d.enabled}" aria-label="${d.enabled?"Disable":"Enable"} scheduled refresh" data-toggle-schedule title="${d.enabled?"Disable":"Enable"} scheduled refresh"><span class="slls-rm-switch-knob"></span></button></div><div class="slls-rm-days">${weekdays.map(([v,l])=>`<button class="slls-rm-day ${(d.days||[]).includes(v)?"active":""}" data-day="${v}">${l}</button>`).join("")}</div><div class="slls-rm-schedule-grid"><div class="slls-rm-field"><label>Times</label><div class="slls-rm-times">${timeRows||`<span class="slls-rm-muted">No times set.</span>`}<button class="slls-rm-btn slls-rm-add-time" data-add-time ${d.times.length>=timeOptions.length?"disabled":""}>${I.plus}Add time</button></div></div><div class="slls-rm-field"><label>Time zone</label><input class="slls-rm-input" data-schedule="timezone" value="${esc(d.localTimeZoneId||"UTC")}"></div><div class="slls-rm-field"><label>Notification</label><select class="slls-rm-select" data-schedule="notify"><option value="NoNotification" ${d.notifyOption==="NoNotification"?"selected":""}>No notification</option><option value="MailOnFailure" ${d.notifyOption==="MailOnFailure"?"selected":""}>Mail on failure</option></select></div></div><div class="slls-rm-actions slls-rm-schedule-actions"><button class="slls-rm-btn primary slls-rm-save-schedule" data-save-schedule ${!(d.days||[]).length?"disabled":""}>${I.save}Save schedule</button><span class="slls-rm-schedule-help">Saving also enables the schedule. Times must be on the hour or half hour.</span></div>`}</div></div>`;}
    function detailError(value){if(!value)return"—";try{const parsed=typeof value==="string"?JSON.parse(value):value;return parsed.errorDescription||parsed.errorCode||parsed.message||"—";}catch(_error){return String(value);}}
    function detail(){const d=model.get("detail")||{},attempts=d.refreshAttempts||[],targets=d.objects||[],fields=[["Current type",d.currentRefreshType||d.type||d.refreshType],["Commit mode",d.commitMode],["Initiated by",d.initiatedBy],["Attempts",d.numberOfAttempts??attempts.length],["Start",date(d.startTime)],["End",d.endTime?date(d.endTime):"-"],["Duration",duration(d.startTime,d.endTime)||"-"]];const attemptRows=attempts.map((a,index)=>`<tr><td>${esc(a.attemptId??index+1)}</td><td>${esc(a.type||"-")}</td><td>${esc(date(a.startTime))}</td><td>${esc(a.endTime?date(a.endTime):"-")}</td><td>${esc(duration(a.startTime,a.endTime)||"-")}</td><td class="slls-rm-detail-error" title="${esc(detailError(a.serviceExceptionJson))}">${esc(detailError(a.serviceExceptionJson))}</td></tr>`).join("");const objectRows=targets.map(o=>`<tr><td>${esc(o.table||"-")}</td><td>${esc(o.partition||"-")}</td><td>${esc(o.status||"-")}</td></tr>`).join("");return`<div class="slls-rm-overlay ${s.detail?"show":""}"><div class="slls-rm-modal"><div class="slls-rm-modal-head"><div><h2>Refresh details</h2><code>${esc(d.requestId||"Loading...")}</code></div><button class="slls-rm-iconbtn" data-close-detail title="Close refresh details" aria-label="Close refresh details">${I.close}</button></div><div class="slls-rm-modal-body">${d.requestId?`<div class="slls-rm-summary">${fields.map(([label,value])=>`<div><span>${label}</span><b>${esc(value??"-")}</b></div>`).join("")}</div><section class="slls-rm-detail-section"><div class="slls-rm-detail-title">Attempts</div>${attemptRows?`<div class="slls-rm-tablewrap"><table class="slls-rm-data"><thead><tr><th>#</th><th>Type</th><th>Start</th><th>End</th><th>Duration</th><th>Error</th></tr></thead><tbody>${attemptRows}</tbody></table></div>`:`<div class="slls-rm-empty">No attempt details are available.</div>`}</section><section class="slls-rm-detail-section"><div class="slls-rm-detail-title">Objects</div>${objectRows?`<div class="slls-rm-tablewrap"><table class="slls-rm-data"><thead><tr><th>Table</th><th>Partition</th><th>Status</th></tr></thead><tbody>${objectRows}</tbody></table></div>`:`<div class="slls-rm-empty">No object details are available.</div>`}</section>`:`<div class="slls-rm-empty">Loading refresh details...</div>`}</div></div></div>`;}
    function combo(kind,items,selectedId,placeholder,disabled=false){const selected=(items||[]).find(item=>item.id===selectedId);return`<div class="slls-rm-combo" data-combo="${kind}"><input class="slls-rm-input" data-combo-input="${kind}" value="${esc(selected?selected.name:"")}" placeholder="${esc(placeholder)}" autocomplete="off" ${disabled?"disabled":""}><div class="slls-rm-combo-list">${(items||[]).length?(items||[]).map(item=>`<div class="slls-rm-combo-item" data-combo-option="${kind}" data-id="${esc(item.id)}" data-name="${esc(item.name)}">${esc(item.name)}</div>`).join(""):`<div class="slls-rm-combo-empty">${esc(placeholder)}</div>`}</div></div>`;}
    function picker(){if(!s.pickWs)s.pickWs=model.get("workspace_id")||"";const workspaces=model.get("workspaces")||[],datasets=(model.get("datasets")||{})[s.pickWs],connected=!!model.get("connected");const datasetPlaceholder=!s.pickWs?"Select a workspace first...":datasets===undefined?"Loading...":!datasets.length?"No semantic models":"Search semantic models...";return`<div class="slls-rm-picker-wrap"><div class="slls-rm-picker"><div class="slls-rm-picker-top"><div><div class="slls-rm-picker-title">Choose a semantic model</div><div class="slls-rm-picker-sub">Pick a workspace and semantic model to manage its refreshes.</div></div><div class="slls-rm-picker-tools"><button class="slls-rm-iconbtn" data-picker-reload title="Reload workspaces and semantic models" aria-label="Reload workspaces and semantic models">${I.refresh}</button>${connected?`<button class="slls-rm-iconbtn" data-picker-close title="Close model picker" aria-label="Close model picker">${I.close}</button>`:""}</div></div><div class="slls-rm-picker-grid"><div class="slls-rm-field"><label>Workspace</label>${combo("workspace",workspaces,s.pickWs,"Search workspaces...")}</div><div class="slls-rm-field"><label>Semantic model</label>${combo("dataset",datasets||[],s.pickDs,datasetPlaceholder,!s.pickWs||datasets===undefined)}</div></div><div class="slls-rm-picker-actions"><button class="slls-rm-btn primary slls-rm-picker-connect" data-picker-connect ${!s.pickDs?"disabled":""}>Connect</button></div></div></div>`;}
    function header(){const connected=!!model.get("connected"),picking=!connected||s.pickerOpen,subtitle=connected?`${esc(model.get("dataset_name"))} &middot; ${esc(model.get("workspace_name"))}`:"Choose a workspace and semantic model";return`<header class="slls-rm-header"><span class="slls-rm-mark">${I.refresh}</span><div class="slls-rm-heading"><div class="slls-rm-title-row"><h1 class="slls-rm-title">Refresh Manager</h1>${connected&&!picking?`<button class="slls-rm-iconbtn" data-change-model title="Change semantic model / workspace" aria-label="Change semantic model / workspace" ${model.get("busy")?"disabled":""}>${I.swap}</button>`:""}</div><div class="slls-rm-subtitle">${subtitle}</div></div><button class="slls-rm-iconbtn" data-fullscreen title="Toggle full screen">${fsMode?I.fullscreenExit:I.fullscreen}</button><button class="slls-rm-iconbtn" data-theme title="Toggle theme">${model.get("dark_mode")?I.sun:I.moon}</button></header>`;}
    function bindHeader(){root.querySelector("[data-theme]").onclick=()=>{model.set("dark_mode",!model.get("dark_mode"));model.save_changes();draw();};root.querySelector("[data-fullscreen]").onclick=()=>setFullscreen(!fsMode);const change=root.querySelector("[data-change-model]");if(change)change.onclick=()=>{s.pickerOpen=true;s.pickWs=model.get("workspace_id")||"";s.pickDs="";draw();};const expandAll=root.querySelector("[data-expand-all]");if(expandAll){const collapse=s.expanded.size>0;expandAll.innerHTML=collapse?I.collapseRows:I.expandRows;expandAll.title=collapse?"Collapse all":"Expand all";}}
    function bindCombo(kind,onChoose){const wrap=root.querySelector(`[data-combo="${kind}"]`),input=root.querySelector(`[data-combo-input="${kind}"]`);if(!wrap||!input)return;const options=[...wrap.querySelectorAll(`[data-combo-option="${kind}"]`)];const filter=()=>{const query=input.value.trim().toLowerCase();options.forEach(option=>{option.style.display=option.dataset.name.toLowerCase().includes(query)?"":"none";});wrap.classList.add("open");};input.onfocus=filter;input.oninput=()=>{if(kind==="workspace"){s.pickWs="";s.pickDs="";}else{s.pickDs="";}const connect=root.querySelector("[data-picker-connect]");if(connect)connect.disabled=true;filter();};input.onblur=()=>setTimeout(()=>wrap.classList.remove("open"),150);input.onkeydown=event=>{if(event.key==="Escape")input.blur();else if(event.key==="Tab"&&kind==="dataset"){event.preventDefault();wrap.classList.remove("open");root.querySelector("[data-picker-connect]").focus();}else if(event.key==="Enter"){event.preventDefault();const first=options.find(option=>option.style.display!=="none");if(first)first.dispatchEvent(new MouseEvent("mousedown"));}};options.forEach(option=>option.onmousedown=event=>{event.preventDefault();input.value=option.dataset.name;wrap.classList.remove("open");onChoose(option.dataset.id);});}
    function bindPicker(){bindHeader();bindCombo("workspace",id=>{s.pickWs=id;s.pickDs="";dispatch("list_datasets",{workspace_id:id});draw();});bindCombo("dataset",id=>{s.pickDs=id;draw();const input=root.querySelector('[data-combo-input="dataset"]');if(input)input.focus();});root.querySelector("[data-picker-reload]").onclick=()=>dispatch("reload_picker",{workspace_id:s.pickWs});const close=root.querySelector("[data-picker-close]");if(close)close.onclick=()=>{s.pickerOpen=false;draw();};root.querySelector("[data-picker-connect]").onclick=()=>{if(!s.pickDs)return;const workspaces=model.get("workspaces")||[],datasets=(model.get("datasets")||{})[s.pickWs]||[];dispatch("connect",{workspace_id:s.pickWs,dataset_id:s.pickDs,workspace_name:(workspaces.find(w=>w.id===s.pickWs)||{}).name||"",dataset_name:(datasets.find(d=>d.id===s.pickDs)||{}).name||""});};}
    function draw(){if(!savedPosition)keepPosition();theme();if(!model.get("connected")||s.pickerOpen){root.innerHTML=header()+`<main class="slls-rm-body">${picker()}<div class="slls-rm-attribution">Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank">Semantic Link Labs</a></div></main>`;bindPicker();restorePosition();return;}const busy=!!model.get("busy"),policyOff=s.commit==="PartialBatch";if(policyOff)s.policy=false;root.innerHTML=header()+`<main class="slls-rm-body"><div class="slls-rm-grid"><section class="slls-rm-card"><div class="slls-rm-card-head"><h2 class="slls-rm-card-title">Objects to refresh</h2><button class="slls-rm-iconbtn" data-expand-all title="Expand or collapse all">${I.caret}</button><button class="slls-rm-iconbtn ${model.get("objects_loading")?"slls-rm-spin":""}" data-reload-objects title="Reload tables">${I.refresh}</button></div><div class="slls-rm-search"><span>${I.search}</span><input class="slls-rm-input" data-search value="${esc(s.search)}" placeholder="Search tables and partitions..."></div><label class="slls-rm-model"><input type="checkbox" data-full ${s.full?"checked":""}>Refresh the entire model</label><div class="slls-rm-tree ${s.full?"disabled":""}">${tree()}</div></section><section><div class="slls-rm-card slls-rm-options"><div class="slls-rm-card-head"><h2 class="slls-rm-card-title">Refresh options</h2></div><div class="slls-rm-fields"><div class="slls-rm-field"><label>Refresh type</label><select class="slls-rm-select" data-option="type">${Object.keys(hints).map(v=>`<option value="${v}" ${s.type===v?"selected":""}>${v==="DataOnly"?"Data only":v==="ClearValues"?"Clear values":v}</option>`).join("")}</select><p>${hints[s.type]}</p></div><div class="slls-rm-field"><label>Commit mode</label><select class="slls-rm-select" data-option="commit"><option value="Transactional" ${s.commit==="Transactional"?"selected":""}>Transactional</option><option value="PartialBatch" ${s.commit==="PartialBatch"?"selected":""}>Partial batch</option></select></div><div class="slls-rm-two"><div class="slls-rm-field"><label>Max parallelism</label><input class="slls-rm-input" type="number" min="1" value="${s.parallel}" data-option="parallel"></div><div class="slls-rm-field"><label>Retry count</label><input class="slls-rm-input" type="number" min="0" value="${s.retry}" data-option="retry"></div></div><div class="slls-rm-toggle-row"><span>Apply refresh policy</span><button class="slls-rm-switch" type="button" role="switch" aria-checked="${s.policy}" aria-label="Toggle apply refresh policy" data-option="policy" ${policyOff||busy?"disabled":""}><span class="slls-rm-switch-knob"></span></button></div>${policyOff?`<p class="slls-rm-toggle-help">Partial batch does not support applying the refresh policy.</p>`:""}<div class="slls-rm-toggle-row" title="Run the refresh under an Analysis Services trace and chart each partition's Process and source-query durations."><span>Visualize refresh</span><button class="slls-rm-switch" type="button" role="switch" aria-checked="${s.visualize}" aria-label="Toggle visualize refresh" data-option="visualize" ${busy?"disabled":""}><span class="slls-rm-switch-knob"></span></button></div>${s.visualize?`<p class="slls-rm-toggle-help">Charts each partition's processing timeline. Cancelling is not available while visualizing.</p>`:""}</div></div><div class="slls-rm-actions"><button class="slls-rm-btn primary ${busy?"slls-rm-spin":""}" data-refresh ${busy||(!s.full&&!objects().length)?"disabled":""}>${I.refresh}${busy?"Refreshing...":"Refresh"}</button>${busy&&!s.visualize?`<button class="slls-rm-btn danger" data-cancel>Cancel</button>`:""}</div></section><section class="slls-rm-wide" data-live-status>${status()}</section><section class="slls-rm-wide" data-live-gantt>${gantt()}</section><section class="slls-rm-wide">${history()}</section><section class="slls-rm-wide">${schedule()}</section></div><div class="slls-rm-attribution">Powered by <a href="https://github.com/microsoft/semantic-link-labs" target="_blank">Semantic Link Labs</a></div></main>${detail()}`;bind();restorePosition();}
    function bind(){const one=q=>root.querySelector(q);bindHeader();one("[data-search]").oninput=e=>{s.search=e.target.value;draw();const x=one("[data-search]");x.focus();x.setSelectionRange(x.value.length,x.value.length);};one("[data-full]").onchange=e=>{s.full=e.target.checked;draw();};one("[data-expand-all]").onclick=()=>{const all=model.get("objects")||[];s.expanded.size?s.expanded.clear():all.forEach(t=>s.expanded.add(t.name));draw();};one("[data-reload-objects]").onclick=()=>dispatch("load_objects");root.querySelectorAll("[data-expand]").forEach(b=>b.onclick=()=>{s.expanded.has(b.dataset.expand)?s.expanded.delete(b.dataset.expand):s.expanded.add(b.dataset.expand);draw();});root.querySelectorAll("[data-table]").forEach(x=>{if(x.dataset.partial)x.indeterminate=true;x.onchange=()=>{const n=x.dataset.table;x.checked?s.tables.add(n):s.tables.delete(n);[...s.parts].filter(v=>v.startsWith(`${n}\u0000`)).forEach(v=>s.parts.delete(v));draw();};});root.querySelectorAll("[data-partition]").forEach(x=>x.onchange=()=>{const k=pkey(x.dataset.owner,x.dataset.partition);x.checked?s.parts.add(k):s.parts.delete(k);s.tables.delete(x.dataset.owner);draw();});one('[data-option="type"]').onchange=e=>{s.type=e.target.value;draw();};one('[data-option="commit"]').onchange=e=>{s.commit=e.target.value;draw();};one('[data-option="parallel"]').onchange=e=>s.parallel=Math.max(1,Number(e.target.value)||1);one('[data-option="retry"]').onchange=e=>s.retry=Math.max(0,Number(e.target.value)||0);const policy=one('[data-option="policy"]');if(policy)policy.onclick=()=>{s.policy=!s.policy;draw();};const visualize=one('[data-option="visualize"]');if(visualize)visualize.onclick=()=>{s.visualize=!s.visualize;draw();};const start=one("[data-refresh]");if(start)start.onclick=()=>dispatch("start_refresh",{refresh_type:s.type,commit_mode:s.commit,max_parallelism:s.parallel,retry_count:s.retry,apply_refresh_policy:s.policy,visualize:s.visualize,objects:objects()});const cancel=one("[data-cancel]");if(cancel)cancel.onclick=()=>dispatch("cancel_refresh");root.querySelectorAll("[data-panel]").forEach(b=>b.onclick=()=>{if(b.dataset.panel==="history"){s.history=!s.history;if(s.history&&!model.get("history_loaded"))dispatch("load_history");}else{s.schedule=!s.schedule;if(s.schedule&&!model.get("schedule_loaded"))dispatch("load_schedule");}draw();});root.querySelectorAll("[data-detail]").forEach(r=>r.onclick=()=>{s.detail=true;model.set("detail",{});dispatch("load_detail",{request_id:r.dataset.detail});draw();});const close=one("[data-close-detail]");if(close)close.onclick=()=>{s.detail=false;draw();};root.querySelectorAll("[data-day]").forEach(b=>b.onclick=()=>{keepPosition();const a=s.draft.days,v=b.dataset.day;a.includes(v)?s.draft.days=a.filter(d=>d!==v):a.push(v);draw();});root.querySelectorAll("[data-time-index]").forEach(select=>select.onchange=e=>{keepPosition();s.draft.times[Number(select.dataset.timeIndex)]=e.target.value;draw();});root.querySelectorAll("[data-remove-time]").forEach(button=>button.onclick=()=>{keepPosition();s.draft.times=s.draft.times.filter((_,index)=>index!==Number(button.dataset.removeTime));draw();});const addTime=one("[data-add-time]");if(addTime)addTime.onclick=()=>{keepPosition();s.draft.times.push(timeOptions.find(option=>!s.draft.times.includes(option))||"00:00");draw();};const zone=one('[data-schedule="timezone"]');if(zone)zone.onchange=e=>s.draft.localTimeZoneId=e.target.value.trim()||"UTC";const notify=one('[data-schedule="notify"]');if(notify)notify.onchange=e=>s.draft.notifyOption=e.target.value;const save=one("[data-save-schedule]");if(save)save.onclick=()=>{keepPosition();s.draft.times=[...new Set(s.draft.times)].sort();dispatch("save_schedule",{schedule:s.draft});};const toggle=one("[data-toggle-schedule]");if(toggle)toggle.onclick=()=>{keepPosition();dispatch("toggle_schedule",{enabled:!s.draft.enabled});};}
    const updateLive=(selector,render)=>{const node=root.querySelector(selector);if(node)node.innerHTML=render();else draw();};
    ["objects","objects_loading","history","history_loaded","history_loading","schedule_loaded","schedule_loading","busy","detail","dark_mode","connected","workspaces","datasets"].forEach(n=>model.on(`change:${n}`,draw));model.on("change:refresh_status",()=>updateLive("[data-live-status]",status));model.on("change:gantt_events",()=>updateLive("[data-live-gantt]",gantt));model.on("change:schedule",()=>{s.draft=null;draw();});model.on("change:connect_done",()=>{s.pickerOpen=false;s.pickDs="";s.tables.clear();s.parts.clear();s.expanded.clear();s.search="";s.history=false;s.schedule=false;s.draft=null;s.visualize=false;draw();});draw();if(model.get("connected"))dispatch("load_objects");
}
export default { render };
"""

_WIDGET_JS = (
    _WIDGET_JS.replace("__I_REFRESH__", _UI_ICONS["refresh"])
    .replace("__I_SUN__", _UI_ICONS["sun"])
    .replace("__I_MOON__", _UI_ICONS["moon"])
    .replace("__I_SEARCH__", _UI_ICONS["search"])
    .replace("__I_CARET__", _UI_ICONS["caret_right"])
    .replace("__I_TABLE__", _UI_ICONS["table"])
    .replace("__I_CALCULATED_TABLE__", _UI_ICONS["calculated_table"])
    .replace("__I_CALCULATION_GROUP__", _UI_ICONS["calculation_group"])
    .replace("__I_FIELD_PARAMETER__", _UI_ICONS["field_parameter"])
    .replace("__I_PARTITION__", _UI_ICONS["partition"])
    .replace("__I_HISTORY__", _UI_ICONS["history"])
    .replace("__I_CALENDAR__", _UI_ICONS["date_table"])
    .replace("__I_CLOSE__", _UI_ICONS["close"])
    .replace("__I_CHECK__", _UI_ICONS["check"])
    .replace("__I_SAVE__", _UI_ICONS["save"])
    .replace("__I_PLUS__", _UI_ICONS["plus"])
    .replace("__I_FULLSCREEN__", _UI_ICONS["fullscreen"])
    .replace("__I_FULLSCREEN_EXIT__", _UI_ICONS["fullscreen_exit"])
    .replace("__I_SWAP__", _UI_ICONS["swap"])
    .replace("__I_EXPAND_ROWS__", _UI_ICONS["expand_rows"])
    .replace("__I_COLLAPSE_ROWS__", _UI_ICONS["collapse_rows"])
)


def _response_json(response: Any) -> dict[str, Any]:
    """Return JSON from a requests-like response or an existing mapping."""

    return response.json() if hasattr(response, "json") else dict(response or {})


def _refresh_detail_json(response: Any, request_id: str) -> dict[str, Any]:
    """Add the route refresh ID omitted by the execution-details response."""

    detail = _response_json(response)
    detail["requestId"] = request_id
    return detail


def _format_refreshed_time(value: Any) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "ToString"):
        text = value.ToString("o")
    else:
        text = str(value)
    return None if text.startswith("0001-") else text


def _get_refresh_trace_events(
    trace_logs: Any, partition_map: Any
) -> list[dict[str, Any]]:
    """Normalize refresh trace rows for the anywidget Gantt timeline."""

    if trace_logs is None or trace_logs.empty:
        return []
    filtered = trace_logs[
        trace_logs["Event Subclass"].isin(["ExecuteSql", "Process"])
    ].reset_index(drop=True)
    if filtered.empty:
        return []
    merged = filtered.merge(
        partition_map[["PartitionID", "Object Name", "TableName", "PartitionName"]],
        left_on="Object ID",
        right_on="PartitionID",
        how="inner",
    )

    def number(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return [
        {
            "objectName": str(row["Object Name"]),
            "tableName": str(row["TableName"]),
            "partitionName": str(row["PartitionName"]),
            "eventSubclass": str(row["Event Subclass"]),
            "startTime": _format_refreshed_time(row["Start Time"]) or "",
            "endTime": _format_refreshed_time(row["End Time"]) or "",
            "durationMs": number(row.get("Duration")),
            "cpuTimeMs": number(row.get("Cpu Time")),
        }
        for _, row in merged.iterrows()
    ]


def _read_refresh_trace_events(
    trace: Any, partition_map: Any, stop: bool = False
) -> list[dict[str, Any]]:
    """Read trace events without surfacing sempy's expected empty-log warning."""

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="No trace logs have been recorded.*",
            category=UserWarning,
            module=r"sempy\.fabric\._trace\._trace",
        )
        trace_logs = trace.stop() if stop else trace.get_trace_logs()
    return _get_refresh_trace_events(trace_logs, partition_map)


def _get_refresh_objects(dataset_id: str, workspace_id: str) -> list[dict[str, Any]]:
    from sempy_labs.tom import connect_semantic_model

    tables = []
    with connect_semantic_model(
        dataset=dataset_id, workspace=workspace_id, readonly=True
    ) as tom:
        for table in tom.model.Tables:
            partitions = [
                {
                    "name": str(partition.Name),
                    "mode": str(getattr(partition, "Mode", "")).split(".")[-1],
                    "refreshedTime": _format_refreshed_time(
                        getattr(partition, "RefreshedTime", None)
                    ),
                }
                for partition in table.Partitions
            ]
            tables.append(
                {
                    "name": str(table.Name),
                    "kind": (
                        "calculation_group"
                        if table.CalculationGroup is not None
                        else (
                            "field_parameter"
                            if tom.is_field_parameter(table_name=str(table.Name))
                            else (
                                "calculated_table"
                                if any(
                                    str(getattr(partition, "SourceType", "")).split(
                                        "."
                                    )[-1]
                                    == "Calculated"
                                    for partition in table.Partitions
                                )
                                else "table"
                            )
                        )
                    ),
                    "partitions": sorted(
                        partitions, key=lambda partition: partition["name"]
                    ),
                }
            )
    return sorted(tables, key=lambda table: table["name"])


def _get_refresh_history(workspace_id: str, dataset_id: str) -> list[dict[str, Any]]:
    from sempy_labs._helper_functions import _base_api

    response = _base_api(
        request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"
    )
    return [
        {
            "requestId": item.get("requestId", ""),
            "refreshType": item.get("refreshType", ""),
            "startTime": item.get("startTime", ""),
            "endTime": item.get("endTime", ""),
            "status": item.get("status", ""),
            "extendedStatus": item.get("extendedStatus", ""),
        }
        for item in _response_json(response).get("value", [])
    ]


def _get_refresh_schedule(workspace_id: str, dataset_id: str) -> dict[str, Any]:
    from sempy_labs._helper_functions import _base_api

    try:
        response = _base_api(
            request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
            client="fabric_sp",
        )
        value = _response_json(response)
        return {
            "exists": True,
            "enabled": bool(value.get("enabled")),
            "days": value.get("days", []),
            "times": value.get("times", []),
            "localTimeZoneId": value.get("localTimeZoneId", "UTC"),
            "notifyOption": value.get("notifyOption", "NoNotification"),
        }
    except Exception as exc:
        return {"exists": False, "message": str(exc)}


@log
def refresh_manager(
    dataset: Optional[str | UUID] = None,
    workspace: Optional[str | UUID] = None,
    dark_mode: bool = False,
):
    """Display an interactive semantic model refresh manager.

    The manager can refresh an entire semantic model, selected tables, or
    selected partitions. It also displays live status, recent enhanced-refresh
    history, execution details, and the model's scheduled-refresh settings.

    Parameters
    ----------
    dataset : str | uuid.UUID, default=None
        Name or ID of the semantic model. Defaults to None which opens a
        workspace / semantic model picker.
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached
        lakehouse or, if no lakehouse is attached, the notebook workspace.
    dark_mode : bool, default=False
        If True, render the interface using the dark theme.
    """
    try:
        import anywidget
        import traitlets
    except ImportError as exc:
        raise ImportError(
            "The 'refresh_manager' function requires the 'anywidget' package. "
            "Install it with: pip install anywidget"
        ) from exc

    import sempy.fabric as fabric
    from IPython.display import display
    from sempy_labs._helper_functions import (
        _base_api,
        resolve_dataset_name_and_id,
        resolve_workspace_name_and_id,
    )

    workspace_name, workspace_id = resolve_workspace_name_and_id(workspace)
    workspace_id = str(workspace_id)
    connected = dataset is not None
    if connected:
        dataset_name, dataset_id = resolve_dataset_name_and_id(dataset, workspace_id)
        dataset_id = str(dataset_id)
    else:
        dataset_name, dataset_id = "", ""

    def list_workspaces() -> list[dict[str, str]]:
        try:
            dataframe = fabric.list_workspaces()
        except Exception:
            return [{"id": workspace_id, "name": str(workspace_name or "")}]
        id_column = "Id" if "Id" in dataframe.columns else dataframe.columns[0]
        name_column = "Name" if "Name" in dataframe.columns else dataframe.columns[-1]
        return sorted(
            [
                {"id": str(row[id_column]), "name": str(row[name_column])}
                for _, row in dataframe.iterrows()
            ],
            key=lambda item: item["name"].lower(),
        )

    def list_datasets(target_workspace_id: str) -> list[dict[str, str]]:
        try:
            dataframe = fabric.list_datasets(
                workspace=target_workspace_id, mode="rest"
            )
        except Exception:
            return []
        id_column = next(
            (
                column
                for column in ["Dataset Id", "Dataset ID", "Id"]
                if column in dataframe.columns
            ),
            dataframe.columns[0] if len(dataframe.columns) else None,
        )
        name_column = next(
            (
                column
                for column in ["Dataset Name", "Name"]
                if column in dataframe.columns
            ),
            dataframe.columns[-1] if len(dataframe.columns) else None,
        )
        if id_column is None or name_column is None:
            return []
        return sorted(
            [
                {"id": str(row[id_column]), "name": str(row[name_column])}
                for _, row in dataframe.iterrows()
            ],
            key=lambda item: item["name"].lower(),
        )

    initial_workspaces = [{"id": workspace_id, "name": workspace_name}]
    initial_datasets = {}

    class _RefreshManagerWidget(anywidget.AnyWidget):
        _esm = _WIDGET_JS
        _css = _WIDGET_CSS

        dataset_name = traitlets.Unicode("").tag(sync=True)
        workspace_name = traitlets.Unicode("").tag(sync=True)
        workspace_id = traitlets.Unicode("").tag(sync=True)
        connected = traitlets.Bool(False).tag(sync=True)
        workspaces = traitlets.List(default_value=[]).tag(sync=True)
        datasets = traitlets.Dict(default_value={}).tag(sync=True)
        dark_mode = traitlets.Bool(False).tag(sync=True)
        objects = traitlets.List(default_value=[]).tag(sync=True)
        objects_loading = traitlets.Bool(False).tag(sync=True)
        history = traitlets.List(default_value=[]).tag(sync=True)
        history_loaded = traitlets.Bool(False).tag(sync=True)
        history_loading = traitlets.Bool(False).tag(sync=True)
        schedule = traitlets.Dict(default_value={}).tag(sync=True)
        schedule_loaded = traitlets.Bool(False).tag(sync=True)
        schedule_loading = traitlets.Bool(False).tag(sync=True)
        detail = traitlets.Dict(default_value={}).tag(sync=True)
        refresh_status = traitlets.Dict(default_value={}).tag(sync=True)
        gantt_events = traitlets.List(default_value=[]).tag(sync=True)
        refresh_id = traitlets.Unicode("").tag(sync=True)
        busy = traitlets.Bool(False).tag(sync=True)
        pending_action = traitlets.Dict(default_value={}).tag(sync=True)
        run = traitlets.Int(0).tag(sync=True)
        connect_done = traitlets.Int(0).tag(sync=True)

    widget = _RefreshManagerWidget(
        dataset_name=dataset_name,
        workspace_name=workspace_name,
        workspace_id=workspace_id,
        connected=connected,
        workspaces=initial_workspaces,
        datasets=initial_datasets,
        dark_mode=bool(dark_mode),
    )

    def set_error(message: str) -> None:
        widget.refresh_status = {
            "status": "Failed",
            "kind": "error",
            "message": message,
        }

    def load_objects() -> None:
        widget.objects_loading = True
        try:
            widget.objects = _get_refresh_objects(dataset_id, workspace_id)
        except Exception as exc:
            set_error(f"Unable to load tables and partitions: {exc}")
        finally:
            widget.objects_loading = False

    def load_history() -> None:
        widget.history_loading = True
        try:
            widget.history = _get_refresh_history(workspace_id, dataset_id)
        except Exception as exc:
            set_error(f"Unable to load refresh history: {exc}")
        finally:
            widget.history_loaded = True
            widget.history_loading = False

    def load_schedule() -> None:
        widget.schedule_loading = True
        try:
            widget.schedule = _get_refresh_schedule(workspace_id, dataset_id)
        finally:
            widget.schedule_loaded = True
            widget.schedule_loading = False

    def submit_refresh(data: dict[str, Any]) -> tuple[str, str]:
        refresh_type = str(data.get("refresh_type", "Full"))
        request_id = fabric.refresh_dataset(
            dataset=dataset_id,
            workspace=workspace_id,
            refresh_type={
                "Full": "full",
                "Automatic": "automatic",
                "Calculate": "calculate",
                "DataOnly": "dataOnly",
                "ClearValues": "clearValues",
                "Defragment": "defragment",
            }.get(refresh_type, "full"),
            retry_count=max(0, int(data.get("retry_count", 0))),
            apply_refresh_policy=bool(data.get("apply_refresh_policy", False)),
            max_parallelism=max(1, int(data.get("max_parallelism", 10))),
            commit_mode=(
                "partialBatch"
                if data.get("commit_mode") == "PartialBatch"
                else "transactional"
            ),
            objects=data.get("objects") or None,
        )
        return str(request_id), refresh_type

    def update_refresh_status(details: dict[str, Any], refresh_type: str) -> str:
        status = details.get("status", "Unknown")
        messages = details.get("messages", [])
        message = "\n".join(
            filter(
                None,
                (
                    f"{item.get('type', '')}: {item.get('message', '')}".strip(": ")
                    for item in messages
                ),
            )
        )
        widget.refresh_status = {
            "status": status,
            "kind": "error" if status == "Failed" else "",
            "message": message or f"{refresh_type} refresh status: {status}",
            "refreshType": details.get("currentRefreshType")
            or details.get("type", refresh_type),
            "startTime": details.get("startTime", ""),
            "endTime": details.get("endTime", ""),
            "objects": details.get("objects", []),
        }
        return status

    def poll_refresh(request_id: str, refresh_type: str) -> None:
        try:
            while True:
                response = _base_api(
                    request=(
                        f"/v1.0/myorg/groups/{workspace_id}/datasets/"
                        f"{dataset_id}/refreshes/{request_id}"
                    ),
                    status_codes=[200, 202],
                )
                details = _response_json(response)
                status = update_refresh_status(details, refresh_type)
                if status in {"Completed", "Failed", "Cancelled", "Disabled"}:
                    break
                time.sleep(3)
            load_history()
            load_objects()
        except Exception as exc:
            set_error(str(exc))
        finally:
            widget.busy = False
            widget.refresh_id = ""

    def run_visualized_refresh(data: dict[str, Any]) -> None:
        import sempy_labs._icons as icons
        from sempy_labs._helper_functions import _get_partition_map

        try:
            partition_map = _get_partition_map(dataset_id, workspace_id)
            with fabric.create_trace_connection(
                dataset=dataset_id, workspace=workspace_id
            ) as trace_connection:
                with trace_connection.create_trace(icons.refresh_event_schema) as trace:
                    trace.start()
                    request_id, refresh_type = submit_refresh(data)
                    widget.refresh_id = request_id
                    status = "Unknown"
                    while True:
                        response = _base_api(
                            request=(
                                f"/v1.0/myorg/groups/{workspace_id}/datasets/"
                                f"{dataset_id}/refreshes/{request_id}"
                            ),
                            status_codes=[200, 202],
                        )
                        status = update_refresh_status(
                            _response_json(response), refresh_type
                        )
                        widget.gantt_events = _read_refresh_trace_events(
                            trace, partition_map
                        )
                        if status in {
                            "Completed",
                            "Failed",
                            "Cancelled",
                            "Disabled",
                        }:
                            break
                        time.sleep(3)
                    if status == "Completed":
                        time.sleep(5)
                        widget.gantt_events = _read_refresh_trace_events(
                            trace, partition_map, stop=True
                        )
            load_history()
            load_objects()
        except Exception as exc:
            set_error(str(exc))
        finally:
            widget.busy = False
            widget.refresh_id = ""

    def start_refresh(data: dict[str, Any]) -> None:
        if widget.busy:
            return
        widget.busy = True
        widget.refresh_status = {
            "status": "Unknown",
            "message": "Submitting enhanced refresh request...",
        }
        widget.gantt_events = []
        try:
            if bool(data.get("visualize")):
                threading.Thread(
                    target=run_visualized_refresh,
                    args=(dict(data),),
                    daemon=True,
                ).start()
            else:
                request_id, refresh_type = submit_refresh(data)
                widget.refresh_id = request_id
                threading.Thread(
                    target=poll_refresh,
                    args=(request_id, refresh_type),
                    daemon=True,
                ).start()
        except Exception as exc:
            widget.busy = False
            set_error(str(exc))

    def cancel_refresh() -> None:
        if not widget.refresh_id:
            return
        _base_api(
            request=(
                f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/"
                f"refreshes/{widget.refresh_id}"
            ),
            method="delete",
        )
        widget.refresh_status = {
            "status": "Unknown",
            "message": "Cancellation requested...",
        }

    def save_schedule(data: dict[str, Any]) -> None:
        schedule = dict(data.get("schedule") or {})
        _base_api(
            request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
            method="patch",
            payload={
                "value": {
                    "days": schedule.get("days", []),
                    "times": schedule.get("times", []),
                    "enabled": True,
                    "localTimeZoneId": schedule.get("localTimeZoneId", "UTC"),
                    "notifyOption": schedule.get("notifyOption", "NoNotification"),
                }
            },
            client="fabric_sp",
        )
        load_schedule()

    def on_run(_change: dict[str, Any]) -> None:
        nonlocal workspace_name, workspace_id, dataset_name, dataset_id
        data = dict(widget.pending_action or {})
        action = data.get("action")
        try:
            if action == "load_objects":
                load_objects()
            elif action == "list_workspaces":
                widget.workspaces = list_workspaces()
            elif action == "reload_picker":
                widget.workspaces = list_workspaces()
                target_workspace_id = str(data.get("workspace_id") or "")
                if target_workspace_id:
                    datasets = dict(widget.datasets)
                    datasets[target_workspace_id] = list_datasets(target_workspace_id)
                    widget.datasets = datasets
            elif action == "list_datasets":
                target_workspace_id = str(data.get("workspace_id") or "")
                if target_workspace_id:
                    datasets = dict(widget.datasets)
                    datasets[target_workspace_id] = list_datasets(target_workspace_id)
                    widget.datasets = datasets
            elif action == "connect":
                target_workspace_id = str(data.get("workspace_id") or "")
                target_dataset_id = str(data.get("dataset_id") or "")
                if not target_workspace_id or not target_dataset_id:
                    raise ValueError("Select a workspace and semantic model.")
                workspace_id = target_workspace_id
                dataset_id = target_dataset_id
                workspace_name = str(data.get("workspace_name") or "")
                dataset_name = str(data.get("dataset_name") or "")
                widget.workspace_id = workspace_id
                widget.workspace_name = workspace_name
                widget.dataset_name = dataset_name
                widget.objects = []
                widget.history = []
                widget.history_loaded = False
                widget.schedule = {}
                widget.schedule_loaded = False
                widget.detail = {}
                widget.refresh_status = {}
                widget.gantt_events = []
                widget.refresh_id = ""
                widget.connected = True
                load_objects()
                widget.connect_done = widget.connect_done + 1
            elif action == "load_history":
                load_history()
            elif action == "load_schedule":
                load_schedule()
            elif action == "load_detail":
                request_id = str(data.get("request_id") or "")
                response = _base_api(
                    request=(
                        f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/"
                        f"refreshes/{request_id}"
                    )
                )
                widget.detail = _refresh_detail_json(response, request_id)
            elif action == "start_refresh":
                start_refresh(data)
            elif action == "cancel_refresh":
                cancel_refresh()
            elif action == "save_schedule":
                save_schedule(data)
            elif action == "toggle_schedule":
                _base_api(
                    request=f"/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshSchedule",
                    method="patch",
                    payload={"value": {"enabled": bool(data.get("enabled"))}},
                    client="fabric_sp",
                )
                load_schedule()
        except Exception as exc:
            set_error(str(exc))

    widget.observe(on_run, names=["run"])
    display(widget)

    if not connected:

        def load_initial_workspaces() -> None:
            widget.workspaces = list_workspaces()

        def load_initial_datasets() -> None:
            datasets = dict(widget.datasets)
            datasets[workspace_id] = list_datasets(workspace_id)
            widget.datasets = datasets

        threading.Thread(target=load_initial_workspaces, daemon=True).start()
        threading.Thread(target=load_initial_datasets, daemon=True).start()
