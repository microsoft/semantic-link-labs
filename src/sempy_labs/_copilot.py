from IPython.display import display, HTML
import uuid
import json
import inspect
import os
import re as _re
from sempy._utils._log import log


def _parse_skills():
    """
    Parse skills.md to extract intent-override rules.
    Returns a list of dicts with keys: patterns, fn, and optionally code_template.
    code_template may contain {dataset} and {workspace} placeholders.
    """
    #skills_path = "/home/trusted-service-user/jupyter-env/python3.11/lib/python3.11/site-packages/sempy_labs/skills.md"
    skills_path = os.path.join(os.path.dirname(__file__), "skills.md")

    if not os.path.exists(skills_path):
        return []

    with open(skills_path, "r", encoding="utf-8") as f:
        text = f.read()

    overrides = []
    current_patterns = None
    current_fn = None
    current_code = None
    in_code_block = False
    code_lines = []

    for line in text.splitlines():
        stripped = line.strip()

        # Code fence handling
        if in_code_block:
            if stripped.startswith("```"):
                current_code = "\n".join(code_lines)
                in_code_block = False
                code_lines = []
            else:
                code_lines.append(line)
            continue

        m_pat = _re.match(r"^-\s+\*\*Patterns\*\*:\s*(.+)$", stripped)
        m_fn = _re.match(r"^-\s+\*\*Function\*\*:\s*(\S+)", stripped)
        m_code = _re.match(r"^-\s+\*\*Code\*\*:", stripped)
        is_heading = stripped.startswith("### ")

        if is_heading:
            # Flush previous rule if any
            if current_patterns and current_fn:
                entry = {"patterns": current_patterns, "fn": current_fn}
                if current_code:
                    entry["code_template"] = current_code
                overrides.append(entry)
            current_patterns = None
            current_fn = None
            current_code = None
        elif m_pat:
            current_patterns = [
                p.strip().lower() for p in m_pat.group(1).split(",") if p.strip()
            ]
        elif m_fn:
            current_fn = m_fn.group(1).strip()
        elif m_code:
            in_code_block = False
            code_lines = []
            # The code fence should be on the next line, but check this line too
        elif stripped.startswith("```python") or stripped.startswith("```"):
            if current_patterns:  # only capture code blocks inside a rule
                in_code_block = True
                code_lines = []

    # Flush last rule
    if current_patterns and current_fn:
        entry = {"patterns": current_patterns, "fn": current_fn}
        if current_code:
            entry["code_template"] = current_code
        overrides.append(entry)

    return overrides


def _build_function_registry():
    """
    Introspect sempy_labs and its submodules to build a registry of public
    functions with their signatures, module paths, and descriptions.
    """
    import sempy_labs

    registry = []
    seen = set()

    def _add_module(mod, import_path):
        all_names = getattr(mod, "__all__", [])
        for name in all_names:
            obj = getattr(mod, name, None)
            if obj is None or not callable(obj):
                continue
            obj_id = id(obj)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            try:
                sig = inspect.signature(obj)
            except (ValueError, TypeError):
                continue

            params = []
            for pname, param in sig.parameters.items():
                if pname in ("self", "cls", "kwargs", "args"):
                    continue
                if pname.startswith("_"):
                    continue
                p_info = {"name": pname}
                if param.default is not inspect.Parameter.empty:
                    d = param.default
                    if d is None:
                        p_info["default"] = None
                        p_info["optional"] = True
                    elif isinstance(d, (str, int, float, bool)):
                        p_info["default"] = d
                        p_info["optional"] = True
                    else:
                        p_info["optional"] = True
                else:
                    p_info["optional"] = False
                params.append(p_info)

            doc = inspect.getdoc(obj) or ""
            description = doc.split("\n")[0] if doc else ""

            # Build search keywords from function name
            keywords = name.replace("_", " ").lower().split()

            # Common abbreviation aliases: maps alias word -> keyword index
            _abbrev = {
                "bpa": ["best", "practice", "analyzer"],
                "qso": ["query", "scale", "out"],
                "dl": ["direct", "lake"],
            }
            aliases = {}
            for ki, kw in enumerate(keywords):
                if kw in _abbrev:
                    for aw in _abbrev[kw]:
                        aliases[aw] = ki
            # Also add description words as searchable terms
            desc_words = description.lower().replace(".", " ").split()

            registry.append(
                {
                    "name": name,
                    "module": import_path,
                    "params": params,
                    "description": description,
                    "keywords": keywords,
                    "aliases": aliases,
                    "desc_words": desc_words,
                }
            )

    # Process submodules first so they get the more specific import path
    submodules = [
        "admin",
        "apache_airflow_job",
        "connection",
        "data_pipeline",
        "dataflow",
        "daxlib",
        "directlake",
        "environment",
        "event_schema_set",
        "eventhouse",
        "eventstream",
        "external_data_share",
        "gateway",
        "git",
        "graph",
        "graph_model",
        "graphql",
        "kql_dashboard",
        "kql_database",
        "kql_queryset",
        "lakehouse",
        "managed_private_endpoint",
        "migration",
        "mirrored_azure_databricks_catalog",
        "mirrored_database",
        "mirrored_warehouse",
        "ml_experiment",
        "ml_model",
        "mounted_data_factory",
        "notebook",
        "operations_agent",
        "report",
        "rti",
        "semantic_model",
        "snowflake_database",
        "spark",
        "sql_database",
        "sql_endpoint",
        "surge_protection",
        "tom",
        "warehouse",
        "warehouse_snapshot",
        "workspace",
    ]
    for sub in submodules:
        try:
            mod = __import__(f"sempy_labs.{sub}", fromlist=[sub])
            _add_module(mod, f"sempy_labs.{sub}")
        except ImportError:
            continue

    # Top-level functions (skips any already seen from submodules)
    _add_module(sempy_labs, "sempy_labs")

    return registry


@log
def chat():
    session_id = str(uuid.uuid4()).replace("-", "")

    # Build function registry from library introspection
    registry = _build_function_registry()
    registry_json = json.dumps(registry)

    # Load intent overrides from skills.md
    overrides = _parse_skills()
    overrides_json = json.dumps(overrides)

    html = f"""
    <style>
        #chat-wrapper-{session_id} {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            display: flex;
            flex-direction: row;
            gap: 0;
            height: 560px;
        }}

        /* ---- History sidebar ---- */
        #history-panel-{session_id} {{
            width: 200px;
            min-width: 200px;
            background: #ffffff;
            border-radius: 18px 0 0 18px;
            border: 1px solid #ddd;
            border-right: none;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            transition: width 0.2s, min-width 0.2s, padding 0.2s, opacity 0.2s;
        }}

        #history-panel-{session_id}.collapsed {{
            width: 0;
            min-width: 0;
            padding: 0;
            opacity: 0;
            border: none;
        }}

        #history-header-{session_id} {{
            padding: 14px 12px;
            font-weight: 600;
            font-size: 13px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-bottom: 1px solid #eee;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}

        #history-list-{session_id} {{
            flex: 1;
            overflow-y: auto;
            padding: 8px 0;
        }}

        .history-item-{session_id} {{
            padding: 8px 12px;
            font-size: 13px;
            color: #333;
            cursor: pointer;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            border-bottom: 1px solid #f0f0f0;
        }}

        .history-item-{session_id}:hover {{
            background: #f0f0f5;
        }}

        #history-toggle-{session_id} {{
            background: none;
            border: 1px solid #ddd;
            border-radius: 8px;
            cursor: pointer;
            font-size: 14px;
            padding: 4px 8px;
            color: #666;
        }}

        #history-toggle-{session_id}:hover {{
            background: #f0f0f5;
        }}

        /* ---- Chat panel ---- */
        #chat-container-{session_id} {{
            width: 480px;
            border-radius: 0 18px 18px 0;
            box-shadow: 0 10px 30px rgba(0,0,0,0.12);
            background: #f5f5f7;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            border: 1px solid #ddd;
        }}

        #history-panel-{session_id}.collapsed + #chat-container-{session_id} {{
            border-radius: 18px;
        }}

        #chat-header-{session_id} {{
            padding: 14px 16px;
            background: rgba(255,255,255,0.85);
            backdrop-filter: blur(12px);
            font-weight: 600;
            font-size: 15px;
            border-bottom: 1px solid #ddd;
            display: flex;
            align-items: center;
            gap: 8px;
        }}

        #chat-messages-{session_id} {{
            flex: 1;
            padding: 16px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}

        .bubble-{session_id} {{
            max-width: 85%;
            min-width: 200px;
            padding: 10px 14px;
            border-radius: 18px;
            font-size: 14px;
            line-height: 1.35;
            word-wrap: break-word;
            white-space: pre-wrap;
            box-sizing: border-box;
            flex-shrink: 0;
        }}

        .user-{session_id} {{
            align-self: flex-end;
            background: #007aff;
            color: white;
            border-bottom-right-radius: 6px;
        }}

        .assistant-{session_id} {{
            align-self: flex-start;
            background: white;
            color: #1c1c1e;
            border-bottom-left-radius: 6px;
        }}

        #chat-input-area-{session_id} {{
            display: flex;
            align-items: flex-end;
            padding: 10px;
            background: rgba(255,255,255,0.9);
            border-top: 1px solid #ddd;
            gap: 8px;
        }}

        #chat-input-{session_id} {{
            flex: 1;
            border: none;
            outline: none;
            padding: 10px 12px;
            font-size: 14px;
            border-radius: 14px;
            background: #f1f1f3;
            resize: none;
            min-height: 60px;
            max-height: 120px;
            line-height: 1.4;
            font-family: inherit;
        }}

        #send-btn-{session_id} {{
            width: 34px;
            height: 34px;
            border-radius: 50%;
            border: none;
            background: #007aff;
            color: white;
            font-size: 16px;
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
            margin-bottom: 4px;
        }}

        #send-btn-{session_id}:hover {{
            background: #0060df;
        }}
    </style>

    <div id="chat-wrapper-{session_id}">
        <!-- History sidebar -->
        <div id="history-panel-{session_id}">
            <div id="history-header-{session_id}">
                <span>History</span>
                <button id="history-toggle-{session_id}" title="Collapse history">◀</button>
            </div>
            <div id="history-list-{session_id}"></div>
        </div>

        <!-- Chat panel -->
        <div id="chat-container-{session_id}">
            <div id="chat-header-{session_id}">
                <span id="expand-history-{session_id}" style="display:none; cursor:pointer; font-size:18px;" title="Show history">▶</span>
                <span>Sempy Chat</span>
            </div>

            <div id="chat-messages-{session_id}"></div>

            <div id="chat-input-area-{session_id}">
                <textarea
                    id="chat-input-{session_id}"
                    placeholder="Message"
                    rows="3"
                ></textarea>
                <button id="send-btn-{session_id}">↑</button>
            </div>
        </div>
    </div>

    <script>
        (function() {{
            const SID = "{session_id}";
            const REGISTRY = {registry_json};
            const OVERRIDES = {overrides_json};
    """

    # JavaScript with regex patterns kept in a raw string to avoid
    # f-string mangling of \b, \s, \w, \d, \n etc.
    js_body = r"""
            const input = document.getElementById("chat-input-" + SID);
            const messages = document.getElementById("chat-messages-" + SID);
            const sendBtn = document.getElementById("send-btn-" + SID);
            const historyList = document.getElementById("history-list-" + SID);
            const historyPanel = document.getElementById("history-panel-" + SID);
            const historyToggle = document.getElementById("history-toggle-" + SID);
            const expandBtn = document.getElementById("expand-history-" + SID);

            // Auto-resize textarea
            input.addEventListener("input", function() {
                this.style.height = "auto";
                this.style.height = Math.min(this.scrollHeight, 120) + "px";
            });

            // Toggle history panel
            historyToggle.onclick = function() {
                historyPanel.classList.add("collapsed");
                expandBtn.style.display = "inline";
            };

            expandBtn.onclick = function() {
                historyPanel.classList.remove("collapsed");
                expandBtn.style.display = "none";
            };

            function addMessage(text, cls) {
                const div = document.createElement("div");
                div.className = "bubble-" + SID + " " + cls + "-" + SID;
                if (cls === "assistant") {
                    div.innerHTML = text.replace(/\n/g, "<br>");
                } else {
                    div.textContent = text;
                }
                messages.appendChild(div);
                messages.scrollTop = messages.scrollHeight;
            }

            // ---- Python syntax highlighter (VS Code Dark+ theme) ----
            // Single-pass tokenizer to avoid rules interfering with each other
            function highlightPython(code) {
                var html = code
                    .replace(/&/g, "&amp;")
                    .replace(/</g, "&lt;")
                    .replace(/>/g, "&gt;");

                var keywords = new Set(["import","from","as","def","class","return","if","elif","else","for","while","with","try","except","finally","raise","pass","break","continue","and","or","not","in","is","None","True","False","lambda","yield","assert","del","global","nonlocal","async","await"]);
                var builtins = new Set(["print","len","range","type","int","str","float","list","dict","set","tuple","bool","open","super","isinstance","getattr","setattr","hasattr","enumerate","zip","map","filter","sorted","reversed","any","all","min","max","sum","abs","round","format"]);

                // Single regex that matches tokens in priority order
                var tokenRe = /('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*")|(#[^\n]*)|(@\w+)|(\b\d+\.?\d*\b)|(\b[a-zA-Z_]\w*\b)/g;

                html = html.replace(tokenRe, function(match, str, comment, decorator, number, word) {
                    if (str) return '<span style="color:#ce9178">' + str + '</span>';
                    if (comment) return '<span style="color:#6a9955">' + comment + '</span>';
                    if (decorator) return '<span style="color:#dcdcaa">' + decorator + '</span>';
                    if (number) return '<span style="color:#b5cea8">' + number + '</span>';
                    if (word) {
                        if (keywords.has(word)) return '<span style="color:#569cd6">' + word + '</span>';
                        if (builtins.has(word)) return '<span style="color:#dcdcaa">' + word + '</span>';
                    }
                    return match;
                });

                return html;
            }

            function addCodeBlock(code) {
                const wrapper = document.createElement("div");
                wrapper.className = "bubble-" + SID + " assistant-" + SID;
                wrapper.style.fontFamily = "'SF Mono', 'Menlo', 'Consolas', monospace";
                wrapper.style.fontSize = "13px";
                wrapper.style.background = "#1e1e1e";
                wrapper.style.color = "#d4d4d4";
                wrapper.style.padding = "12px 14px";
                wrapper.style.borderRadius = "10px";
                wrapper.style.whiteSpace = "pre";
                wrapper.style.overflowX = "auto";
                wrapper.style.maxWidth = "90%";
                wrapper.style.lineHeight = "1.5";
                wrapper.innerHTML = highlightPython(code);
                messages.appendChild(wrapper);
                messages.scrollTop = messages.scrollHeight;
            }

            // ------- Intent matching -------
            function extractQuoted(text) {
                var matches = [];
                var re = /['"\u2018\u2019\u201c\u201d]([^'"\u2018\u2019\u201c\u201d]+)['"\u2018\u2019\u201c\u201d]/g;
                var m;
                while ((m = re.exec(text)) !== null) matches.push(m[1]);
                return matches;
            }

            // Score how well a function matches the user's prompt
            function scoreFunction(fn, words) {
                var score = 0;
                var fnWords = fn.keywords;
                var fnAliases = fn.aliases || {};
                var fnDescWords = fn.desc_words || [];
                var fnName = fn.name.toLowerCase();

                // Exact function name match (underscores removed)
                var nameNoUnder = fnName.replace(/_/g, "");
                var textNoSpaces = words.join("");
                if (nameNoUnder === textNoSpaces) return 1000;

                // Check each word in the prompt against keywords, aliases, and description words
                var matchedFnWords = new Set();
                for (var i = 0; i < words.length; i++) {
                    var w = words[i];
                    if (w.length < 2) continue;

                    // Keyword match (from function name)
                    for (var k = 0; k < fnWords.length; k++) {
                        if (fnWords[k] === w) {
                            score += 10;
                            matchedFnWords.add(k);
                        } else if (fnWords[k].indexOf(w) >= 0 || w.indexOf(fnWords[k]) >= 0) {
                            score += 3;
                            matchedFnWords.add(k);
                        }
                    }

                    // Alias match: aliases is a dict mapping alias_word -> keyword_index
                    if (fnAliases.hasOwnProperty(w)) {
                        score += 8;
                        matchedFnWords.add(fnAliases[w]);
                    }

                    // Description word match
                    for (var d = 0; d < fnDescWords.length; d++) {
                        if (fnDescWords[d] === w && w.length > 2) {
                            score += 2;
                        }
                    }
                }

                // Bonus: if the first keyword (action verb) is an exact match, boost score
                if (matchedFnWords.has(0)) {
                    for (var vi = 0; vi < words.length; vi++) {
                        if (fnWords[0] === words[vi]) { score += 5; break; }
                    }
                }

                // Penalize function keywords NOT mentioned in the prompt
                var unmatchedFnWords = 0;
                for (var u = 0; u < fnWords.length; u++) {
                    if (!matchedFnWords.has(u)) unmatchedFnWords++;
                }
                score -= unmatchedFnWords * 5;

                // Bonus for matching parameter names mentioned in prompt
                for (var p = 0; p < fn.params.length; p++) {
                    var pname = fn.params[p].name.replace(/_/g, " ").toLowerCase();
                    for (var j = 0; j < words.length; j++) {
                        if (pname.indexOf(words[j]) >= 0 && words[j].length > 2) {
                            score += 1;
                        }
                    }
                }

                return score;
            }

            function findBestFunction(text) {
                var lower = text.toLowerCase()
                    .replace(/['"]/g, " ")
                    .replace(/[^\w\s]/g, " ");

                // Intent overrides loaded from skills.md
                for (var oi = 0; oi < OVERRIDES.length; oi++) {
                    var entry = OVERRIDES[oi];
                    for (var pi = 0; pi < entry.patterns.length; pi++) {
                        if (lower.indexOf(entry.patterns[pi]) >= 0) {
                            // If override has a code_template, return a synthetic entry
                            if (entry.code_template) {
                                return {
                                    name: entry.fn,
                                    module: "",
                                    params: [],
                                    description: "",
                                    keywords: [],
                                    aliases: {},
                                    desc_words: [],
                                    _override_template: entry.code_template
                                };
                            }
                            for (var ri = 0; ri < REGISTRY.length; ri++) {
                                var fnBaseName = entry.fn.indexOf(".") >= 0 ? entry.fn.substring(entry.fn.lastIndexOf(".") + 1) : entry.fn;
                                if (REGISTRY[ri].name === fnBaseName) return REGISTRY[ri];
                            }
                        }
                    }
                }
                var words = lower.split(/\s+/).filter(function(w) { return w.length > 0; });
                // Remove common filler words
                var stopwords = new Set(["the","a","an","to","in","for","of","my","i","me","can","you","how","do","please","want","need","use","using","code","show","write","give","help","with","against","on","from","that","this","it","its","all","just"]);
                var filtered = words.filter(function(w) { return !stopwords.has(w); });
                if (filtered.length === 0) filtered = words;

                var best = null;
                var bestScore = 0;
                for (var i = 0; i < REGISTRY.length; i++) {
                    var s = scoreFunction(REGISTRY[i], filtered);
                    if (s > bestScore) {
                        bestScore = s;
                        best = REGISTRY[i];
                    }
                }
                return (bestScore >= 5) ? best : null;
            }

            function buildCode(fn, text) {
                var quoted = extractQuoted(text);
                var module = fn.module;
                var name = fn.name;
                var params = fn.params;

                // Map quoted values to required params first, then optional
                var required = params.filter(function(p) { return !p.optional; });
                var optional = params.filter(function(p) { return p.optional; });

                var assignments = {};
                var qi = 0;

                // Try to match quoted values to params by name hints in text
                var lower = text.toLowerCase();
                for (var i = 0; i < params.length; i++) {
                    var pname = params[i].name.replace(/_/g, " ").toLowerCase();
                    // Look for pattern: param_name 'value' or param_name "value"
                    var pwords = pname.split(" ");
                    for (var j = 0; j < quoted.length; j++) {
                        if (assignments[params[i].name]) break;
                        // Check if the param name words appear near this quoted value
                        var qval = quoted[j];
                        var qpos = lower.indexOf(qval.toLowerCase());
                        if (qpos < 0) continue;
                        var before = lower.substring(Math.max(0, qpos - 40), qpos);
                        var matched = pwords.some(function(pw) { return pw.length > 2 && before.indexOf(pw) >= 0; });
                        if (matched) {
                            assignments[params[i].name] = qval;
                        }
                    }
                }

                // Assign remaining quoted values to unassigned required params in order
                for (var r = 0; r < required.length; r++) {
                    if (!assignments[required[r].name] && qi < quoted.length) {
                        assignments[required[r].name] = quoted[qi++];
                    }
                }

                // Assign leftover quoted values to unassigned optional params in order
                for (var o = 0; o < optional.length; o++) {
                    if (!assignments[optional[o].name] && qi < quoted.length) {
                        assignments[optional[o].name] = quoted[qi++];
                    }
                }

                // Build code string
                var code = "import " + module + "\n";
                code += module + "." + name + "(\n";

                var hasAny = false;
                for (var a = 0; a < params.length; a++) {
                    var pn = params[a].name;
                    if (assignments[pn]) {
                        code += "    " + pn + '=\"' + assignments[pn] + '\",\n';
                        hasAny = true;
                    } else if (!params[a].optional) {
                        code += "    " + pn + "=...,  # required\n";
                        hasAny = true;
                    } else if (pn === "workspace") {
                        code += "    workspace=None,\n";
                        hasAny = true;
                    }
                }
                code += ")";

                return code;
            }

            // Fill {dataset} and {workspace} placeholders in a code template
            function fillTemplate(template, text) {
                var quoted = extractQuoted(text);
                var lower = text.toLowerCase();
                var dataset = null;
                var workspace = null;

                // Try contextual matching: look for "workspace" or "model/dataset/semantic" near quoted values
                for (var i = 0; i < quoted.length; i++) {
                    var qval = quoted[i];
                    var qpos = lower.indexOf(qval.toLowerCase());
                    if (qpos < 0) continue;
                    var before = lower.substring(Math.max(0, qpos - 50), qpos);
                    if (!workspace && (before.indexOf("workspace") >= 0)) {
                        workspace = qval;
                    } else if (!dataset && (before.indexOf("model") >= 0 || before.indexOf("dataset") >= 0 || before.indexOf("semantic") >= 0)) {
                        dataset = qval;
                    }
                }

                // Assign remaining by position: first unassigned -> dataset, second -> workspace
                for (var j = 0; j < quoted.length; j++) {
                    if (!dataset && quoted[j] !== workspace) { dataset = quoted[j]; continue; }
                    if (!workspace && quoted[j] !== dataset) { workspace = quoted[j]; continue; }
                }

                // Detect language names in the prompt
                var knownLanguages = ["afrikaans","albanian","amharic","arabic","armenian","azerbaijani","basque","belarusian","bengali","bosnian","bulgarian","burmese","catalan","cebuano","chichewa","chinese","corsican","croatian","czech","danish","dutch","english","esperanto","estonian","filipino","finnish","french","frisian","galician","georgian","german","greek","gujarati","haitian creole","hausa","hawaiian","hebrew","hindi","hmong","hungarian","icelandic","igbo","indonesian","irish","italian","japanese","javanese","kannada","kazakh","khmer","kinyarwanda","korean","kurdish","kyrgyz","lao","latin","latvian","lithuanian","luxembourgish","macedonian","malagasy","malay","malayalam","maltese","maori","marathi","mongolian","nepali","norwegian","odia","pashto","persian","polish","portuguese","punjabi","romanian","russian","samoan","scots gaelic","serbian","sesotho","shona","sindhi","sinhala","slovak","slovenian","somali","spanish","sundanese","swahili","swedish","tajik","tamil","tatar","telugu","thai","turkish","turkmen","ukrainian","urdu","uyghur","uzbek","vietnamese","welsh","xhosa","yiddish","yoruba","zulu"];
                var foundLanguages = [];
                var lowerWords = lower.split(/[\s,]+/);
                for (var li = 0; li < lowerWords.length; li++) {
                    var lw = lowerWords[li].replace(/[^a-z]/g, "");
                    if (lw.length > 2 && knownLanguages.indexOf(lw) >= 0) {
                        // Capitalize first letter
                        foundLanguages.push(lw.charAt(0).toUpperCase() + lw.slice(1));
                    }
                }

                var code = template;
                code = code.replace(/\{dataset\}/g, dataset ? '"' + dataset + '"' : "...");
                code = code.replace(/\{workspace\}/g, workspace ? '"' + workspace + '"' : "None");
                if (foundLanguages.length > 0) {
                    var langStr = "[" + foundLanguages.map(function(l) { return '"' + l + '"'; }).join(", ") + "]";
                    code = code.replace(/\{languages\}/g, langStr);
                } else {
                    code = code.replace(/\{languages\}/g, '["..."]  # specify language(s)');
                }
                // Generic placeholder fallback for any remaining {xyz} placeholders
                code = code.replace(/\{(\w+)\}/g, "...");
                return code;
            }

            function generateResponse(text) {
                var fn = findBestFunction(text);

                if (fn) {
                    // Check if this function came from an override with a code template
                    var template = null;
                    if (fn._override_template) {
                        template = fn._override_template;
                    }

                    if (template) {
                        var code = fillTemplate(template, text);
                        addMessage("Here's the code for <b>" + fn.name + "</b>:", "assistant");
                        addCodeBlock(code);
                    } else {
                        var code = buildCode(fn, text);
                        addMessage("Here's the code for <b>" + fn.name + "</b>:", "assistant");
                        addCodeBlock(code);
                    }
                    if (fn.description) {
                        addMessage(fn.description, "assistant");
                    }
                    return;
                }

                // ---- fallback ----
                addMessage("I couldn't find a matching function. Try describing what you want to do, for example:\n\n\u2022 rebind report 'A' to semantic model 'B'\n\u2022 list workspaces\n\u2022 refresh semantic model 'Sales'\n\u2022 export report 'MyReport'", "assistant");
            }

            function addHistory(text) {
                var item = document.createElement("div");
                item.className = "history-item-" + SID;
                item.textContent = text;
                item.title = text;
                historyList.insertBefore(item, historyList.firstChild);
            }

            function sendMessage() {
                var text = input.value.trim();
                if (!text) return;

                addMessage(text, "user");
                generateResponse(text);
                addHistory(text);

                input.value = "";
                input.style.height = "auto";
            }

            sendBtn.onclick = sendMessage;

            input.addEventListener("keydown", function(e) {
                if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    sendMessage();
                }
            });
        })();
    """

    html_end = """
    </script>
    """

    display(HTML(html + js_body + html_end))
