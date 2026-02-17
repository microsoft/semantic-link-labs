from IPython.display import display, HTML
import uuid
from sempy._utils._log import log


def copilot():
    session_id = str(uuid.uuid4()).replace("-", "")

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
            max-width: 75%;
            padding: 10px 14px;
            border-radius: 18px;
            font-size: 14px;
            line-height: 1.35;
            word-wrap: break-word;
            white-space: pre-wrap;
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
                <span>Chat</span>
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
    """

    # JavaScript with regex patterns kept in a regular string to avoid
    # f-string mangling of \\b, \\s, \\w, \\d, \\n etc.
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

            function generateResponse(text) {
                var lower = text.toLowerCase();

                // ---- rebind report ----
                if (lower.includes("rebind") && lower.includes("report")) {
                    var quoted = extractQuoted(text);
                    var report = quoted[0] || "MyReport";
                    var dataset = null;
                    var reportWs = null;
                    var datasetWs = null;

                    var rebindRe = /rebind\s+report\s+['"]([\w\s]+)['"](?:\s+in\s+workspace\s+['"]([\w\s]+)['"])?\s+to\s+(?:semantic\s+model|dataset)\s+['"]([\w\s]+)['"](?:\s+in\s+workspace\s+['"]([\w\s]+)['"])?/i;
                    var match = text.match(rebindRe);
                    if (match) {
                        report = match[1];
                        reportWs = match[2] || null;
                        dataset = match[3];
                        datasetWs = match[4] || null;
                    }

                    if (!dataset) dataset = quoted.length > 1 ? quoted[1] : "MyModel";
                    if (!reportWs && quoted.length > 2) reportWs = quoted[2];
                    if (!datasetWs && quoted.length > 3) datasetWs = quoted[3];

                    var code = "import sempy_labs.report\n";
                    code += "sempy_labs.report.report_rebind(\n";
                    code += "    report='" + report + "',\n";
                    code += "    dataset='" + dataset + "',\n";
                    if (reportWs) code += "    report_workspace='" + reportWs + "',\n";
                    if (datasetWs) code += "    dataset_workspace='" + datasetWs + "',\n";
                    code += ")";

                    addMessage("Here's the code to rebind the report:", "assistant");
                    addCodeBlock(code);
                    return;
                }

                // ---- fallback: echo ----
                addMessage(text, "assistant");
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