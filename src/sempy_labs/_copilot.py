from IPython.display import display, HTML
import uuid


def copilot():
    session_id = str(uuid.uuid4()).replace("-", "")

    html = f"""
    <style>
        #chat-container-{session_id} {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            width: 420px;
            height: 520px;
            border-radius: 18px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.12);
            background: #f5f5f7;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }}

        #chat-header-{session_id} {{
            padding: 14px 16px;
            background: rgba(255,255,255,0.85);
            backdrop-filter: blur(12px);
            font-weight: 600;
            font-size: 15px;
            border-bottom: 1px solid #ddd;
        }}

        #chat-messages-{session_id} {{
            flex: 1;
            padding: 16px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 10px;
        }}

        .bubble {{
            max-width: 75%;
            padding: 10px 14px;
            border-radius: 18px;
            font-size: 14px;
            line-height: 1.35;
            word-wrap: break-word;
        }}

        .user {{
            align-self: flex-end;
            background: #007aff;
            color: white;
            border-bottom-right-radius: 6px;
        }}

        .assistant {{
            align-self: flex-start;
            background: white;
            color: #1c1c1e;
            border-bottom-left-radius: 6px;
        }}

        #chat-input-area-{session_id} {{
            display: flex;
            align-items: center;
            padding: 10px;
            background: rgba(255,255,255,0.9);
            border-top: 1px solid #ddd;
        }}

        #chat-input-{session_id} {{
            flex: 1;
            border: none;
            outline: none;
            padding: 10px 12px;
            font-size: 14px;
            border-radius: 14px;
            background: #f1f1f3;
        }}

        #send-btn-{session_id} {{
            margin-left: 8px;
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
        }}

        #send-btn-{session_id}:hover {{
            background: #0060df;
        }}
    </style>

    <div id="chat-container-{session_id}">
        <div id="chat-header-{session_id}">Chat</div>

        <div id="chat-messages-{session_id}"></div>

        <div id="chat-input-area-{session_id}">
            <input
                id="chat-input-{session_id}"
                placeholder="Message"
                autocomplete="off"
            />
            <button id="send-btn-{session_id}">↑</button>
        </div>
    </div>

    <script>
        const input = document.getElementById("chat-input-{session_id}");
        const messages = document.getElementById("chat-messages-{session_id}");
        const sendBtn = document.getElementById("send-btn-{session_id}");

        function addMessage(text, cls) {{
            const div = document.createElement("div");
            div.className = "bubble " + cls;
            div.textContent = text;
            messages.appendChild(div);
            messages.scrollTop = messages.scrollHeight;
        }}

        function sendMessage() {{
            const text = input.value.trim();
            if (!text) return;

            addMessage(text, "user");
            addMessage(text, "assistant");

            input.value = "";
        }}

        sendBtn.onclick = sendMessage;

        input.addEventListener("keydown", e => {{
            if (e.key === "Enter") {{
                sendMessage();
            }}
        }});
    </script>
    """

    display(HTML(html))