APP_UI_STYLES = """
<style>
:root {
    --refine-popover-width: 900px;
    --sidebar-width: 15rem;
}
#MainMenu,
[data-testid="stDeployButton"],
[data-testid="stDecoration"],
button[title="Deploy"] {
    display: none !important;
}
[data-testid="stSidebarCollapseButton"],
[data-testid="collapsedControl"],
button[title="Close sidebar"],
button[title="Open sidebar"],
button[aria-label="Close sidebar"],
button[aria-label="Open sidebar"] {
    display: none !important;
}
header[data-testid="stHeader"],
.stApp > header {
    display: none !important;
    height: 0 !important;
}
.stApp {
    background: var(--app-bg);
    color: var(--app-text);
}
.main .block-container {
    max-width: 980px;
    padding-top: 3.2rem;
    padding-bottom: 7rem;
}
.st-key-start_screen {
    padding-top: min(4vh, 2.5rem);
}
.st-key-start_screen h1 {
    margin-bottom: 0.6rem;
    color: var(--app-text);
}
.st-key-start_screen h4,
[data-testid="stMarkdownContainer"] h1,
[data-testid="stMarkdownContainer"] h2,
[data-testid="stMarkdownContainer"] h3,
[data-testid="stMarkdownContainer"] h4 {
    color: var(--app-text);
}
.st-key-start_screen [data-testid="stMarkdownContainer"] p {
    color: var(--app-muted);
}
.compact-title {
    color: var(--app-muted);
    font-size: 0.9rem;
    margin-bottom: 1.5rem;
}
.st-key-response-actions {
    width: 100%;
    margin-top: 0.25rem;
    gap: 0.6rem;
}
.st-key-response-actions [data-testid="stHorizontalBlock"] {
    width: auto !important;
    flex: 0 0 auto !important;
    justify-content: flex-start !important;
}
.st-key-response-actions [data-testid="stHorizontalBlock"] > div,
.st-key-response-actions [data-testid="stElementContainer"] {
    width: auto !important;
    flex: 0 0 auto !important;
}
.st-key-response-actions button[kind="tertiary"],
.st-key-response-actions button[kind="primary"] {
    min-width: 1.75rem;
    padding: 0.2rem;
}
.st-key-response-actions button[kind="tertiary"] p,
.st-key-response-actions button[kind="primary"] p {
    display: none;
}
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
[data-testid="stMainBlockContainer"],
[data-testid="stBottom"],
[data-testid="stBottom"] > div {
    background: var(--app-bg) !important;
}
.stButton > button {
    min-height: 2.2rem;
    border: 1px solid var(--app-border);
    background: var(--app-surface);
    color: var(--app-text);
    border-radius: 0.45rem;
    box-shadow: none;
}
.stButton > button:hover {
    border-color: var(--app-accent);
    background: var(--app-accent-soft);
    color: var(--app-text);
}
.stButton > button[kind="primary"] {
    border-color: var(--app-accent);
    background: var(--app-accent);
    color: var(--app-primary-text);
}
.stButton > button p,
[data-testid="stMarkdownContainer"] p,
[data-testid="stCaptionContainer"],
[data-testid="stExpander"] summary,
[data-testid="stExpander"] p {
    color: inherit;
}
[data-testid="stAlert"] {
    border: 1px solid var(--app-border);
    background: var(--app-surface);
    color: var(--app-text);
}
[data-testid="stAlert"] p {
    color: var(--app-text);
}
[data-testid="stExpander"] {
    border-color: var(--app-border);
    background: var(--app-bg);
}
[data-testid="stExpander"] details,
[data-testid="stExpander"] summary {
    background: var(--app-bg);
    color: var(--app-text);
}
[data-testid="stSelectbox"] label,
[data-testid="stTextInput"] label {
    color: var(--app-muted);
}
[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
[data-testid="stTextInput"] input {
    border-color: var(--app-border);
    background: var(--app-input-bg);
    color: var(--app-text);
}
[data-testid="stSelectbox"] div[data-baseweb="select"] span,
[data-testid="stSelectbox"] svg {
    color: var(--app-text);
    fill: var(--app-text);
}
[data-testid="stSidebar"] {
    background: var(--app-sidebar);
    min-width: var(--sidebar-width) !important;
    width: var(--sidebar-width) !important;
}
[data-testid="stSidebarContent"] {
    padding-bottom: 5.25rem;
}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
    color: var(--app-muted);
}
[data-testid="stSidebar"] .stButton > button {
    min-height: 2rem; padding: 0.25rem 0.45rem; border: 0;
    background: transparent; color: var(--app-muted);
    font-size: 0.8rem; font-weight: 400; text-align: left;
    border-radius: 0.45rem;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background: var(--app-accent-soft); color: var(--app-text);
}
[data-testid="stSidebar"] [class*="st-key-history_actions"]
[data-testid="stHorizontalBlock"] {
    align-items: center;
}
[data-testid="stSidebar"] [class*="st-key-history_actions"] .stButton > button {
    align-items: center !important;
    display: inline-flex !important;
    height: 2rem !important;
    justify-content: center;
    min-height: 2rem !important;
    padding-bottom: 0 !important;
    padding-top: 0 !important;
}
[data-testid="stSidebar"] [class*="st-key-sidebar_toggle"] button {
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    color: var(--app-muted) !important;
}
[data-testid="stSidebar"] [class*="st-key-sidebar_toggle"] button:hover {
    background: var(--app-accent-soft) !important;
    color: var(--app-text) !important;
}
[data-testid="stSidebar"] [class*="st-key-sidebar_toggle"] button svg {
    color: currentColor !important;
    fill: currentColor !important;
}
[data-testid="stSidebar"] [data-testid="stVerticalBlock"] {
    gap: 0.25rem;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"],
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"] {
    margin: 0;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[data-testid="stHorizontalBlock"],
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[data-testid="stHorizontalBlock"] {
    gap: 0.15rem;
    align-items: center;
}
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
.stButton:first-child > button {
    background: var(--app-accent-soft);
    color: var(--app-text);
}
[data-testid="stSidebar"] [class*="st-key-history_"] button,
[data-testid="stSidebar"] [class*="st-key-history_"] button p,
[data-testid="stSidebar"] [class*="st-key-history_"] button span,
[data-testid="stSidebar"] [class*="st-key-history_"] button svg {
    color: var(--app-text) !important;
    fill: var(--app-text) !important;
}
[data-testid="stSidebar"] [class*="st-key-history_"] button p {
    display: block;
    max-width: 100%;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] {
    opacity: 0;
    pointer-events: none;
    transition: opacity 120ms ease;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]:hover
[class*="st-key-chat_menu_"],
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]:hover
[class*="st-key-chat_menu_"] {
    opacity: 1;
    pointer-events: auto;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button,
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button:hover {
    min-width: 1.8rem; padding: 0.1rem 0; background: transparent;
    color: var(--app-muted);
    justify-content: center;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button p {
    display: block;
}
[data-testid="stSidebar"] [data-testid="stPopover"] button {
    border: 0; background: transparent; color: var(--app-muted);
    font-size: 0.8rem;
}
[data-testid="stSidebar"] [data-testid="stExpander"] {
    border: 0;
    background: transparent;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary,
[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
[data-testid="stSidebar"] [data-testid="stExpander"] summary span,
[data-testid="stSidebar"] [data-testid="stExpander"] summary svg {
    color: var(--app-muted) !important;
    fill: var(--app-muted) !important;
    font-size: 0.8rem;
}
[data-testid="stSidebar"] [class*="st-key-sidebar_footer"] {
    position: fixed;
    bottom: 0;
    left: 0;
    width: var(--sidebar-width);
    max-width: 100%;
    box-sizing: border-box;
    padding: 0.75rem 1rem 1rem;
    background: var(--app-sidebar);
    border-top: 1px solid var(--app-border);
    z-index: 20;
}
[data-testid="stSidebar"] [class*="st-key-sidebar_footer"] label,
[data-testid="stSidebar"] [class*="st-key-sidebar_footer"] p {
    color: var(--app-muted);
    font-size: 0.8rem;
}
[data-testid="stChatInput"] {
    max-width: 980px;
    margin: 0 auto;
    background: var(--app-input-bg);
    border: 1px solid var(--app-border);
    border-radius: 0.6rem;
    box-shadow: none;
}
[data-testid="stChatInput"] > div {
    background: transparent;
}
[data-testid="stChatInput"] textarea,
[data-testid="stChatInput"] textarea:focus {
    background: var(--app-input-bg);
    color: var(--app-text);
    caret-color: var(--app-accent);
}
[data-testid="stChatInput"] textarea::placeholder,
[data-testid="stTextInput"] input::placeholder {
    color: var(--app-muted);
}
[data-testid="stChatInput"] button {
    background: var(--app-accent-soft);
    color: var(--app-accent);
    border-radius: 0.45rem;
}
[data-testid="stChatInput"] button svg {
    color: var(--app-accent);
    fill: var(--app-accent);
}
div[data-baseweb="popover"]:has([data-testid="stForm"]) {
    width: min(var(--refine-popover-width), calc(100vw - 2rem)) !important;
    max-width: min(var(--refine-popover-width), calc(100vw - 2rem)) !important;
    max-height: none !important;
    overflow: visible !important;
}
div[data-baseweb="popover"]:has([data-testid="stForm"]) > div,
div[data-baseweb="popover"]:has([data-testid="stForm"]) > div > div {
    width: 100% !important;
    max-width: 100% !important;
    max-height: none !important;
    overflow: visible !important;
    box-sizing: border-box;
}
div[data-baseweb="popover"]:has([data-testid="stForm"])
[data-testid="stForm"],
div[data-baseweb="popover"]:has([data-testid="stForm"])
[data-testid="stTextArea"],
div[data-baseweb="popover"]:has([data-testid="stForm"])
[data-testid="stTextArea"] > div {
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box;
}
div[data-baseweb="popover"]:has([data-testid="stForm"]) textarea {
    width: 100%;
    min-height: 4rem;
}
</style>
"""
