APP_UI_STYLES = """
<style>
:root {
    /* Main layout knobs. Increase offset to make the block narrower. */
    --text-block-side-offset: clamp(1rem, 24vw, 28rem);
    --text-block-width: min(
        100%,
        max(
            20rem,
            calc(100% - var(--text-block-side-offset) - var(--text-block-side-offset))
        )
    );
    --text-block-padding-x: 0rem;
    --search-input-height: 2.45rem;
    --search-input-button-size: 1.9rem;
    --refine-popover-width: 900px;
    --sidebar-width: 15rem;
}
#MainMenu,
footer,
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
.block-container,
.main .block-container {
    box-sizing: border-box;
    max-width: none !important;
    margin-left: auto !important;
    margin-right: auto !important;
    padding-left: var(--text-block-padding-x) !important;
    padding-right: var(--text-block-padding-x) !important;
    padding-top: 0.5rem !important;
    padding-bottom: 0 !important;
    width: var(--text-block-width) !important;
}
section[data-testid="stSidebar"] + div > div:first-child {
    padding-top: 0.5rem !important;
}
.st-key-start_screen {
    padding-top: 0;
}
.st-key-start_screen h1 {
    margin-bottom: 0.6rem;
    color: var(--app-text);
}
.st-key-start_screen .stButton > button {
    align-items: center;
    min-height: 60px;
    white-space: normal;
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
[class*="st-key-result_home_link"] button {
    border: 0 !important;
    background: transparent !important;
    color: var(--app-muted) !important;
    padding: 0.2rem 0 !important;
}
[class*="st-key-result_home_link"] button:hover {
    color: var(--app-accent) !important;
}
.st-key-response-actions {
    width: 100%;
    margin-top: 0.25rem;
    margin-bottom: 6.5rem;
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
.st-key-response-actions [data-testid="stPopover"] button {
    background: transparent !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
}
.st-key-response-actions [data-testid="stPopover"] button:hover {
    background: var(--app-accent-soft) !important;
    border-color: var(--app-accent) !important;
    color: var(--app-text) !important;
}
.st-key-response-actions [data-testid="stPopover"] button svg {
    color: var(--app-text) !important;
    fill: var(--app-text) !important;
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
[data-testid="stChatMessage"] {
    background: transparent;
    color: var(--app-text);
    margin: 0 !important;
    padding: 0.15rem 0 !important;
    position: relative;
}
[data-testid="stChatMessage"]:has([class*="st-key-question_card_"]) {
    margin-bottom: -0.35rem !important;
    padding-bottom: 0 !important;
}
[data-testid="stChatMessage"]:has([class*="st-key-answer_card_"]) {
    margin-top: 0 !important;
    padding-top: 0 !important;
}
[data-testid="stChatMessage"]:has([class*="st-key-answer_card_"]) > div:last-child,
[data-testid="stChatMessage"]:has([class*="st-key-answer_card_"])
[data-testid="stVerticalBlock"] {
    max-width: 100% !important;
    width: 100% !important;
}
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"],
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p,
[data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] li {
    color: var(--app-text);
}
@media (min-width: 900px) {
    [data-testid="stChatMessage"] > div:first-child {
        left: -3rem;
        position: absolute !important;
        top: 0.15rem;
        z-index: 1;
    }

    [data-testid="stChatMessage"] > div:last-child {
        margin-left: 0 !important;
        max-width: 100% !important;
        width: 100% !important;
    }
}
[class*="st-key-answer_card_"],
[class*="st-key-question_card_"] {
    background: var(--app-surface);
    border: 1px solid var(--app-border);
    border-radius: 0.5rem;
    box-shadow: 0 0.1rem 0.45rem rgba(15, 23, 42, 0.06);
    color: var(--app-text);
    margin: 0.1rem 0 0.45rem;
    padding: 0.85rem 0.95rem;
    word-break: break-word;
}
[class*="st-key-answer_card_"] {
    border-left: 3px solid var(--app-accent);
    box-sizing: border-box;
    width: 100%;
}
[class*="st-key-question_card_"] {
    align-items: center;
    background: var(--app-accent-soft);
    display: flex;
    margin-left: 0;
    margin-right: auto;
    max-width: min(760px, 100%);
    min-height: 2.6rem;
}
[class*="st-key-question_card_"] [data-testid="stMarkdownContainer"] {
    align-items: center;
    display: flex;
    min-height: 100%;
    width: 100%;
}
[class*="st-key-question_card_"] [data-testid="stMarkdownContainer"] p {
    line-height: 1.35;
    margin: 0 !important;
}
[class*="st-key-answer_card_"] [data-testid="stMarkdownContainer"] p:first-child,
[class*="st-key-question_card_"] [data-testid="stMarkdownContainer"] p:first-child {
    margin-top: 0;
}
[class*="st-key-answer_card_"] [data-testid="stMarkdownContainer"] p:last-child,
[class*="st-key-question_card_"] [data-testid="stMarkdownContainer"] p:last-child {
    margin-bottom: 0;
}
[class*="st-key-answer_card_"] .katex {
    font-size: 1em;
    line-height: 1;
    vertical-align: baseline;
}
[class*="st-key-answer_card_"] .katex-display {
    margin: 0.6rem 0;
    overflow-x: auto;
    text-align: left;
}
[class*="st-key-answer_card_"] table {
    border: 1px solid var(--app-border);
    border-collapse: collapse;
    color: var(--app-text);
    max-width: 100%;
    table-layout: auto;
    width: 100%;
}
[class*="st-key-answer_card_"] [data-testid="stMarkdownContainer"]:has(table) {
    overflow-x: auto;
}
[class*="st-key-answer_card_"] thead tr {
    background: var(--app-accent-soft);
}
[class*="st-key-answer_card_"] th,
[class*="st-key-answer_card_"] td {
    border: 1px solid var(--app-border);
    color: var(--app-text);
    padding: 0.45rem 0.6rem;
}
[class*="st-key-answer_card_"] tbody tr:nth-child(even) {
    background: var(--app-bg);
}
[class*="st-key-answer_card_"] hr {
    border: 0;
    border-top: 1px solid var(--app-border);
    margin: 0.95rem 0 0.75rem;
}
[class*="st-key-answer_card_"] blockquote {
    background: rgba(240, 165, 0, 0.12);
    border-left: 3px solid #f0a500;
    color: var(--app-text);
    margin: 0.6rem 0;
    padding: 0.65rem 0.8rem;
}
[class*="st-key-answer_card_"] blockquote p {
    color: var(--app-text) !important;
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
[class*="st-key-sources_panel_"] [data-testid="stExpander"] {
    background: var(--app-surface);
    border: 1px solid var(--app-border);
    border-radius: 0.5rem;
    color: var(--app-text);
}
[class*="st-key-sources_panel_"] [data-testid="stExpander"] details,
[class*="st-key-sources_panel_"] [data-testid="stExpander"] summary {
    background: var(--app-surface) !important;
    color: var(--app-text) !important;
}
[class*="st-key-sources_panel_"] [data-testid="stExpander"] summary:hover {
    background: var(--app-accent-soft) !important;
}
[class*="st-key-sources_panel_"] [data-testid="stExpander"] summary p,
[class*="st-key-sources_panel_"] [data-testid="stExpander"] summary span,
[class*="st-key-sources_panel_"] [data-testid="stExpander"] summary svg {
    color: var(--app-text) !important;
    fill: var(--app-text) !important;
}
[class*="st-key-sources_panel_"] [data-testid="stCaptionContainer"] {
    color: var(--app-muted);
}
[class*="st-key-sources_panel_"] [data-testid="stDivider"] hr {
    border-color: var(--app-border);
}
[class*="st-key-sources_panel_"] [data-testid="stDataFrame"],
[class*="st-key-sources_panel_"] table {
    width: 100% !important;
}
[class*="st-key-sources_panel_"] [data-testid="stCodeBlock"],
[class*="st-key-sources_panel_"] pre,
[class*="st-key-sources_panel_"] code {
    background: var(--app-input-bg) !important;
    border-color: var(--app-border) !important;
    color: var(--app-text) !important;
    line-height: 1.45;
    overflow-wrap: anywhere;
    white-space: pre-wrap !important;
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
    white-space: nowrap !important;
}
[data-testid="stSidebar"] [class*="st-key-new_search"] button {
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
    justify-content: center !important;
    white-space: nowrap !important;
}
[data-testid="stSidebar"] [class*="st-key-new_search"] button:hover {
    background: var(--app-accent-soft) !important;
    border-color: var(--app-accent) !important;
}
[data-testid="stSidebar"] [class*="st-key-new_search"] button p {
    white-space: nowrap !important;
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
    position: relative;
}
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"] {
    background: rgba(200, 168, 90, 0.16);
    border-left: 3px solid #c8a85a;
    border-radius: 0.45rem;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[data-testid="stHorizontalBlock"],
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[data-testid="stHorizontalBlock"] {
    gap: 0.1rem;
    align-items: center;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button {
    align-items: center !important;
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    justify-content: flex-start !important;
    min-height: 2.15rem !important;
    padding: 0.18rem 0.35rem !important;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button:hover {
    background: var(--app-accent-soft) !important;
}
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
.stButton:first-child > button {
    background: var(--app-accent-soft) !important;
    color: var(--app-text) !important;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button,
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button p,
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button span,
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button svg,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button p,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button span,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button svg {
    color: var(--app-text) !important;
    fill: var(--app-text) !important;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button p,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button p {
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 2;
    background: transparent !important;
    display: -webkit-box;
    font-size: 0.72rem;
    line-height: 1.18;
    max-width: 100%;
    overflow: hidden;
    text-align: left;
    white-space: normal;
    word-break: break-word;
}
[data-testid="stSidebar"] [class*="st-key-history_item_"]
[class*="st-key-history_"] button *,
[data-testid="stSidebar"] [class*="st-key-history_item_selected_"]
[class*="st-key-history_"] button * {
    background: transparent !important;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] {
    opacity: 0;
    pointer-events: none;
    position: absolute;
    right: 0.2rem;
    top: 50%;
    transform: translateY(-50%);
    transition: opacity 120ms ease;
    z-index: 10;
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
    align-items: center !important;
    background: transparent !important;
    border: 0 !important;
    box-shadow: none !important;
    color: var(--app-muted) !important;
    height: 1.45rem !important;
    justify-content: center !important;
    min-height: 1.45rem !important;
    min-width: 1.45rem !important;
    padding: 0 !important;
    width: 1.45rem !important;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button p {
    display: block !important;
    font-size: 0.9rem;
    line-height: 1;
    margin: 0 !important;
    padding: 0 !important;
    text-align: center;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button svg {
    display: none !important;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button > :not(:has(p)) {
    display: none !important;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button
:not([data-testid="stMarkdownContainer"]):not(p) {
    background: transparent !important;
}
[data-testid="stSidebar"] [class*="st-key-chat_menu_"] button::after {
    display: none !important;
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
[data-testid="stSidebar"] [class*="st-key-auth_panel"] button {
    align-items: center !important;
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
    justify-content: center !important;
}
[data-testid="stSidebar"] [class*="st-key-auth_panel"] button:hover {
    background: var(--app-accent-soft) !important;
    border-color: var(--app-accent) !important;
}
[data-testid="stSidebar"] [class*="st-key-auth_panel"] button svg,
[data-testid="stSidebar"] [class*="st-key-auth_panel"] button p {
    color: var(--app-text) !important;
    fill: var(--app-text) !important;
}
[data-testid="stChatInput"] {
    box-sizing: border-box;
    max-width: none !important;
    min-height: var(--search-input-height) !important;
    width: 100% !important;
    background: var(--app-input-bg);
    border: 1px solid var(--app-border);
    border-radius: 0.6rem;
    box-shadow: none;
}
[data-testid="stBottom"] {
    box-sizing: border-box;
    left: 0 !important;
    max-width: none !important;
    right: 0 !important;
    transform: none !important;
    width: 100% !important;
}
[data-testid="stBottomBlockContainer"] {
    box-sizing: border-box;
    margin-left: auto !important;
    margin-right: auto !important;
    max-width: none !important;
    padding-left: var(--text-block-padding-x) !important;
    padding-right: var(--text-block-padding-x) !important;
    width: var(--text-block-width) !important;
}
[data-testid="stBottom"] > div,
div[data-testid="stChatInputContainer"],
section.main > div > div:last-child {
    box-sizing: border-box;
    max-width: 100% !important;
    margin-left: 0 !important;
    margin-right: 0 !important;
    width: 100% !important;
}
[data-testid="stChatInput"] > div {
    background: transparent;
    min-height: var(--search-input-height) !important;
}
[data-testid="stChatInput"] textarea,
[data-testid="stChatInput"] textarea:focus {
    background: var(--app-input-bg);
    color: var(--app-text);
    caret-color: var(--app-accent);
    min-height: var(--search-input-height) !important;
    overflow: hidden;
    padding-bottom: 0.35rem !important;
    padding-top: 0.35rem !important;
    text-overflow: ellipsis;
}
[data-testid="stChatInput"] textarea::placeholder,
[data-testid="stTextInput"] input::placeholder {
    color: var(--app-muted);
}
[data-testid="stChatInput"] button {
    background: var(--app-accent-soft);
    color: var(--app-accent);
    border-radius: 0.45rem;
    height: var(--search-input-button-size) !important;
    min-height: var(--search-input-button-size) !important;
    min-width: var(--search-input-button-size) !important;
    width: var(--search-input-button-size) !important;
}
[data-testid="stChatInput"] button svg {
    color: var(--app-accent);
    fill: var(--app-accent);
}
div[data-baseweb="popover"]:has([data-testid="stForm"]) {
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
    width: min(var(--refine-popover-width), calc(100vw - 2rem)) !important;
    max-width: min(var(--refine-popover-width), calc(100vw - 2rem)) !important;
    max-height: none !important;
    overflow: visible !important;
}
div[data-baseweb="popover"]:has([data-testid="stForm"]) > div,
div[data-baseweb="popover"]:has([data-testid="stForm"]) > div > div {
    background: var(--app-surface) !important;
    color: var(--app-text) !important;
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
    background: var(--app-input-bg) !important;
    border-color: var(--app-border) !important;
    color: var(--app-text) !important;
    caret-color: var(--app-accent);
    width: 100%;
    min-height: 4rem;
}
div[data-baseweb="popover"]:has([data-testid="stForm"]) textarea::placeholder {
    color: var(--app-muted);
}
div[data-baseweb="popover"]:has([data-testid="stForm"])
[data-testid="stForm"],
div[data-baseweb="popover"]:has([data-testid="stForm"])
[data-testid="stForm"] label,
div[data-baseweb="popover"]:has([data-testid="stForm"])
[data-testid="stForm"] p {
    color: var(--app-text);
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) {
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) > div,
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) > div > div {
    background: var(--app-surface) !important;
    color: var(--app-text) !important;
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) label,
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) p,
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) span {
    color: var(--app-text) !important;
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"])
[data-testid="stCaptionContainer"] {
    color: var(--app-muted) !important;
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"])
[data-baseweb="select"] > div {
    background: var(--app-input-bg) !important;
    border-color: var(--app-border) !important;
    color: var(--app-text) !important;
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) button {
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
}
div[data-baseweb="popover"]:has([class*="st-key-auth_panel_content"]) button:hover {
    background: var(--app-accent-soft) !important;
    border-color: var(--app-accent) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"]),
div[role="dialog"]:has([class*="st-key-auth_dialog"]) > div {
    background: var(--app-surface) !important;
    border-color: var(--app-border) !important;
    color: var(--app-text) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"]) h1,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) h2,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) h3,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) label,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) p,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) span {
    color: var(--app-text) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stTextInput"] input {
    background: var(--app-input-bg) !important;
    border-color: var(--app-border) !important;
    color: var(--app-text) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stTextInput"] input::placeholder {
    color: var(--app-muted) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stDivider"] hr {
    border-color: var(--app-border) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stExpander"] {
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stExpander"] details,
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stExpander"] summary {
    background: var(--app-surface) !important;
    color: var(--app-text) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stExpander"] summary:hover {
    background: var(--app-accent-soft) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stExpander"] svg {
    color: var(--app-text) !important;
    fill: var(--app-text) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stFormSubmitButton"] button,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) .stButton > button {
    background: var(--app-surface) !important;
    border: 1px solid var(--app-border) !important;
    color: var(--app-text) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stFormSubmitButton"] button:hover,
div[role="dialog"]:has([class*="st-key-auth_dialog"]) .stButton > button:hover {
    background: var(--app-accent-soft) !important;
    border-color: var(--app-accent) !important;
}
div[role="dialog"]:has([class*="st-key-auth_dialog"])
[data-testid="stFormSubmitButton"] button[kind="primary"] {
    background: var(--app-accent) !important;
    border-color: var(--app-accent) !important;
    color: var(--app-primary-text) !important;
}
</style>
"""
