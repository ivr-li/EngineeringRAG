from dataclasses import dataclass


DEFAULT_THEME_KEY = "light"


@dataclass(frozen=True)
class AppTheme:
    label: str
    background: str
    surface: str
    sidebar: str
    border: str
    text: str
    muted: str
    accent: str
    accent_soft: str
    primary_text: str
    input_bg: str


THEMES = {
    "light": AppTheme(
        label="Белая",
        background="#ffffff",
        surface="#f7f9fc",
        sidebar="#f1f4f8",
        border="#d8dee8",
        text="#2d3340",
        muted="#6f7887",
        accent="#2f80ed",
        accent_soft="#e7f1ff",
        primary_text="#ffffff",
        input_bg="#f2f5f9",
    ),
    "dark": AppTheme(
        label="Темная",
        background="#111318",
        surface="#1b2029",
        sidebar="#171b22",
        border="#303846",
        text="#eef2f7",
        muted="#9aa5b5",
        accent="#7db7ff",
        accent_soft="#21364f",
        primary_text="#0d1117",
        input_bg="#202631",
    ),
    "engineering": AppTheme(
        label="Инженерная",
        background="#f8fafb",
        surface="#eef4f7",
        sidebar="#e8eef2",
        border="#cfd9df",
        text="#26323b",
        muted="#687782",
        accent="#167c80",
        accent_soft="#dff2f2",
        primary_text="#ffffff",
        input_bg="#edf3f5",
    ),
    "graphite": AppTheme(
        label="Графит",
        background="#191a1d",
        surface="#24262b",
        sidebar="#202226",
        border="#3b3f46",
        text="#f1f0eb",
        muted="#aaa79e",
        accent="#d5a84c",
        accent_soft="#433821",
        primary_text="#1c1710",
        input_bg="#2b2e34",
    ),
    "green": AppTheme(
        label="Зеленая",
        background="#fbfcf9",
        surface="#edf4ec",
        sidebar="#e7efe5",
        border="#cfdcc9",
        text="#263326",
        muted="#6c7b66",
        accent="#3f7d46",
        accent_soft="#e1f0df",
        primary_text="#ffffff",
        input_bg="#f0f5ef",
    ),
}
