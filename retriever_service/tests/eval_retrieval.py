"""
Запуск:
    python eval_retrieval.py                        # все тесты
    python eval_retrieval.py --mode hybrid          # только hybrid
    python eval_retrieval.py --top-k 5              # top-k override

Выходные файлы:
    eval_results_<timestamp>.json   — полный лог всех тестов
    eval_report_<timestamp>.html    — визуальный отчёт
"""

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

import structlog
from openai import OpenAI
from retriever.retriever import QdrantRetriever

log = structlog.get_logger(__name__)

# ====================================================================
# Тест-кейсы
# ====================================================================

SearchMode = Literal["hybrid", "dense", "sparse"]


@dataclass
class TestCase:
    """Один тест-кейс для retrieval."""

    id: str
    query: str
    expected_filename: str  # подстрока в filename (не точное совпадение)
    expected_keywords: list[str]  # хотя бы одно слово должно быть в тексте топ-3
    mode: SearchMode = "hybrid"
    category: str = "general"  # priming: direct / table / chain / absent
    top_k: int = 10
    min_score: float = 8.0  # минимальный ожидаемый score топ-1


TEST_CASES: list[TestCase] = [
    # ====================================================================
    # Блок 1 — Прямой точный ответ
    # ====================================================================
    TestCase(
        id="direct_01",
        query="Минимальная ширина эвакуационного выхода",
        expected_filename="СП 1.13130",
        expected_keywords=["ширина", "выход", "эвакуационн"],
        category="direct",
        min_score=5.0,
    ),
    TestCase(
        id="direct_02",
        query="Расстояние от края проезда до стены здания для пожарных автомобилей",
        expected_filename="СП 4.13130",
        expected_keywords=["проезд", "пожарн", "расстояние"],
        category="direct",
        min_score=9.0,
    ),
    TestCase(
        id="direct_03",
        query="Нормируемое сопротивление теплопередаче наружных стен",
        expected_filename="СП 50.13330",
        expected_keywords=["сопротивление", "теплопередача", "стен"],
        category="direct",
        min_score=9.0,
    ),
    TestCase(
        id="direct_04",
        query="Предел огнестойкости несущих конструкций REI",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкость", "несущ"],
        category="direct",
        min_score=9.0,
    ),
    TestCase(
        id="direct_05",
        query="Инсоляция жилых помещений в многоквартирных домах продолжительность часов",
        expected_filename="СП 54.13330",
        expected_keywords=["инсоляция", "жилых", "час"],
        category="direct",
        min_score=7.0,
    ),
    # ====================================================================
    # Блок 2 — Ответ в таблице (тест Sliding Window)
    # ====================================================================
    TestCase(
        id="table_01",
        query="Минимальная толщина защитного слоя бетона для арматуры в перекрытии",
        expected_filename="СП 63.13330",
        expected_keywords=["защитный слой", "бетон", "арматура"],
        category="table",
        min_score=8.0,
    ),
    TestCase(
        id="table_02",
        query="Допустимые прогибы балок перекрытий при пролёте 6 метров",
        expected_filename="СП 20.13330",
        expected_keywords=["прогиб", "балк", "пролёт"],
        category="table",
        min_score=8.0,
    ),
    TestCase(
        id="table_03",
        query="Классификация зданий по функциональной пожарной опасности Ф1 Ф2 Ф3",
        expected_filename="123-ФЗ",
        expected_keywords=["Ф1", "Ф2", "функциональн", "пожарн"],
        category="table",
        min_score=8.0,
    ),
    TestCase(
        id="table_04",
        query="Нормативная снеговая нагрузка для III снегового района кПа",
        expected_filename="СП 20.13330",
        expected_keywords=["снеговая", "нагрузка", "район"],
        category="table",
        min_score=8.0,
    ),
    # ====================================================================
    # Блок 3 — Межсекционная цепочка (тест Ref Expansion)
    # ====================================================================
    TestCase(
        id="chain_01",
        query="Требования к автоматической пожарной сигнализации в жилых многоквартирных домах",
        expected_filename="СП 54.13330",
        expected_keywords=["сигнализация", "пожарн", "многоквартирн"],
        category="chain",
        min_score=8.0,
    ),
    TestCase(
        id="chain_02",
        query="Молниезащита зданий с металлической кровлей категория",
        expected_filename="РД 34",
        expected_keywords=["молниезащита", "кровл", "категори"],
        category="chain",
        min_score=7.0,
    ),
    TestCase(
        id="chain_03",
        query="Расстояния от стоянок автомобилей до жилых зданий норматив",
        expected_filename="СП 113.13330",
        expected_keywords=["стоянк", "жилых", "расстояние"],
        category="chain",
        min_score=8.0,
    ),
    TestCase(
        id="chain_04",
        query="Системы противодымной защиты вентиляция при пожаре",
        expected_filename="СП 7.13130",
        expected_keywords=["дымоудаление", "противодымн", "пожар"],
        category="chain",
        min_score=8.0,
    ),
    # ====================================================================
    # Блок 4 — Документа нет в базе
    # ====================================================================
    TestCase(
        id="absent_01",
        query="При какой этажности здания требуется устройство автоматической пожарной сигнализации",
        expected_filename="СП 486",
        expected_keywords=["этажност", "486"],
        category="absent",
        min_score=99.0,
    ),
    TestCase(
        id="absent_02",
        query="Требования к сейсмостойкости зданий в зоне 8 баллов",
        expected_filename="СП 14",
        expected_keywords=["сейсм", "балл"],
        category="absent",
        min_score=99.0,
    ),
    TestCase(
        id="absent_03",
        query="Нормы освещённости для офисных помещений люкс",
        expected_filename="СП 52.13330",
        expected_keywords=["освещённость", "офис", "лк"],
        category="absent",
        min_score=99.0,
    ),
    # ====================================================================
    # Блок 5 — Режимный тест
    # ====================================================================
    TestCase(
        id="mode_hybrid",
        query="таблица классов конструктивной пожарной опасности REI огнестойкость несущих конструкций",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкост"],
        mode="hybrid",
        category="mode_compare",
        min_score=9.0,
    ),
    TestCase(
        id="mode_dense",
        query="таблица классов конструктивной пожарной опасности REI огнестойкость несущих конструкций",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкост"],
        mode="dense",
        category="mode_compare",
        min_score=0.55,
    ),
    TestCase(
        id="mode_sparse",
        query="таблица классов конструктивной пожарной опасности REI огнестойкость несущих конструкций",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкост"],
        mode="sparse",
        category="mode_compare",
        min_score=3.0,
    ),
]


@dataclass
class TestResult:
    case_id: str
    mode: str
    query: str
    effective_query: str  # после rewrite или равно query если нет rewrite
    was_rewritten: bool
    category: str
    expected_filename: str
    top1_filename: str
    top1_score: float
    top1_text_snippet: str
    top3_filenames: list[str]
    filename_hit: bool  # expected_filename в топ-3
    keyword_hit: bool  # хотя бы одно ключевое слово в тексте топ-3
    score_ok: bool  # top1_score >= min_score
    passed: bool  # filename_hit AND keyword_hit
    latency_ms: float
    rewrite_latency_ms: float
    all_scores: list[float]
    error: str | None = None


class QueryRewriter:
    """
    Переформулирует запрос через vllm-light.
    При недоступности модели возвращает оригинальный запрос.
    """

    REWRITE_SYSTEM_PROMPT = """
    Ты — ассистент по переформулированию поисковых запросов для системы RAG.
    Твоя задача: преобразовать вопрос пользователя в точный поисковый запрос,
    пригодный для векторного поиска по нормативным документам (СП, ГОСТ, СНиП).

    Правила:
    - Верни ТОЛЬКО переформулированный запрос, без пояснений и кавычек.
    - Убери разговорные обороты («расскажи мне», «хочу узнать» и т.п.).
    - Сохрани все технические термины, номера стандартов, классы материалов.
    - Добавь релевантные синонимы и уточнения области применения, если очевидны.
    - Длина ответа — не более двух предложений.
    """
    REWRITE_SYSTEM_PROMPT_V2 = (
        "Ты - помощник для улучшения поисковых запросов к базе строительных нормативов (СП, ГОСТ, ФЗ). "
        "Перефразируй запрос так, чтобы он лучше соответствовал терминологии нормативных документов. "
        "Используй профессиональные термины. Отвечай только перефразированным запросом, без пояснений."
    )
    REWRITER_BASE_URL = "http://vllm-light:8020/v1"
    REWRITER_MODEL = "query-rewriter"

    def __init__(self, timeout: float = 5.0) -> None:
        self.client = OpenAI(
            base_url=self.REWRITER_BASE_URL,
            api_key="",
            timeout=timeout,
        )

    def rewrite(self, query: str) -> tuple[str, bool]:
        try:
            resp = self.client.chat.completions.create(
                model=self.REWRITER_MODEL,
                messages=[
                    {"role": "system", "content": self.REWRITE_SYSTEM_PROMPT},
                    {"role": "user", "content": query},
                ],
                temperature=0.2,
                max_tokens=256,
            )
            rewritten = (resp.choices[0].message.content or "").strip()
            if not rewritten:
                return query, False
            return rewritten, True
        except Exception as e:
            log.error("rewriter_error", query=query, error=str(e))
            return query, False


class RetrievalEvaluator:
    def __init__(
        self,
        top_k_override: int | None = None,
        mode_override: SearchMode | None = None,
        use_rewriter: bool = False,
    ) -> None:

        self.retriever = QdrantRetriever()
        self.top_k_override = top_k_override
        self.mode_override = mode_override
        self.rewriter = QueryRewriter() if use_rewriter else None

    def _maybe_rewrite(self, query: str) -> tuple[str, bool, float]:
        if self.rewriter is None:
            return query, False, 0.0

        t0 = time.perf_counter()
        rewritten, ok = self.rewriter.rewrite(query)
        return rewritten, ok, (time.perf_counter() - t0) * 1000

    def run_case(self, case: TestCase) -> TestResult:
        mode = self.mode_override or case.mode
        top_k = self.top_k_override or case.top_k

        effective_query, was_rewritten, rw_ms = self._maybe_rewrite(case.query)

        t0 = time.perf_counter()
        error = None
        results = []
        try:
            results = self.retriever.search(
                query=effective_query,
                top_k=top_k,
                prefetch_k=top_k * 4,
                mode=mode,
            )
        except Exception as exc:
            error = str(exc)
        latency_ms = (time.perf_counter() - t0) * 1000

        top3 = results[:3]
        top1 = results[0] if results else None
        top1_filename = top1.filename if top1 else ""
        top1_score = top1.score if top1 else 0.0
        top1_snippet = (top1.text[:200] + "…") if top1 and top1.text else ""
        top3_filenames = [r.filename for r in top3]
        all_scores = [r.score for r in results]

        filename_hit = any(case.expected_filename.lower() in fn.lower() for fn in top3_filenames)
        keyword_hit = any(
            kw.lower() in (r.text or "").lower() for r in top3 for kw in case.expected_keywords
        )
        score_ok = top1_score >= case.min_score
        passed = (not filename_hit) if case.category == "absent" else (filename_hit and keyword_hit)

        return TestResult(
            case_id=case.id,
            query=case.query,
            effective_query=effective_query,
            was_rewritten=was_rewritten,
            mode=mode,
            category=case.category,
            expected_filename=case.expected_filename,
            top1_filename=top1_filename,
            top1_score=top1_score,
            top1_text_snippet=top1_snippet,
            top3_filenames=top3_filenames,
            filename_hit=filename_hit,
            keyword_hit=keyword_hit,
            score_ok=score_ok,
            passed=passed,
            latency_ms=latency_ms,
            rewrite_latency_ms=rw_ms,
            all_scores=all_scores,
            error=error,
        )

    def run_all(self, cases: list[TestCase] | None = None) -> list[TestResult]:
        cases = cases or TEST_CASES
        results = []
        total = len(cases)
        for i, case in enumerate(cases, 1):
            print(
                f"  [{i:2d}/{total}] {case.id:<20} mode={case.mode:<7} ",
                end="",
                flush=True,
            )
            r = self.run_case(case)
            status = "✅ PASS" if r.passed else "❌ FAIL"
            rw_tag = " [✏]" if r.was_rewritten else ""
            print(
                f"{status}  score={r.top1_score:7.4f}  {r.latency_ms:5.0f}ms{rw_tag}  → {r.top1_filename[:40]}"
            )
            results.append(r)
        return results


def _summary(results: list[TestResult]) -> dict:
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    by_cat: dict[str, dict] = {}
    for r in results:
        c = by_cat.setdefault(
            r.category, {"total": 0, "passed": 0, "avg_score": [], "avg_latency": []}
        )
        c["total"] += 1
        c["passed"] += int(r.passed)
        c["avg_score"].append(r.top1_score)
        c["avg_latency"].append(r.latency_ms)
    for c in by_cat.values():
        c["pass_rate"] = round(c["passed"] / c["total"], 3) if c["total"] else 0
        c["avg_score"] = (
            round(sum(c["avg_score"]) / len(c["avg_score"]), 4) if c["avg_score"] else 0
        )
        c["avg_latency_ms"] = (
            round(sum(c["avg_latency"]) / len(c["avg_latency"]), 1) if c["avg_latency"] else 0
        )
    rewritten_count = sum(1 for r in results if r.was_rewritten)
    avg_rw_ms = (
        round(
            sum(r.rewrite_latency_ms for r in results if r.was_rewritten) / rewritten_count,
            1,
        )
        if rewritten_count
        else 0
    )
    return {
        "generated_at": datetime.now().isoformat(),
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": round(passed / total, 3) if total else 0,
        "avg_latency_ms": round(sum(r.latency_ms for r in results) / total, 1) if total else 0,
        "rewriter_used": rewritten_count > 0,
        "rewritten_count": rewritten_count,
        "avg_rewrite_latency_ms": avg_rw_ms,
        "by_category": by_cat,
    }


def save_json(results: list[TestResult], path: Path) -> None:
    data = {
        "generated_at": datetime.now().isoformat(),
        "summary": _summary(results),
        "results": [asdict(r) for r in results],
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n📄 JSON-лог сохранён: {path}")


def _esc(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def save_html(results: list[TestResult], path: Path) -> None:
    summary = _summary(results)
    rows = []
    for r in results:
        score_cls = "score-ok" if r.score_ok else "score-low"
        top3_str = "<br>".join(_esc(f) for f in r.top3_filenames) or "—"
        error_str = f'<span class="error">⚠ {_esc(r.error)}</span>' if r.error else ""
        rw_str = f'<span class="rw">✏ {_esc(r.effective_query)}</span>' if r.was_rewritten else ""
        rows.append(f"""<tr class="{"pass" if r.passed else "fail"}">
  <td><code>{r.case_id}</code></td>
  <td>{_esc(r.query)}{rw_str}</td>
  <td><span class="badge badge-{r.category}">{r.category}</span></td>
  <td>{r.mode}</td>
  <td class="{score_cls}">{r.top1_score:.4f}</td>
  <td class="snip">{_esc(r.top1_text_snippet)}{error_str}</td>
  <td class="fns">{top3_str}</td>
  <td>{"🟢" if r.filename_hit else "🔴"}</td>
  <td>{"🟢" if r.keyword_hit else "🔴"}</td>
  <td>{r.latency_ms:.0f}ms</td>
  <td>{"✅" if r.passed else "❌"}</td>
</tr>""")

    cat_labels = list(summary["by_category"].keys())
    cat_pass = [summary["by_category"][c]["pass_rate"] * 100 for c in cat_labels]
    cat_scores = [summary["by_category"][c]["avg_score"] for c in cat_labels]

    rw_indicator = (
        '<span style="background:#1d4ed8;color:#fff;padding:2px 10px;border-radius:20px;font-size:.8rem">✏ rewriter ON</span>'
        if summary["rewriter_used"]
        else '<span style="background:#f3f4f6;color:#6b7280;padding:2px 10px;border-radius:20px;font-size:.8rem">rewriter OFF</span>'
    )

    html = f"""<!DOCTYPE html>
<html lang="ru"><head><meta charset="utf-8"><title>Retrieval Eval</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:#f9fafb;color:#111827;font-size:14px}}
header{{background:#fff;border-bottom:1px solid #e5e7eb;padding:14px 24px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}}
header h1{{font-size:1.2rem;font-weight:700;flex:1}}
.metrics{{display:flex;gap:10px;padding:16px 24px;flex-wrap:wrap}}
.mc{{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px 16px;min-width:110px}}
.mc .lb{{color:#6b7280;font-size:.7rem;text-transform:uppercase;letter-spacing:.04em}}
.mc .vl{{font-size:1.5rem;font-weight:700;margin-top:2px}}
.mc.green .vl{{color:#16a34a}}.mc.red .vl{{color:#dc2626}}.mc.blue .vl{{color:#2563eb}}.mc.amber .vl{{color:#d97706}}
.charts{{display:flex;gap:16px;padding:0 24px 16px;flex-wrap:wrap}}
.cb{{background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:14px;flex:1;min-width:240px;max-width:420px}}
.cb h3{{font-size:.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:.04em;margin-bottom:8px}}
.tw{{padding:0 24px 32px;overflow-x:auto}}
table{{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,.07)}}
th{{background:#f3f4f6;color:#4b5563;font-size:.69rem;text-transform:uppercase;letter-spacing:.04em;padding:7px 9px;text-align:left;white-space:nowrap}}
td{{padding:7px 9px;border-bottom:1px solid #f3f4f6;vertical-align:top}}
tr.fail td{{background:#fef2f2}}tr:hover td{{background:#fafafa}}
.snip{{max-width:180px;font-size:.73rem;color:#6b7280}}.fns{{max-width:160px;font-size:.7rem;color:#6b7280}}
code{{background:#f3f4f6;padding:2px 4px;border-radius:3px;font-size:.75rem}}
.score-ok{{color:#16a34a;font-weight:600}}.score-low{{color:#dc2626;font-weight:600}}
.error{{color:#dc2626}}.rw{{display:block;font-size:.72rem;color:#1d4ed8;margin-top:2px}}
.badge{{padding:2px 6px;border-radius:20px;font-size:.65rem;font-weight:600}}
.badge-direct{{background:#dbeafe;color:#1e40af}}.badge-table{{background:#dcfce7;color:#166534}}
.badge-chain{{background:#fef9c3;color:#854d0e}}.badge-absent{{background:#fee2e2;color:#991b1b}}
.badge-mode_compare{{background:#f3e8ff;color:#6b21a8}}
</style></head><body>
<header>
  <h1>📐 Retrieval Eval</h1>{rw_indicator}
  <span style="color:#6b7280;font-size:.8rem">{datetime.now().strftime("%Y-%m-%d %H:%M")}</span>
</header>
<div class="metrics">
  <div class="mc {"green" if summary["pass_rate"] >= 0.8 else "red"}">
    <div class="lb">Pass Rate</div><div class="vl">{summary["pass_rate"] * 100:.0f}%</div></div>
  <div class="mc green"><div class="lb">Passed</div><div class="vl">{summary["passed"]}</div></div>
  <div class="mc red"><div class="lb">Failed</div><div class="vl">{summary["failed"]}</div></div>
  <div class="mc blue"><div class="lb">Total</div><div class="vl">{summary["total"]}</div></div>
  <div class="mc amber"><div class="lb">Avg Latency</div><div class="vl">{summary["avg_latency_ms"]:.0f}ms</div></div>
  {'<div class="mc blue"><div class="lb">Avg Rewrite</div><div class="vl">' + str(summary["avg_rewrite_latency_ms"]) + "ms</div></div>" if summary["rewriter_used"] else ""}
</div>
<div class="charts">
  <div class="cb"><h3>Pass Rate по категориям</h3><canvas id="cPass" height="180"></canvas></div>
  <div class="cb"><h3>Средний Score топ-1</h3><canvas id="cScore" height="180"></canvas></div>
</div>
<div class="tw"><table>
<thead><tr>
  <th>ID</th><th>Запрос</th><th>Кат.</th><th>Mode</th>
  <th>Score</th><th>Топ-1 текст</th><th>Топ-3 файлы</th>
  <th>Файл</th><th>KW</th><th>Lat.</th><th>✓</th>
</tr></thead>
<tbody>{"".join(rows)}</tbody>
</table></div>
<script>
const pal=['#2563eb','#16a34a','#d97706','#dc2626','#7c3aed'];
new Chart(document.getElementById('cPass'),{{type:'bar',
  data:{{labels:{json.dumps(cat_labels, ensure_ascii=False)},datasets:[{{data:{json.dumps(cat_pass)},backgroundColor:pal,borderRadius:5}}]}},
  options:{{plugins:{{legend:{{display:false}}}},scales:{{
    y:{{min:0,max:110,ticks:{{color:'#6b7280',callback:v=>v+'%'}},grid:{{color:'#f3f4f6'}}}},
    x:{{ticks:{{color:'#374151'}},grid:{{display:false}}}}}}}}
}});
new Chart(document.getElementById('cScore'),{{type:'bar',
  data:{{labels:{json.dumps(cat_labels, ensure_ascii=False)},datasets:[{{data:{json.dumps(cat_scores)},backgroundColor:pal,borderRadius:5}}]}},
  options:{{plugins:{{legend:{{display:false}}}},scales:{{
    y:{{ticks:{{color:'#6b7280'}},grid:{{color:'#f3f4f6'}}}},
    x:{{ticks:{{color:'#374151'}},grid:{{display:false}}}}}}}}
}});
</script></body></html>"""

    path.write_text(html, encoding="utf-8")
    print(f"🌐 HTML-отчёт: {path}")


def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Retrieval evaluator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
                Примеры:
                python eval_retrieval.py                              # все тесты, без rewriter
                python eval_retrieval.py --rewrite                    # все тесты, с rewriter
                python eval_retrieval.py --mode sparse                # override mode
                python eval_retrieval.py --category direct            # только direct
                python eval_retrieval.py --category direct --rewrite  # direct + rewriter
                pytest eval_retrieval.py -v                           # pytest без rewriter
                pytest eval_retrieval.py -v --rewrite                 # pytest с rewriter
                """,
    )
    parser.add_argument("--mode", choices=["hybrid", "dense", "sparse"], default=None)
    parser.add_argument("--top-k", type=int, default=None, dest="top_k")
    parser.add_argument("--category", default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument(
        "--rewrite",
        action="store_true",
        default=False,
        help="Использовать QueryRewriter (по умолчанию ВЫКЛЮЧЕН)",
    )
    args = parser.parse_args()

    cases = TEST_CASES
    if args.category:
        cases = [c for c in cases if c.category == args.category]
        if not cases:
            print(f"Нет тест-кейсов для категории '{args.category}'")
            sys.exit(1)

    ts = datetime.now().strftime("%d%m%Y_%H%M")
    json_path = Path(f"results_{ts}.json")
    html_path = Path(f"results_uot_{ts}.html")

    cases = TEST_CASES
    if args.category:
        cases = [c for c in cases if c.category == args.category]
        if not cases:
            print(f"Нет тест-кейсов для категории '{args.category}'")
            sys.exit(1)

    print(
        f"\n🔍 {len(cases)} тестов  |  mode={args.mode or 'per-case'}  |  rewriter={'ON' if args.rewrite else 'OFF'}\n"
    )

    evaluator = RetrievalEvaluator(
        top_k_override=args.top_k,
        mode_override=args.mode,
        use_rewriter=args.rewrite,
    )
    results = evaluator.run_all(cases)
    summary = _summary(results)

    print(f"\n{'─' * 60}")
    print(
        f"  Итого: {summary['passed']}/{summary['total']} ({summary['pass_rate'] * 100:.0f}%)"
        f"  |  avg latency: {summary['avg_latency_ms']:.0f}ms"
    )
    if summary["rewriter_used"]:
        print(f"  Avg rewrite latency: {summary['avg_rewrite_latency_ms']:.0f}ms")
    for cat, s in summary["by_category"].items():
        bar = "█" * int(s["pass_rate"] * 20)
        print(f"  {cat:<14} {bar:<20} {s['pass_rate'] * 100:.0f}%  avg_score={s['avg_score']:.2f}")
    print(f"{'─' * 60}\n")

    save_json(results, json_path)
    save_html(results, html_path)


if __name__ == "__main__":
    _cli()
