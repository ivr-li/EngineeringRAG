#!/usr/bin/env python3
"""
eval_retrieval.py
=================
Автономный скрипт оценки качества retrieval-системы.

Запуск:
    python eval_retrieval.py                        # все тесты
    python eval_retrieval.py --mode hybrid          # только hybrid
    python eval_retrieval.py --top-k 5              # top-k override
    python eval_retrieval.py --output results.json  # кастомный путь отчёта
    pytest eval_retrieval.py -v                     # через pytest

Выходные файлы:
    eval_results_<timestamp>.json   — полный лог всех тестов
    eval_report_<timestamp>.html    — визуальный отчёт
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

# ---------------------------------------------------------------------------
# Тест-кейсы
# ---------------------------------------------------------------------------

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
    # ------------------------------------------------------------------
    # Блок 1 — Прямой точный ответ
    # ------------------------------------------------------------------
    TestCase(
        id="direct_01",
        query="Минимальная ширина эвакуационного выхода",
        expected_filename="СП 1.13130",
        expected_keywords=["ширина", "выход", "эвакуационн"],
        category="direct",
        min_score=10.0,
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
        query="Инсоляция жилых помещений продолжительность часов",
        expected_filename="СП 54.13330",
        expected_keywords=["инсоляция", "жилых", "час"],
        category="direct",
        min_score=8.0,
    ),
    # ------------------------------------------------------------------
    # Блок 2 — Ответ в таблице (тест Sliding Window)
    # ------------------------------------------------------------------
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
        query="Нормы освещённости для офисных помещений люкс",
        expected_filename="СП 52",
        expected_keywords=["освещённость", "офис", "лк"],
        category="table",
        min_score=7.0,
    ),
    TestCase(
        id="table_04",
        query="Классификация зданий по функциональной пожарной опасности Ф1 Ф2 Ф3",
        expected_filename="123-ФЗ",
        expected_keywords=["Ф1", "Ф2", "функциональн", "пожарн"],
        category="table",
        min_score=8.0,
    ),
    # ------------------------------------------------------------------
    # Блок 3 — Межсекционная цепочка (тест Ref Expansion)
    # ------------------------------------------------------------------
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
    # ------------------------------------------------------------------
    # Блок 4 — Документа нет в базе (ожидаем низкий score или отсутствие)
    # ------------------------------------------------------------------
    TestCase(
        id="absent_01",
        query="При какой этажности здания требуется устройство автоматической пожарной сигнализации",
        expected_filename="СП 486",  # документа нет → ожидаем провал
        expected_keywords=["этажност", "486"],
        category="absent",
        min_score=99.0,  # заведомо высокий порог → тест ожидает fail
    ),
    TestCase(
        id="absent_02",
        query="Требования к сейсмостойкости зданий в зоне 8 баллов",
        expected_filename="СП 14",  # нет в базе
        expected_keywords=["сейсм", "балл"],
        category="absent",
        min_score=99.0,
    ),
    # ------------------------------------------------------------------
    # Блок 5 — Режимный тест (один запрос во всех трёх режимах)
    # ------------------------------------------------------------------
    TestCase(
        id="mode_hybrid",
        query="предел огнестойкости несущих конструкций REI таблица",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкост"],
        mode="hybrid",
        category="mode_compare",
        min_score=9.0,
    ),
    TestCase(
        id="mode_dense",
        query="предел огнестойкости несущих конструкций REI таблица",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкост"],
        mode="dense",
        category="mode_compare",
        min_score=0.55,
    ),
    TestCase(
        id="mode_sparse",
        query="предел огнестойкости несущих конструкций REI таблица",
        expected_filename="СП 2.13130",
        expected_keywords=["REI", "огнестойкост"],
        mode="sparse",
        category="mode_compare",
        min_score=3.0,
    ),
]


# ---------------------------------------------------------------------------
# Результат одного теста
# ---------------------------------------------------------------------------


@dataclass
class TestResult:
    case_id: str
    query: str
    mode: str
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
    all_scores: list[float]
    error: str | None = None


# ---------------------------------------------------------------------------
# Движок тестирования
# ---------------------------------------------------------------------------


class RetrievalEvaluator:
    def __init__(self, top_k_override: int | None = None, mode_override: SearchMode | None = None):
        from retriever.retriever import QdrantRetriever  # noqa: PLC0415

        self.retriever = QdrantRetriever()
        self.top_k_override = top_k_override
        self.mode_override = mode_override

    def run_case(self, case: TestCase) -> TestResult:
        mode = self.mode_override or case.mode
        top_k = self.top_k_override or case.top_k
        t0 = time.perf_counter()
        error = None
        results = []
        try:
            results = self.retriever.search(
                query=case.query,
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

        # Проверки
        filename_hit = any(case.expected_filename.lower() in fn.lower() for fn in top3_filenames)
        keyword_hit = any(
            kw.lower() in (r.text or "").lower() for r in top3 for kw in case.expected_keywords
        )
        score_ok = top1_score >= case.min_score

        # Для absent-категории: тест "проходит" если filename НЕ найден (документа нет)
        if case.category == "absent":
            passed = not filename_hit
        else:
            passed = filename_hit and keyword_hit

        return TestResult(
            case_id=case.id,
            query=case.query,
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
            all_scores=all_scores,
            error=error,
        )

    def run_all(self, cases: list[TestCase] | None = None) -> list[TestResult]:
        cases = cases or TEST_CASES
        results = []
        total = len(cases)
        for i, case in enumerate(cases, 1):
            print(f"  [{i:2d}/{total}] {case.id:<20} mode={case.mode:<7} ", end="", flush=True)
            r = self.run_case(case)
            status = "✅ PASS" if r.passed else "❌ FAIL"
            print(
                f"{status}  score={r.top1_score:7.4f}  {r.latency_ms:5.0f}ms  → {r.top1_filename[:40]}"
            )
            results.append(r)
        return results


def save_json(results: list[TestResult], path: Path) -> None:
    data = {
        "generated_at": datetime.now().isoformat(),
        "summary": _summary(results),
        "results": [asdict(r) for r in results],
    }
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n📄 JSON-лог сохранён: {path}")


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
    return {
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": round(passed / total, 3) if total else 0,
        "avg_latency_ms": round(sum(r.latency_ms for r in results) / total, 1) if total else 0,
        "by_category": by_cat,
    }


def save_html(results: list[TestResult], path: Path) -> None:
    summary = _summary(results)
    rows = []
    for r in results:
        status_cls = "pass" if r.passed else "fail"
        status_icon = "✅" if r.passed else "❌"
        score_cls = "score-ok" if r.score_ok else "score-low"
        kw_badge = "🟢" if r.keyword_hit else "🔴"
        fn_badge = "🟢" if r.filename_hit else "🔴"
        top3_str = "<br>".join(r.top3_filenames) or "—"
        error_str = f'<span class="error">⚠ {r.error}</span>' if r.error else ""
        rows.append(f"""
        <tr class="{status_cls}">
          <td><code>{r.case_id}</code></td>
          <td class="query-cell">{r.query}</td>
          <td><span class="badge badge-{r.category}">{r.category}</span></td>
          <td>{r.mode}</td>
          <td class="{score_cls}">{r.top1_score:.4f}</td>
          <td class="snippet">{r.top1_text_snippet}{error_str}</td>
          <td class="filenames">{top3_str}</td>
          <td>{fn_badge}</td>
          <td>{kw_badge}</td>
          <td>{r.latency_ms:.0f}ms</td>
          <td>{status_icon}</td>
        </tr>""")

    # Данные для мини-графика по категориям (Chart.js inline)
    cat_labels = list(summary["by_category"].keys())
    cat_pass = [summary["by_category"][c]["pass_rate"] * 100 for c in cat_labels]
    cat_scores = [summary["by_category"][c]["avg_score"] for c in cat_labels]

    html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>Retrieval Eval — {summary["generated_at"] if False else datetime.now().strftime("%Y-%m-%d %H:%M")}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0f0f12; color: #d4d4d8; font-size: 14px; }}
  header {{ background: #18181b; border-bottom: 1px solid #27272a; padding: 20px 32px; display: flex; align-items: center; gap: 24px; }}
  header h1 {{ font-size: 1.4rem; font-weight: 600; color: #fafafa; }}
  header span {{ color: #71717a; font-size: 0.85rem; }}
  .metrics {{ display: flex; gap: 16px; padding: 24px 32px; flex-wrap: wrap; }}
  .metric-card {{ background: #18181b; border: 1px solid #27272a; border-radius: 10px; padding: 16px 24px; min-width: 140px; }}
  .metric-card .label {{ color: #71717a; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; }}
  .metric-card .value {{ font-size: 1.8rem; font-weight: 700; margin-top: 4px; }}
  .metric-card.green .value {{ color: #4ade80; }}
  .metric-card.red .value {{ color: #f87171; }}
  .metric-card.blue .value {{ color: #60a5fa; }}
  .metric-card.yellow .value {{ color: #fbbf24; }}
  .charts {{ display: flex; gap: 24px; padding: 0 32px 24px; flex-wrap: wrap; }}
  .chart-box {{ background: #18181b; border: 1px solid #27272a; border-radius: 10px; padding: 20px; flex: 1; min-width: 300px; max-width: 480px; }}
  .chart-box h3 {{ font-size: 0.85rem; color: #a1a1aa; margin-bottom: 12px; text-transform: uppercase; letter-spacing: 0.05em; }}
  .table-wrap {{ padding: 0 32px 32px; overflow-x: auto; }}
  table {{ width: 100%; border-collapse: collapse; background: #18181b; border-radius: 10px; overflow: hidden; }}
  th {{ background: #27272a; color: #a1a1aa; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.05em; padding: 10px 12px; text-align: left; white-space: nowrap; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid #27272a; vertical-align: top; }}
  tr.pass {{ }}
  tr.fail td {{ background: rgba(248,113,113,0.04); }}
  tr:hover td {{ background: rgba(255,255,255,0.03); }}
  .query-cell {{ max-width: 260px; color: #e4e4e7; }}
  .snippet {{ max-width: 220px; font-size: 0.78rem; color: #71717a; }}
  .filenames {{ max-width: 200px; font-size: 0.75rem; color: #a1a1aa; }}
  code {{ background: #27272a; padding: 2px 6px; border-radius: 4px; font-size: 0.8rem; }}
  .score-ok {{ color: #4ade80; font-weight: 600; }}
  .score-low {{ color: #f87171; font-weight: 600; }}
  .error {{ color: #f87171; }}
  .badge {{ padding: 2px 8px; border-radius: 20px; font-size: 0.7rem; font-weight: 600; }}
  .badge-direct {{ background: #1e3a5f; color: #60a5fa; }}
  .badge-table {{ background: #1a3a2a; color: #4ade80; }}
  .badge-chain {{ background: #3a2a1a; color: #fbbf24; }}
  .badge-absent {{ background: #3a1a1a; color: #f87171; }}
  .badge-mode_compare {{ background: #2a1a3a; color: #c084fc; }}
</style>
</head>
<body>
<header>
  <h1>📐 Retrieval Eval Report</h1>
  <span>Сгенерировано: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</span>
</header>

<div class="metrics">
  <div class="metric-card {"green" if summary["pass_rate"] >= 0.8 else "red"}">
    <div class="label">Pass Rate</div>
    <div class="value">{summary["pass_rate"] * 100:.0f}%</div>
  </div>
  <div class="metric-card green"><div class="label">Passed</div><div class="value">{summary["passed"]}</div></div>
  <div class="metric-card red"><div class="label">Failed</div><div class="value">{summary["failed"]}</div></div>
  <div class="metric-card blue"><div class="label">Total Tests</div><div class="value">{summary["total"]}</div></div>
  <div class="metric-card yellow"><div class="label">Avg Latency</div><div class="value">{summary["avg_latency_ms"]:.0f}ms</div></div>
</div>

<div class="charts">
  <div class="chart-box">
    <h3>Pass Rate по категориям</h3>
    <canvas id="chartPassRate" height="180"></canvas>
  </div>
  <div class="chart-box">
    <h3>Средний Score топ-1 по категориям</h3>
    <canvas id="chartScore" height="180"></canvas>
  </div>
</div>

<div class="table-wrap">
<table>
  <thead>
    <tr>
      <th>ID</th><th>Запрос</th><th>Категория</th><th>Mode</th>
      <th>Score</th><th>Топ-1 текст</th><th>Топ-3 файлы</th>
      <th>Файл</th><th>Слова</th><th>Latency</th><th>Статус</th>
    </tr>
  </thead>
  <tbody>
    {"".join(rows)}
  </tbody>
</table>
</div>

<script>
const catLabels = {json.dumps(cat_labels, ensure_ascii=False)};
const catPass   = {json.dumps(cat_pass)};
const catScores = {json.dumps(cat_scores)};
const palette = ['#60a5fa','#4ade80','#fbbf24','#f87171','#c084fc'];

new Chart(document.getElementById('chartPassRate'), {{
  type: 'bar',
  data: {{
    labels: catLabels,
    datasets: [{{ data: catPass, backgroundColor: palette, borderRadius: 6, label: 'Pass %' }}]
  }},
  options: {{
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      y: {{ min: 0, max: 100, ticks: {{ color: '#71717a', callback: v => v+'%' }}, grid: {{ color: '#27272a' }} }},
      x: {{ ticks: {{ color: '#a1a1aa' }}, grid: {{ display: false }} }}
    }}
  }}
}});

new Chart(document.getElementById('chartScore'), {{
  type: 'bar',
  data: {{
    labels: catLabels,
    datasets: [{{ data: catScores, backgroundColor: palette, borderRadius: 6, label: 'Avg Score' }}]
  }},
  options: {{
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      y: {{ ticks: {{ color: '#71717a' }}, grid: {{ color: '#27272a' }} }},
      x: {{ ticks: {{ color: '#a1a1aa' }}, grid: {{ display: false }} }}
    }}
  }}
}});
</script>
</body>
</html>"""

    path.write_text(html, encoding="utf-8")
    print(f"🌐 HTML-отчёт сохранён: {path}")


# # ---------------------------------------------------------------------------
# # pytest-адаптер
# # ---------------------------------------------------------------------------

# try:
#     import pytest  # noqa: F401

#     HAS_PYTEST = True
# except ImportError:
#     HAS_PYTEST = False


# def _make_pytest_cases():
#     """Генерирует pytest-параметры из TEST_CASES."""
#     return [(c.id, c) for c in TEST_CASES]


# if HAS_PYTEST:
#     import pytest as _pytest

#     @_pytest.mark.parametrize("case_id,case", _make_pytest_cases(), ids=[c.id for c in TEST_CASES])
#     def test_retrieval(case_id: str, case: TestCase):
#         from retriever.retriever import QdrantRetriever

#         retriever = QdrantRetriever()
#         results = retriever.search(
#             query=case.query,
#             top_k=case.top_k,
#             prefetch_k=case.top_k * 4,
#             mode=case.mode,
#         )
#         top3_filenames = [r.filename for r in results[:3]]
#         keyword_hit = any(
#             kw.lower() in (r.text or "").lower()
#             for r in results[:3]
#             for kw in case.expected_keywords
#         )
#         if case.category == "absent":
#             filename_hit = any(
#                 case.expected_filename.lower() in fn.lower() for fn in top3_filenames
#             )
#             assert not filename_hit, (
#                 f"[{case_id}] Документ '{case.expected_filename}' неожиданно найден — "
#                 f"он должен отсутствовать в базе. top3: {top3_filenames}"
#             )
#         else:
#             filename_hit = any(
#                 case.expected_filename.lower() in fn.lower() for fn in top3_filenames
#             )
#             assert filename_hit, (
#                 f"[{case_id}] Ожидался файл '{case.expected_filename}' в топ-3. "
#                 f"Получено: {top3_filenames}"
#             )
#             assert keyword_hit, (
#                 f"[{case_id}] Ключевые слова {case.expected_keywords} не найдены в тексте топ-3."
#             )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Retrieval evaluator")
    parser.add_argument("--mode", choices=["hybrid", "dense", "sparse"], default=None)
    parser.add_argument("--top-k", type=int, default=None, dest="top_k")
    parser.add_argument(
        "--category",
        default=None,
        help="Фильтр по категории: direct/table/chain/absent/mode_compare",
    )
    parser.add_argument(
        "--output", default=None, help="Путь для JSON-лога (по умолчанию: eval_results_<ts>.json)"
    )
    args = parser.parse_args()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = Path(args.output) if args.output else Path(f"eval_results_{ts}.json")
    html_path = json_path.with_name(json_path.stem.replace("results", "report") + ".html")

    cases = TEST_CASES
    if args.category:
        cases = [c for c in cases if c.category == args.category]
        if not cases:
            print(f"Нет тест-кейсов для категории '{args.category}'")
            sys.exit(1)

    print(
        f"\n🔍 Запуск {len(cases)} тестов  |  mode={args.mode or 'per-case'}  |  top_k={args.top_k or 'per-case'}\n"
    )
    evaluator = RetrievalEvaluator(top_k_override=args.top_k, mode_override=args.mode)
    results = evaluator.run_all(cases)

    summary = _summary(results)
    print(f"\n{'─' * 60}")
    print(
        f"  Итого: {summary['passed']}/{summary['total']} passed  ({summary['pass_rate'] * 100:.0f}%)"
    )
    print(f"  Средняя задержка: {summary['avg_latency_ms']:.0f} ms")
    for cat, s in summary["by_category"].items():
        bar = "█" * int(s["pass_rate"] * 20)
        print(f"  {cat:<14} {bar:<20} {s['pass_rate'] * 100:.0f}%  avg_score={s['avg_score']:.2f}")
    print(f"{'─' * 60}\n")

    save_json(results, json_path)
    save_html(results, html_path)


if __name__ == "__main__":
    _cli()
