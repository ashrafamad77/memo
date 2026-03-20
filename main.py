#!/usr/bin/env python3
"""CLI for the Personal Memory Pipeline."""
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pipeline import MemoryPipeline
from pipeline.extractor import ExtractedEntity, ExtractionResult
from config import DATA_DIR, USER_NAME

console = Console()
DATA_DIR.mkdir(parents=True, exist_ok=True)


def cmd_add(pipeline: MemoryPipeline, text: str):
    """Process and store a journal entry."""
    result = pipeline.process(text)
    
    console.print(Panel("[green]✓ Entry stored[/green]", title="Success"))
    console.print(f"  ID: [dim]{result['entry_id']}[/dim]")
    
    if result["entities"]:
        table = Table(title="Extracted entities")
        table.add_column("Entity", style="cyan")
        table.add_column("Type", style="magenta")
        for e in result["entities"]:
            table.add_row(e["text"], e["type"])
        console.print(table)
    if result.get("relations"):
        rel_table = Table(title="Extracted relations (triplets)")
        rel_table.add_column("Subject", style="cyan")
        rel_table.add_column("Predicate", style="yellow")
        rel_table.add_column("Object", style="green")
        rel_table.add_column("Sentiment", justify="right")
        for r in result["relations"]:
            rel_table.add_row(r["subject"], r["predicate"], r["object"], f"{r['sentiment']:.2f}")
        console.print(rel_table)

    console.print(f"  Graph: {result['graph']} | Vector: {result['vector']}")


def cmd_add_agentic(pipeline: MemoryPipeline, text: str):
    """Process and store a journal entry via LangGraph workflow."""
    result = pipeline.process_agentic(text)

    console.print(Panel("[green]✓ Entry stored (agentic)[/green]", title="Success"))
    console.print(f"  ID: [dim]{result['entry_id']}[/dim]")

    if result["entities"]:
        table = Table(title="Extracted entities")
        table.add_column("Entity", style="cyan")
        table.add_column("Type", style="magenta")
        for e in result["entities"]:
            table.add_row(e["text"], e["type"])
        console.print(table)
    if result.get("relations"):
        rel_table = Table(title="Extracted relations (triplets)")
        rel_table.add_column("Subject", style="cyan")
        rel_table.add_column("Predicate", style="yellow")
        rel_table.add_column("Object", style="green")
        rel_table.add_column("Sentiment", justify="right")
        for r in result["relations"]:
            rel_table.add_row(r["subject"], r["predicate"], r["object"], f"{r['sentiment']:.2f}")
        console.print(rel_table)

    console.print(f"  Graph: {result['graph']} | Vector: {result['vector']}")


def cmd_search(pipeline: MemoryPipeline, query: str, n: int = 5):
    """Semantic search over entries."""
    results = pipeline.search_semantic(query, n_results=n)
    
    if not results:
        console.print("[yellow]No matching entries found.[/yellow]")
        return
    
    console.print(Panel(f"Query: [cyan]{query}[/cyan]", title="Semantic search"))
    for i, r in enumerate(results, 1):
        text_preview = r["text"][:200] + "..." if len(r["text"]) > 200 else r["text"]
        console.print(f"\n[bold]{i}. [{r['id'][:8]}...][/bold]")
        console.print(f"  {text_preview}")


def cmd_entity(pipeline: MemoryPipeline, name: str):
    """Search entries by entity name."""
    results = pipeline.search_by_entity(name)
    
    if not results:
        console.print(f"[yellow]No entries found for '{name}'.[/yellow]")
        return
    
    console.print(Panel(f"Entity: [cyan]{name}[/cyan]", title="Graph search"))
    for r in results:
        text_preview = (r["text"] or "")[:150] + "..." if len(r.get("text", "") or "") > 150 else (r.get("text") or "")
        console.print(f"\n  • {text_preview}")


def cmd_list(pipeline: MemoryPipeline, limit: int = 20):
    """List known entities."""
    entities = pipeline.list_entities(limit=limit)
    
    if not entities:
        console.print("[yellow]No entities in graph. Add some journal entries first.[/yellow]")
        return
    
    table = Table(title="Known entities")
    table.add_column("Type", style="magenta")
    table.add_column("Name", style="cyan")
    table.add_column("Mentions", justify="right")
    for e in entities:
        table.add_row(
            str(e.get("type", "")),
            str(e.get("name", "")),
            str(e.get("mentions", 0)),
        )
    console.print(table)


def cmd_reset(pipeline: MemoryPipeline):
    """Clear Neo4j graph (event-centric schema reset)."""
    if pipeline.reset_graph():
        console.print(Panel("[green]✓ Graph Neo4j vidé.[/green]", title="Reset"))
    else:
        console.print("[yellow]Neo4j non disponible.[/yellow]")


def cmd_reset_graph(pipeline: MemoryPipeline):
    if pipeline.reset_graph():
        console.print(Panel("[green]✓ Graph Neo4j vidé.[/green]", title="Reset graph"))
    else:
        console.print("[yellow]Neo4j non disponible.[/yellow]")


def cmd_reset_vector(pipeline: MemoryPipeline):
    if pipeline.reset_vector():
        console.print(Panel("[green]✓ Vector store (Weaviate) vidé.[/green]", title="Reset vector"))
    else:
        console.print("[yellow]Weaviate non disponible.[/yellow]")


def cmd_reset_all(pipeline: MemoryPipeline):
    res = pipeline.reset_all()
    msg = f"Graph: {'ok' if res['graph'] else 'skipped'} | Vector: {'ok' if res['vector'] else 'skipped'}"
    console.print(Panel(f"[green]✓ Reset terminé.[/green]\n{msg}", title="Reset all"))


def cmd_self_test(pipeline: MemoryPipeline):
    """Automated regression: reset, ingest fixtures, validate CIDOC shape."""
    fixtures = [
        (
            "wake_up_causal",
            "Je n'avais pas de cours a donner aujourd'hui pour l'universite An-Najah a Nablus, "
            "mais d'habitude je me leve plus tot a cause du decalage horaire entre la France et la Palestine.",
        ),
        (
            "multi_event_sequence",
            "Ce matin j'ai pris le RER C a 7h40, puis je suis arrive au bureau a 8h20.",
        ),
        (
            "same_name_ambiguity",
            "Aujourd'hui j'ai dejeuné avec Marie a Paris pour discuter du projet IA.",
        ),
    ]

    console.print(Panel("[bold]Running automated regression tests[/bold]", title="Self test"))
    cmd_reset_all(pipeline)

    ingest_rows = []
    for name, sentence in fixtures:
        try:
            out = pipeline.process_agentic(sentence)
            ok = (out.get("graph") == "ok")
            ingest_rows.append((name, "PASS" if ok else "FAIL", out.get("graph", "unknown")))
        except Exception as e:
            ingest_rows.append((name, "FAIL", str(e)))

    # Deterministic fixture (no LLM dependence) to validate core CIDOC writing invariants.
    # This avoids random regressions from extraction variability.
    try:
        deterministic = ExtractionResult(
            entities=[
                ExtractedEntity(text=(USER_NAME or "User"), label="Person", start_char=0, end_char=0),
                ExtractedEntity(text="France", label="Place", start_char=0, end_char=0),
                ExtractedEntity(text="Palestine", label="Place", start_char=0, end_char=0),
                ExtractedEntity(text="An-Najah University", label="Organization", start_char=0, end_char=0),
                ExtractedEntity(text="time difference", label="Concept", start_char=0, end_char=0),
            ],
            metadata={
                "event_type": "wake up",
                "event_time_iso": "2026-03-13T06:30:00Z",
                "event_time_confidence": 0.9,
                "causal_factors": [
                    {
                        "target_idx": 1,
                        "factor_kind": "today_specific",
                        "text": "no teaching today",
                        "relation": "INFLUENCES",
                        "confidence": 0.8,
                        "evidence": "explicit condition",
                    },
                    {
                        "target_idx": 1,
                        "factor_kind": "habit",
                        "text": "usually wake earlier",
                        "relation": "INFLUENCES",
                        "confidence": 0.7,
                        "evidence": "habit statement",
                    },
                    {
                        "target_idx": 1,
                        "factor_kind": "propositional",
                        "text": "time difference France-Palestine",
                        "relation": "INFLUENCES",
                        "confidence": 0.7,
                        "evidence": "cause proposition",
                    },
                ],
                # Force fallback single-event path but keep activity label explicit.
                "events": [],
            },
            raw_text="Deterministic self-test input for CIDOC causal chain.",
        )
        out = pipeline.persist_extraction(
            text="Deterministic self-test input for CIDOC causal chain.",
            extraction=deterministic,
        )
        ok = (out.get("graph") == "ok")
        ingest_rows.append(("deterministic_causal", "PASS" if ok else "FAIL", out.get("graph", "unknown")))
    except Exception as e:
        ingest_rows.append(("deterministic_causal", "FAIL", str(e)))

    # Validate CIDOC and causal quality with direct Neo4j checks.
    checks = []
    if not pipeline.graph_store:
        checks.append(("neo4j_available", False, "Graph store unavailable"))
    else:
        def _count(query: str) -> int:
            with pipeline.graph_store.driver.session() as session:
                rec = session.run(query).single()
                if not rec:
                    return 0
                val = rec.get("c", 0)
                return int(val or 0)

        try:
            checks.append((
                "only_cidoc_labels",
                _count("MATCH (n) WHERE any(l IN labels(n) WHERE l IN ['Person','Place','Concept','Event','Entry','Date','Day','User']) RETURN count(n) AS c") == 0,
                "No legacy labels",
            ))
            checks.append((
                "no_placeholder_types",
                _count("MATCH (t:E55_Type) WHERE toLower(coalesce(t.name,'')) IN ['none','unknown','null','n/a'] RETURN count(t) AS c") == 0,
                "No placeholder type nodes",
            ))
            checks.append((
                "has_today_specific_edge",
                _count("MATCH (:E7_Activity)-[r:P15_was_influenced_by]->(:E89_Propositional_Object) WHERE r.inference_type='TODAY_SPECIFIC' RETURN count(r) AS c") > 0,
                "Today-specific causal edge exists",
            ))
            checks.append((
                "has_habit_proposition_chain",
                _count("MATCH (:E28_Conceptual_Object)-[r:P15_was_influenced_by]->(:E89_Propositional_Object) WHERE r.inference_type='PROPOSITION_TO_HABIT' RETURN count(r) AS c") > 0,
                "Habit <- proposition chain exists",
            ))
            checks.append((
                "no_fallback_wrong_physical_places",
                _count("MATCH (e:E7_Activity)-[:P7_took_place_at]->(:E53_Place) WHERE toLower(coalesce(e.event_type,''))='wake up' RETURN count(*) AS c") == 0,
                "Wake-up fallback should not create physical place links",
            ))
            p14_count = _count("MATCH (:E7_Activity)-[:P14_carried_out_by]->(:E39_Actor) RETURN count(*) AS c")
            p14i_count = _count("MATCH (:E39_Actor)-[:P14i_performed]->(:E7_Activity) RETURN count(*) AS c")
            has_configured_user = bool((USER_NAME or "").strip())
            checks.append((
                "p14_direction_pair",
                ((p14_count == p14i_count) and (p14_count > 0 if has_configured_user else True)),
                f"P14/P14i paired counts (p14={p14_count}, p14i={p14i_count}, user_configured={has_configured_user})",
            ))
        except Exception as e:
            checks.append(("neo4j_checks", False, str(e)))

    ingest_table = Table(title="Ingestion phase")
    ingest_table.add_column("Fixture", style="cyan")
    ingest_table.add_column("Status")
    ingest_table.add_column("Details", style="dim")
    for name, status, details in ingest_rows:
        color = "green" if status == "PASS" else "red"
        ingest_table.add_row(name, f"[{color}]{status}[/{color}]", str(details))
    console.print(ingest_table)

    check_table = Table(title="Validation phase")
    check_table.add_column("Check", style="cyan")
    check_table.add_column("Status")
    check_table.add_column("Expectation", style="dim")
    passed = 0
    for name, ok, detail in checks:
        status = "PASS" if ok else "FAIL"
        color = "green" if ok else "red"
        if ok:
            passed += 1
        check_table.add_row(name, f"[{color}]{status}[/{color}]", detail)
    console.print(check_table)
    total = len(checks)
    overall_ok = passed == total and all(r[1] == "PASS" for r in ingest_rows)
    end_msg = f"{passed}/{total} validation checks passed."
    if overall_ok:
        console.print(Panel(f"[green]✓ Self test passed[/green]\n{end_msg}", title="Result"))
    else:
        console.print(Panel(f"[red]✗ Self test failed[/red]\n{end_msg}", title="Result"))


def main():
    if len(sys.argv) < 2:
        console.print(Panel("""
[bold]Personal Memory Pipeline[/bold] - PoC

Usage:
  python main.py add "<texte du journal>"
  python main.py add-agentic "<texte du journal>"
  python main.py search "<requête sémantique>" [--n 5]
  python main.py entity "<nom personne/lieu>"
  python main.py list [--limit 20]
  python main.py reset
  python main.py reset-graph
  python main.py reset-vector
  python main.py reset-all
  python main.py self-test

Examples:
  python main.py add "Aujourd'hui j'ai déjeuné avec Marie à Paris. On a parlé du projet."
  python main.py add-agentic "Aujourd'hui j'ai déjeuné avec Marie à Paris. On a parlé du projet."
  python main.py search "repas avec des amis"
  python main.py entity "Marie"
  python main.py list
""", title="Usage"))
        return 1
    
    command = sys.argv[1].lower()
    args = sys.argv[2:]
    
    # Parse --n and --limit
    n_results = 5
    limit = 20
    if "--n" in args:
        idx = args.index("--n")
        if idx + 1 < len(args):
            n_results = int(args[idx + 1])
            args = [a for i, a in enumerate(args) if i not in (idx, idx + 1)]
    if "--limit" in args:
        idx = args.index("--limit")
        if idx + 1 < len(args):
            limit = int(args[idx + 1])
            args = [a for i, a in enumerate(args) if i not in (idx, idx + 1)]
    
    pipeline = MemoryPipeline()
    
    try:
        if command == "add":
            text = " ".join(args) if args else input("Entrée journal: ")
            if not text.strip():
                console.print("[red]Texte vide.[/red]")
                return 1
            cmd_add(pipeline, text)

        elif command == "add-agentic":
            text = " ".join(args) if args else input("Entrée journal: ")
            if not text.strip():
                console.print("[red]Texte vide.[/red]")
                return 1
            cmd_add_agentic(pipeline, text)
        
        elif command == "search":
            query = " ".join(args) if args else "expériences récentes"
            cmd_search(pipeline, query, n=n_results)
        
        elif command == "entity":
            name = " ".join(args) if args else ""
            if not name:
                console.print("[red]Précisez un nom d'entité.[/red]")
                return 1
            cmd_entity(pipeline, name)
        
        elif command == "list":
            cmd_list(pipeline, limit=limit)

        elif command == "reset":
            cmd_reset(pipeline)

        elif command == "reset-graph":
            cmd_reset_graph(pipeline)

        elif command == "reset-vector":
            cmd_reset_vector(pipeline)

        elif command == "reset-all":
            cmd_reset_all(pipeline)

        elif command == "self-test":
            cmd_self_test(pipeline)

        else:
            console.print(f"[red]Commande inconnue: {command}[/red]")
            return 1
    finally:
        pipeline.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
