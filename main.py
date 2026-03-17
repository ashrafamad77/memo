#!/usr/bin/env python3
"""CLI for the Personal Memory Pipeline."""
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from pipeline import MemoryPipeline
from config import DATA_DIR

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


def main():
    if len(sys.argv) < 2:
        console.print(Panel("""
[bold]Personal Memory Pipeline[/bold] - PoC

Usage:
  python main.py add "<texte du journal>"
  python main.py search "<requête sémantique>" [--n 5]
  python main.py entity "<nom personne/lieu>"
  python main.py list [--limit 20]
  python main.py reset
  python main.py reset-graph
  python main.py reset-vector
  python main.py reset-all

Examples:
  python main.py add "Aujourd'hui j'ai déjeuné avec Marie à Paris. On a parlé du projet."
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

        else:
            console.print(f"[red]Commande inconnue: {command}[/red]")
            return 1
    finally:
        pipeline.close()
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
