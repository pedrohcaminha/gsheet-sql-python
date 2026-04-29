import sys

import click
from rich.console import Console
from rich.table import Table as RichTable

console = Console()


def run_repl(db, spreadsheet_name: str) -> None:
    console.print(f"[bold green]gsheets-sql[/] (spreadsheet: [bold]{spreadsheet_name}[/])")
    console.print("Type [bold]\\?[/] for help, [bold]\\q[/] to quit.\n")

    buffer: list[str] = []

    while True:
        try:
            prompt = f"{spreadsheet_name}> " if not buffer else "... "
            line = input(prompt)
        except (EOFError, KeyboardInterrupt):
            console.print("\nBye!")
            break

        stripped = line.strip()
        if not stripped:
            continue

        if stripped.startswith("\\"):
            _handle_meta(stripped, db)
            continue

        buffer.append(stripped)

        if stripped.endswith(";"):
            sql = " ".join(buffer).rstrip(";").strip()
            buffer.clear()
            _execute_and_display(sql, db)


def _handle_meta(cmd: str, db) -> None:
    parts = cmd.split()
    name = parts[0]

    if name == "\\q":
        console.print("Bye!")
        sys.exit(0)

    elif name == "\\?":
        _show_help()

    elif name == "\\dt":
        tables = db.tables()
        t = RichTable(title="Tables")
        t.add_column("Name", style="cyan")
        for tbl in tables:
            t.add_row(tbl)
        console.print(t)

    elif name == "\\d" and len(parts) > 1:
        table_name = parts[1]
        try:
            schema = db[table_name].schema()
        except Exception as e:
            console.print(f"[red]Error:[/] {e}")
            return
        t = RichTable(title=f"Table: {table_name}")
        t.add_column("Column", style="cyan")
        t.add_column("Type", style="yellow")
        for col, typ in schema.items():
            t.add_row(col, typ)
        console.print(t)

    elif name == "\\d":
        console.print("[yellow]Usage:[/] \\d <table_name>")

    else:
        console.print(f"[red]Unknown meta-command:[/] {name}  (type \\? for help)")


def _execute_and_display(sql: str, db) -> None:
    sql_upper = sql.strip().upper()
    try:
        if sql_upper.startswith("SELECT"):
            df = db.query(sql, as_dataframe=True)
            _display_df(df)
        else:
            count = db.execute(sql)
            console.print(f"[green]{count} row(s) affected[/]")
    except Exception as e:
        console.print(f"[red]Error:[/] {e}")


def _display_df(df) -> None:
    if df.empty:
        console.print("[yellow](0 rows)[/]")
        return
    t = RichTable(show_header=True, header_style="bold")
    for col in df.columns:
        t.add_column(str(col))
    for _, row in df.iterrows():
        t.add_row(*["NULL" if v is None else str(v) for v in row])
    console.print(t)
    console.print(f"[dim]({len(df)} {'row' if len(df) == 1 else 'rows'})[/]")


def _show_help() -> None:
    console.print(
        """[bold]Meta-commands[/]
  [cyan]\\dt[/]           list all tables
  [cyan]\\d <table>[/]    describe table schema
  [cyan]\\q[/]            quit
  [cyan]\\?[/]            show this help

[bold]SQL[/]
  Terminate statements with [bold];[/]
  Multi-line queries are supported — keep typing until you add the semicolon

[bold]Examples[/]
  SELECT * FROM clientes LIMIT 10;
  INSERT INTO clientes (nome, idade) VALUES ('Ana', 28);
  UPDATE clientes SET idade = 29 WHERE nome = 'Ana';
  DELETE FROM clientes WHERE nome = 'Ana';"""
    )


@click.command()
@click.option("--id", "spreadsheet_id", required=True, help="Google Spreadsheet ID")
@click.option(
    "--credentials",
    default="credentials.json",
    show_default=True,
    help="Path to credentials JSON (service account or OAuth client secrets)",
)
@click.option("--cache-ttl", default=60, show_default=True, help="Cache TTL in seconds (0 = off)")
@click.option("--header-row", default=1, show_default=True, help="Row number of headers")
def cli(spreadsheet_id: str, credentials: str, cache_ttl: int, header_row: int) -> None:
    from . import connect

    try:
        db = connect(
            spreadsheet_id=spreadsheet_id,
            credentials=credentials,
            cache_ttl=cache_ttl,
            header_row=header_row,
        )
    except Exception as e:
        console.print(f"[red]Connection failed:[/] {e}")
        sys.exit(1)

    run_repl(db, db._ss.title)
