#!/usr/bin/env python3
"""
Command Line Interface for PostgreSQL → Firebolt conversion

Examples:
  - Convert from a file and print to stdout (rule-based only):
      python cli.py convert -i input.sql

  - Convert from stdin and write to a file, with AI polish enabled:
      cat input.sql | python cli.py convert --enable-ai-polish -o output.sql

  - Convert with explicit OpenAI key override (otherwise .env OPENAI_API_KEY is used if present):
      python cli.py convert -i input.sql --openai-key sk-...
"""

import os
import sys
import argparse
from typing import Optional
from dotenv import load_dotenv

# Ensure project imports work when running as script
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from converter.query_converter import PostgreSQLToFireboltConverter


def read_sql_input(input_path: str) -> str:
    if input_path == '-' or input_path is None:
        return sys.stdin.read()
    with open(input_path, 'r') as f:
        return f.read()


def write_sql_output(sql_text: str, output_path: Optional[str]) -> None:
    if output_path:
        with open(output_path, 'w') as f:
            f.write(sql_text)
    else:
        sys.stdout.write(sql_text.rstrip() + "\n")


def cmd_convert(args: argparse.Namespace) -> int:
    # Load environment variables from .env (default behavior)
    load_dotenv()

    # Set AI polish flag via environment for converter to read
    os.environ['ENABLE_AI_POLISH'] = 'true' if args.enable_ai_polish else 'false'

    # Prefer explicit CLI key override, otherwise leave env as-is (from .env)
    openai_key = args.openai_key or os.getenv('OPENAI_API_KEY')

    # Initialize converter
    converter = PostgreSQLToFireboltConverter(openai_api_key=openai_key)

    # Read input SQL
    sql_in = read_sql_input(args.input)
    if not sql_in.strip():
        sys.stderr.write("No SQL provided on input. Use -i <file> or pipe into stdin.\n")
        return 2

    # Convert
    result = converter.convert(sql_in)
    converted = result.get('converted_sql', sql_in)

    # Optional diagnostics
    if args.print_method:
        sys.stderr.write(f"method_used: {result.get('method_used', 'unknown')}\n")
    if args.print_warnings and result.get('warnings'):
        for w in result['warnings']:
            sys.stderr.write(f"warning: {w}\n")
    if args.print_explanations and result.get('explanations'):
        for ex in result['explanations']:
            sys.stderr.write(f"explain: {ex}\n")

    # Output
    write_sql_output(converted, args.output)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='converter-cli',
        description='PostgreSQL → Firebolt converter CLI',
    )

    subparsers = parser.add_subparsers(dest='command', required=True)

    # convert command
    p_convert = subparsers.add_parser('convert', help='Convert PostgreSQL SQL to Firebolt SQL')
    p_convert.add_argument('-i', '--input', default='-', help='Input SQL file path or - for stdin (default: -)')
    p_convert.add_argument('-o', '--output', default=None, help='Output SQL file path (default: stdout)')
    p_convert.add_argument('--enable-ai-polish', action='store_true', help='Apply OpenAI polish after rule-based conversion')
    p_convert.add_argument('--openai-key', default=None, help='Override OpenAI API key (otherwise .env OPENAI_API_KEY is used)')
    p_convert.add_argument('--print-warnings', action='store_true', help='Print conversion warnings to stderr')
    p_convert.add_argument('--print-explanations', action='store_true', help='Print applied conversions to stderr')
    p_convert.add_argument('--print-method', action='store_true', help='Print conversion method used to stderr')
    p_convert.set_defaults(func=cmd_convert)

    return parser


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())


