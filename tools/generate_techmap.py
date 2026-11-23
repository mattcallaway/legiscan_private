"""Generate a deterministic first-party Python symbol and call inventory."""

import argparse
import ast
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import tokenize

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "docs" / "techmap.json"
DEFINITIONS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def expression(node):
    return ast.unparse(node) if node is not None else None


class Inventory(ast.NodeVisitor):
    def __init__(self, module):
        self.module = module
        self.scope = []
        self.classes = []
        self.symbols = {}
        self.imports = []
        self.calls = []

    @property
    def owner(self):
        return ".".join([self.module, *self.scope])

    def define(self, node, kind, **details):
        identifier = f"{self.owner}.{node.name}"
        self.symbols[identifier] = {
            "id": identifier,
            "name": node.name,
            "owner": self.owner,
            "kind": kind,
            "line": node.lineno,
            "end_line": node.end_lineno,
            "decorators": [expression(item) for item in node.decorator_list],
            **details,
        }
        for item in node.decorator_list:
            self.visit(item)
        self.scope.append(node.name)

    def visit_ClassDef(self, node):
        for item in [*node.bases, *node.keywords]:
            self.visit(item)
        self.define(node, "class", bases=[expression(base) for base in node.bases])
        self.classes.append(self.owner)
        for statement in node.body:
            self.visit(statement)
        self.classes.pop()
        self.scope.pop()

    def visit_FunctionDef(self, node):
        self.visit(node.args)
        if node.returns is not None:
            self.visit(node.returns)
        kind = "method" if self.classes and self.owner == self.classes[-1] else "function"
        self.define(
            node,
            kind,
            parameters=expression(node.args),
            returns=expression(node.returns),
            async_function=isinstance(node, ast.AsyncFunctionDef),
        )
        for statement in node.body:
            self.visit(statement)
        self.scope.pop()

    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Lambda(self, node):
        self.visit(node.args)
        name = f"<lambda@{node.lineno}:{node.col_offset}>"
        identifier = f"{self.owner}.{name}"
        self.symbols[identifier] = {
            "id": identifier,
            "name": name,
            "owner": self.owner,
            "kind": "lambda",
            "line": node.lineno,
            "end_line": node.end_lineno,
            "parameters": expression(node.args),
        }
        self.scope.append(name)
        self.visit(node.body)
        self.scope.pop()

    def visit_Import(self, node):
        for alias in node.names:
            self.imports.append({
                "owner": self.owner,
                "line": node.lineno,
                "binding": alias.asname or alias.name.split(".")[0],
                "target": alias.name if alias.asname else alias.name.split(".")[0],
                "imported": alias.name,
            })

    def visit_ImportFrom(self, node):
        source = "." * node.level + (node.module or "")
        for alias in node.names:
            self.imports.append({
                "owner": self.owner,
                "line": node.lineno,
                "binding": alias.asname or alias.name,
                "target": f"{source}.{alias.name}" if node.module else f"{source}{alias.name}",
                "imported": source,
            })

    def visit_Call(self, node):
        receiver = node.func
        self.calls.append({
            "caller": self.owner,
            "line": node.lineno,
            "column": node.col_offset,
            "expression": expression(receiver),
            "attribute": receiver.attr if isinstance(receiver, ast.Attribute) else None,
            "enclosing_class": self.classes[-1] if self.classes else None,
            "positional_arguments": len(node.args),
            "keyword_arguments": [item.arg for item in node.keywords],
        })
        self.generic_visit(node)


def scope_chain(owner):
    parts = owner.split(".")
    while parts:
        yield ".".join(parts)
        parts.pop()


def resolve(call, symbols, bindings, methods, modules):
    """Supply lexical links where possible and label heuristic attribute matches."""
    callee = call["expression"]
    direct = set()
    if callee.startswith(("self.", "cls.")) and call["enclosing_class"]:
        target = call["enclosing_class"] + "." + callee.split(".", 1)[1]
        if target in symbols:
            direct.add(target)
    elif all(part.isidentifier() for part in callee.split(".")):
        head, _, tail = callee.partition(".")
        for scope in scope_chain(call["caller"]):
            local = f"{scope}.{callee}"
            imports = bindings.get((scope, head), set())
            if local in symbols:
                direct.add(local)
            direct.update(
                imported + (f".{tail}" if tail else "")
                for imported in imports
                if imported + (f".{tail}" if tail else "") in symbols or (
                    not tail and imported in modules
                )
            )
            if direct or imports:
                break
        if not direct and callee in symbols:
            direct.add(callee)
    if direct:
        return "lexical", sorted(direct)
    candidates = methods.get(call["attribute"], set())
    if candidates:
        return "attribute_name_only", sorted(candidates)
    return "unresolved", []


def build():
    files = sorted((*ROOT.glob("*.py"), *ROOT.glob("tests/**/*.py"), *ROOT.glob("tools/**/*.py")))
    inventories = []
    for path in files:
        if path.is_symlink():
            continue
        relative = path.relative_to(ROOT)
        module = ".".join(relative.with_suffix("").parts)
        with tokenize.open(path) as handle:
            tree = ast.parse(handle.read(), filename=relative.as_posix())
        inventory = Inventory(module)
        inventory.visit(tree)
        inventories.append((relative, path, inventory))

    symbols = {}
    bindings = defaultdict(set)
    methods = defaultdict(set)
    modules = {inventory.module for _, _, inventory in inventories}
    for _, _, inventory in inventories:
        symbols.update(inventory.symbols)
        for record in inventory.imports:
            bindings[record["owner"], record["binding"]].add(record["target"])
        for identifier, symbol in inventory.symbols.items():
            if symbol["kind"] == "method":
                methods[symbol["name"]].add(identifier)

    mapped = []
    for relative, path, inventory in inventories:
        calls = []
        for call in sorted(inventory.calls, key=lambda item: (item["line"], item["column"], item["caller"])):
            resolution, targets = resolve(call, symbols, bindings, methods, modules)
            calls.append({
                **{key: value for key, value in call.items() if key not in {"attribute", "enclosing_class"}},
                "resolution": resolution,
                "targets": targets,
            })
        mapped.append({
            "path": relative.as_posix(),
            "module": inventory.module,
            "source_sha256_normalized_newlines": hashlib.sha256(
                path.read_bytes().replace(b"\r\n", b"\n")
            ).hexdigest(),
            "symbols": list(inventory.symbols.values()),
            "imports": inventory.imports,
            "calls": calls,
        })
    return {
        "schema_version": 1,
        "generator": "tools/generate_techmap.py",
        "related_docs": ["docs/TECHMAP.md", "docs/installation.json"],
        "scope": ["*.py", "tests/**/*.py", "tools/**/*.py"],
        "resolution": {
            "lexical": "A definition or import visible by static name. Runtime rebinding can change the target.",
            "attribute_name_only": "Candidate methods share the attribute spelling. Receiver type is unknown.",
            "unresolved": "No suitable project target. Review the expression and code for external, builtin or dynamic calls.",
        },
        "files": mapped,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if the checked-in inventory is stale.")
    args = parser.parse_args()
    generated = json.dumps(build(), ensure_ascii=False, indent=2) + "\n"
    if args.check:
        if not OUTPUT.is_file() or OUTPUT.read_text(encoding="utf-8") != generated:
            parser.exit(1, "Tech map is stale. Run python tools/generate_techmap.py.\n")
        return
    with OUTPUT.open("w", encoding="utf-8", newline="\n") as output:
        output.write(generated)


if __name__ == "__main__":
    main()
