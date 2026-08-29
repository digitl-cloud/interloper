---
paths:
  - "**/*.py"
---

# Python style

How Python is written in this repo, beyond what ruff and ty enforce. These rules are binding for all produced code.

## 1. Follow the Zen of Python (PEP 20, Tim Peters)

```
Beautiful is better than ugly.
Explicit is better than implicit.
Simple is better than complex.
Complex is better than complicated.
Flat is better than nested.
Sparse is better than dense.
Readability counts.
Special cases aren't special enough to break the rules.
Although practicality beats purity.
Errors should never pass silently.
Unless explicitly silenced.
In the face of ambiguity, refuse the temptation to guess.
There should be one-- and preferably only one --obvious way to do it.
Although that way may not be obvious at first unless you're Dutch.
Now is better than never.
Although never is often better than *right* now.
If the implementation is hard to explain, it's a bad idea.
If the implementation is easy to explain, it may be a good idea.
Namespaces are one honking great idea -- let's do those more!
```

Every rule below is an application of these lines to this codebase. When a situation is not covered by a specific rule, decide by the Zen — in this repo the load-bearing lines are *simple is better than complex*, *flat is better than nested*, *special cases aren't special enough to break the rules*, and *errors should never pass silently*.

## 2. Explicit names, no abbreviations

Write names out in full: `context` not `ctx`, `config` not `cfg`, `connection` not `conn`, `response` not `resp`, `request` not `req`, `value` not `val`, `index` not `idx`, `database` not `db`. This applies to variables, parameters, attributes, functions, and modules alike. A name is read far more often than it is typed; never trade readability for keystrokes.

Exceptions, all conventional Python rather than abbreviation:

- `*args` / `**kwargs`
- `i`, `j` as bare loop/comprehension counters
- `_` for intentionally unused values
- `df` for a DataFrame in pandas-facing code
- when the explicit word is reserved or effectively so — a Python keyword, a builtin, or a keyword in the wider ecosystem (`function`, `object`, `class`, `type`, `id`, `input`): prefer a more specific word (`target`, `member`, `owner`, `handler`), or use the established short form (`fn`, `obj`, `cls`). Never a trailing-underscore hack (`type_`).

## 3. Don't over-comment

The default is no comment — explicit names (rule 2) and clear structure carry the meaning. A comment earns its place only when it carries information the code itself cannot: a non-obvious why, an invisible constraint (protocol quirk, vendor behavior, ordering requirement), or a trap that would catch the next reader. If the code is merely effortful to read, improve the code instead of annotating it. One or two lines, on the exact code it explains.

One additional permitted form: a short **section label** that names what a block of code is doing, to surface the structure or sequence of a longer function (`# resolve upstream assets`, `# stamp partition column`). Optional, and not to be overused — a function that needs many section labels usually needs restructuring instead.

Never:

- attribute/field-level comments: no `#:` or trailing comments on pydantic/SQLModel fields — the design rationale lives in ONE place (module docstring, migration docstring, PR description), not mirrored per field
- comments that restate what the next line does
- comments addressed to the reviewer: narrating a change, justifying it, or saying where code came from

When in doubt, delete.

## 4. Section dividers

The house format for structuring a long module or class is the dash-padded divider, always dash-filled to column 80 (total line length, indentation included):

```python
# -- Registry ------------------------------------------------------------------

    # -- Construction ----------------------------------------------------------
```

- Module level: group top-level definitions. Class level (indented): group methods by concern.
- Titles are short capitalized noun phrases. Reuse the established vocabulary before inventing a synonym: `Construction`, `Identity`, `Serialization & resolution`, `Definition`, `Introspection`, `Reconfiguration`, `Public API`, `Internals`.
- This is the only banner style — no `====`, `####`, or boxed variants.
- Same restraint as rule 3's section labels: dividers earn their place in files and classes long enough to need a map. A class with a handful of methods doesn't get them.

## 5. Modules live in packages

Every concept gets a package (a directory with `__init__.py`), never a loose module file next to its siblings. Inside the package:

- `base.py` holds the core logic the package is named for: package `asset` → `Asset` in `asset/base.py`, package `source` → `Source` in `source/base.py`.
- Split ancillary logic into sibling modules whenever there is a real seam of responsibility (`decorator.py`, `context.py`, `fields.py`, `ref.py`) — one responsibility per module, split by seam, not by line count.
- `__init__.py` re-exports the package's public surface with an explicit `__all__`. The package is the import surface: import from it, and reach into a submodule directly only within the package itself or to break an import cycle.
- **`__init__.py` defines nothing.** It holds a module docstring, imports and `__all__` — no classes, no functions, no module-level state. A definition there has no module of its own to be found in, cannot be imported without pulling in every sibling re-export, and hides from the file layout: the package's own core class belongs in `base.py`, not in the file that re-exports it.

## 6. Logic has an owner

The default home for behavior is a method on the class whose data it reads or whose invariants it maintains. Before writing a module-level function, name its owner; if the owner is a class, the function is a method (private `_` method if internal). If the function's body mostly touches one class's fields, it belongs to that class — wherever it currently sits. Code that parses or builds an `X` is a constructor in disguise: `X.from_*(...)` classmethod, not a free function.

A module-level function is the exception and must be one of:

- the module's **public entry point** where the concept genuinely is a function: decorators (`@asset`, `@source`), field factories (`InputField`), CLI command wiring, procedural init over global state (`init_telemetry`)
- a **pure, type-agnostic utility** — which lives in `utils/`, never at the bottom of a domain module
- a **dispatch-table entry or callback** passed by contract (`RECONCILERS`, registry adopt hooks)
- **forced to module level by the runtime**: pickled by a process pool, referenced by an entry point

Never park logic in a `*Utils`/`*Helper` class or a bag of staticmethods — that's the junk drawer wearing a class.

Accumulation is a signal, not a storage problem: when helpers multiply and share the same arguments, that argument cluster is a class waiting to be born — introduce the type instead of the third helper. The acceptance test is the glance: a module body reads as constants, one or a few classes, and almost nothing else.

## 8. Constructors live on the type

A function whose job is to build an `X` is a constructor, and belongs on `X` as a classmethod — never a free function named after the type it returns.

```python
# no — a constructor hiding as a module function
def destination_response(destination: Component) -> DestinationResponse:
    return DestinationResponse(id=destination.id, ...)

# yes — the type owns the ways it can be built
class DestinationResponse(BaseModel):
    @classmethod
    def from_destination(cls, destination: Component) -> DestinationResponse:
        return cls(id=destination.id, ...)
```

The tell is a return annotation naming a type you own plus a body that ends in `return X(...)`. It covers every convert/build/render helper: response models built from database rows, specs parsed from files, domain objects assembled from settings.

Name the classmethod `from_<source>` — after what it converts, not what it returns: `RunResponse.from_run(run)`, `DAG.from_paths(paths)`, `Event.from_log_line(line)`. When a type can be built several ways, the suffix is what tells them apart. Reserve a plain `build`/`make` prefix for the case where there is no single source to name.

Two things are not constructors in disguise:

- a **framework entry point** that happens to return a model — a FastAPI route handler is an endpoint, and its response type is incidental
- construction of a type you do **not** own, where the classmethod cannot live on it; keep those as functions, close to the code that needs them

## 7. Docstrings are complete and uniform

Every module, class, function, and method gets a Google-style docstring — private helpers, `__init__`, and dunders included. A docstring always carries **all** the sections that apply to its signature, no exceptions:

- `Args:` whenever there are parameters (every parameter listed — D417 enforces completeness)
- `Returns:` whenever a value is returned (DOC201)
- `Yields:` for generators (DOC402)
- `Raises:` for every exception raised (DOC501)

Uniformity outranks brevity here: the reader must find the same structure on every function, so a short entry that mostly restates the parameter is still written — completeness is the point, and a missing section reads as an unfinished docstring, not as intentional minimalism. Sections must stay accurate (DOC202/403/502 guard extraneous entries); make each entry as informative as it can be — defaults, None semantics, units, shapes — rather than a bare re-noun when there is something to say.

Mechanics, all verified against the linter:

- `Args:` lists **every** parameter, `self`/`cls` excluded, `*args` and `**kwargs` included (written as `*args:` / `**kwargs:`). D417 rejects an omission, DOC102 rejects a name that is not a real parameter.
- `Raises:` documents only exceptions raised by an explicit `raise` **in the function's own body**. DOC502 rejects one that comes from a callee (`response.raise_for_status()`, `model_validate()`), and a bare `raise` re-raise cannot be documented at all.
- `@overload` stubs carry no docstring — the implementation holds it.
- Functions nested inside another function body are exempt; the enclosing function's docstring covers them.
- Note that the linter cannot catch a **missing** `Args:` section (ruff implements no such rule) — only an incomplete one. Writing the section is on you.

The summary line still matters most: one line, imperative, stating the contract. Prose paragraphs between the summary and the sections carry the why and the caveats, same bar as rule 3.
