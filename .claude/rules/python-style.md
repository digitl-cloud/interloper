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
