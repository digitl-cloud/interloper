# The components store as a package

Step 2b of the interloper-db / interloper-api simplification, after the run lifecycle (#427).

## Problem

`store/components.py` was the largest store module (1243 lines, one 1125-line class). It held three concerns:
- the rows (CRUD, write rules, state writes);
- what a row reads as in this deployment (status, decoded config);
- turning a row into a live framework component.

Two neighbouring modules each held half of one concern: `store/hydration.py` (spec building, decryption) and `store/status.py` (status as free functions).

Catalog questions sat in the store, and one was answered twice. Components' job granularity and insights' coverage each resolved a row's partitioning, under different drift rules.

The catalog lookup took `(key, parent_key)`, a database shape. The framework itself names an owned asset by its qualified key (`source.asset`): `Component.qualified_key`, relation declarations, `ComponentIdentity`.

## Decisions

1. **Two concepts, two modules.** `store/components/` is a package:
   - `base.py`, `ComponentStore`: rows and what one reads as. CRUD, delete guards, write rules, state writes, job granularity, and `read(row) -> ComponentReading`. It also holds `ComponentStatus`.
   - `hydration.py`, `Hydrator`: live components. The `load` path, spec building, and the drift checks it fails closed on.

   `store.components.load` delegates to the hydrator. `status.py` and the old modules are gone.
2. **The descriptive status stays with the row.** `read` reports `ok`, `disabled`, `missing` or `unreadable`. Hydration only needs a yes or no ("does this key resolve here?"), so it asks the catalog directly, and its drift errors no longer distinguish disabled from missing. That detail is what `read` reports.
3. **Qualified keys everywhere a definition is looked up.**
   - `Catalog.get(key)` takes a bare key or `source.asset`. A qualified key resolves only through a source that declares the asset; the flat fallback is gone. `Catalog.vocabulary(kind, key)` follows. `parent_key` leaves both.
   - The row model derives `qualified_key` from its loaded parent (listings select-in load it, children's parents included). It replaces `Component.parent_key(session)` and every `parent_key=` threaded through the store, insights and the API.
4. **The class declares, the row supplies.** `Component.public_fields()` sits next to `discriminator_field()`: the class names which fields are public and which one discriminates, and `read` takes their values from the decoded payload. No `..._of(config)` methods, and no config class.
5. **One partitioning resolution.** `SourceDefinition.partitionings()` with `AssetDefinition.partitioning`, reached through `Catalog.get(row.qualified_key)`. Components' job granularity and insights' coverage share the strict owned-asset rule.
6. **The payload codec belongs to the row.** `Component.write_config(config, *, encrypt, encrypted)` and `Component.read_config(decrypt)` replace the encrypt half in the store and the decrypt half in the hydrator, so neither module depends on the other.

## Behaviour changes

- A qualified key whose source does not declare the asset resolves to nothing, where the flat fallback could find an unrelated standalone asset of the same key.
- An owned asset under a disabled source reads `missing` when the source no longer declares it (before: `disabled`, inherited from the source).
- Hydration's drift errors say the key "does not resolve in this deployment" instead of naming disabled or missing.
- An undecryptable payload fails hydration with the codec's error ("Failed to decrypt component ...").

## Size

Source net −133 lines (core +22, db −150, api −5); tests −68. `components/base.py` is 960 lines and `components/hydration.py` 462.
