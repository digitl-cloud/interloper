---
paths:
  - "packages/platform/interloper-app/app/**"
---

# App design

How the interloper app's UI is built. These rules are binding for all code under `packages/platform/interloper-app/app/`.

**Where the theme lives**
- `app/app.config.ts`:
  - `ui.colors`, `ui.icons`
  - component theme overrides
  - the app's concept icons under `icons`
- `app/assets/css/main.css`:
  - `@theme` fonts and blue scale
  - `--ui-*` tokens
  - the graph's `--graph-*` tokens

Lint enforces the class rules (`vue/no-restricted-class` in `eslint.config.mjs`); everything lint cannot see (`:ui` objects, class strings in script, app.config) follows the same rules by review.

## 1. Nuxt UI first, customised declaratively

Use the Nuxt UI component for every job it covers. Change its look on the first rung that does the job:

1. **Props:** `color`, `variant`, `size`, `icon`, `orientation`, `loading`, `actions`, ...
2. **`app.config.ts` `ui.<component>`:** `slots`, `variants`, `compoundVariants`, `defaultVariants`. These apply to every instance.
3. **Tokens:** `@theme`, `--ui-*`, `ui.colors`, `ui.icons`, `icons`.
4. **`UTheme`** around a subtree that needs other component defaults.
5. **Per-instance `:ui` / `class`, for layout only:** width, placement, flex and grid, overflow. Never colour, type, radius, shadow or the component's own padding.
6. **Own components.**
   - They compose Nuxt UI components and add domain meaning (`StatusBadge`, `EntityBadge`, `DataTable`, `LoadError`, `PageTabs`). They never re-implement a Nuxt UI component.
   - New visuals only where Nuxt UI has none: the graph canvas, the execution timeline, the coverage calendar and strips. They use the tokens and scales below.

The reference for "default" is the Nuxt UI dashboard template.

## 2. Colour

**Semantic colours only:** `primary`, `success`, `info`, `warning`, `error`, `neutral`, as component `color` props or as `text-*`, `bg-*`, `border-*`, `ring-*` utilities, plus the neutral roles below. Never:
- a palette class (`text-emerald-700`, `bg-gray-400`)
- a hex value
- `text-white` / `text-black` (use `text-inverted`)
- `bg-[var(--ui-success)]` where a utility exists (`bg-success`)

**Text roles**
- `text-highlighted`: titles and primary values
- body copy: inherits `text-default`
- `text-muted`: secondary text and descriptions
- `text-dimmed`: tertiary meta (timestamps, counts, placeholders, the empty-cell dash)

**Surfaces**
- `bg-default`: the panel
- `bg-muted`: the card tone (cards, sidebar)
- `bg-elevated`: hover and insets inside a card
- `bg-accented`: tracks

**Tints:** `/10` for soft fills, `/50` for hover rings. Nothing else.

**Edges:** `ring ring-default` around containers you draw; `border-default` only for dividers (`border-t`, `border-b`, `divide-y`).

**Colour in script.** Use CSS expressions (`var(--ui-success)`, `color-mix(...)` of tokens). Canvas contexts (ECharts) resolve them at runtime through `useChartTheme()` / `resolveColor()`. Never keep hex mirrors of tokens.

**Dark mode** comes from the tokens. Components never write `dark:` variants. A `--ui-*` token overridden under `:root` is restated under `.dark`: both selectors weigh the same, so the later light value would otherwise win in dark mode too.

## 3. Typography

**Family.** Inter, the only family. `font-mono` is for code only:
- error messages and tracebacks
- logs
- JSON, SQL and cron, including the inputs that hold them
- code in agent answers

Never for ids, names, numbers, dates or captions.

**Sizes.** Tailwind steps only, one per role:
- `text-xs`: meta and helper text; captions take it through `caption`
- `text-sm`: body; the default for content
- `text-base`: a heading outside any component title
- `text-2xl`: the heading of a standalone page (sign-in, welcome, invite, the agent's start)
- `text-3xl`: KPI numbers

Titles of cards, page cards, modals, drawers, empty states and the navbar take the size their Nuxt UI slot ships. No arbitrary sizes.

**Weights**
- `font-normal`: body
- `font-medium`: labels and names
- `font-semibold`: titles and emphasised values

No `font-bold`.

**Captions.** A section's label, the name over a block ("Input", "History", a list's group header, a separator's label, a sidebar's portal badge), is the `caption` class from main.css: `text-xs`, `font-medium`, wider tracking, capitals. It takes a text colour, `text-muted` or a state colour, and nothing else that sizes it. It is the app's only uppercase and only letter-spacing: templates never write `uppercase` or `tracking-*`. A label beside its control ("Status", "Window") is not a caption: `text-sm text-muted`. The source writes a caption in sentence case, like every other label.

**No** line-height utilities except on text clamped in a fixed box.

**Numbers, durations and times** use `tabular-nums`. Machine ids add `slashed-zero`.

## 4. Spacing, radius, size

**Spacing.** Tailwind scale only. Our own markup uses 0.5, 1, 1.5, 2, 3, 4, 6, 8 and 12 for gaps, padding and margins. Nothing arbitrary.

**Page rhythm**
- `UDashboardPanel` pads the body.
- Sections stack at `gap-4 sm:gap-6`.
- A component's internal padding is its own; our markup only places it.

**Radius**
- `rounded-md`: small things (chips, tiles, swatches)
- `rounded-lg`: containers
- `rounded-full`: dots, avatars, bars
- graph cards: `rounded-xl` / `rounded-2xl`

No arbitrary radii.

**Icon sizes**
- `size-3.5`: next to `text-xs`
- `size-4`: next to `text-sm`, and in rows
- `size-5`: leading icon of a list item, tile or panel header
- `size-6`: inside an icon tile

Buttons, badges and empty states size their own icons.

**Widths**
- data pages: full width
- settings and forms: one column, `max-w-2xl mx-auto`
- auth pages: `max-w-sm`
- the chat: `max-w-3xl`
- drawers: one width, from the theme
- modals: Nuxt UI's widths

**Elevation.** Cards are flat. Overlays keep the shadows Nuxt UI ships. The graph keeps its own hover lift. Nothing else casts a shadow.

## 5. Icons

- **Sets.** Lucide (`i-lucide-*`). Brand marks come from the `icon:` collection (`assets/icons/`, hyphenated names, 64×64 canvas, official artwork only).
- **Generic concepts** use `appConfig.ui.icons`: `loading`, `success`, `error`, `warning`, `info`, `close`, `external`, `reload`, `copy`, `ellipsis`, `search`, `plus`, `chevronDown`, ...
- **Domain concepts** use `appConfig.icons`: `source`, `destination`, `connection`, `job`, `hook`, `asset`, `run`, `backfill`, ...
  - A concept has one icon, and an icon means one concept.
  - A concept used in two places goes in the map; never write its literal twice.
- **Spinner:** `appConfig.ui.icons.loading` with `animate-spin`, or a component's `loading` prop.

## 6. Status

One table, `utils/status.ts`. `statusMeta(status)` gives the label, colour, icon and whether the status is still executing, for every run, backfill and execution status. Everything derives from it:
- `StatusBadge`: a soft UBadge that spins while executing
- `StatusDot`
- progress segments
- timeline and chart colours
- legends

| status | colour |
|---|---|
| success | success |
| failed | error |
| running, dispatched | info, executing |
| queued, pending | neutral |
| canceled | warning |
| skipped | neutral |

Graph node states (`idle`, `attention`, `paused`, ...) map onto the same colours in `composables/nodeStatus.ts`.

## 7. Components by job

| Job | Use |
|---|---|
| Page frame | `UDashboardPanel`; `#header` is `AppNavbar`: Nuxt UI's default navbar (its height and bottom border, no theme override), the same on every page (sidebar toggle, title, search, agent toggle) and never given actions. Detail pages put a `UBreadcrumb` in its `#title`. `UDashboardToolbar` holds sub-page navigation (`PageTabs`) on its left and a detail page's own actions (cancel, retry, open) on its right; the theme makes it the first row of the page's content (no rule, the content's top spacing). |
| Where an action goes | With the card it acts on: a list's create action in `DataTable` `#actions`, a canvas's in its toolbar's `#actions` (CollectionTable, GraphToolbar), a card's in its `CardHeader`. A page with no data repeats its create action in its `UEmpty`. |
| Section header above a card | `UPageCard variant="naked" orientation="horizontal"`: title, description, actions in its default slot |
| Header inside a card | `CardHeader` in the `UCard #header` |
| Section label | `caption text-muted` on the label; `USeparator label` and navigation `type: 'label'` items take it from the theme |
| Container | `UCard` (the card tone) |
| Settings and key/value rows | `UFormField orientation="horizontal"` rows separated by `USeparator`, inside a `UCard`; a read-only value is the field's content |
| List of records | `DataTable`. A raw `UTable` only inside a card that is not a list page (detail sub-tables, overview cards). |
| Status | `StatusBadge`; `StatusDot` where a badge does not fit |
| Entity reference | `EntityBadge` (outline neutral). Outline is reserved for it. |
| Tags, counts, kinds, ids beside a name | `UBadge color="neutral"` (soft by theme), `size="sm"` beside text |
| Maturity | `MaturityBadge` |
| Empty: no data on a page | `UEmpty variant="outline"` with `actions` |
| Empty: inside a card or panel | `UEmpty variant="naked" size="sm"` |
| Empty: inside a wizard step | `UEmpty variant="subtle" size="sm"` with `actions` |
| Loading, first load | `USkeleton` shaped like the content where the layout is known (`withSkeletons` for tables); otherwise one centred spinner, `appConfig.ui.icons.loading` at `size-5 animate-spin text-dimmed` |
| Loading, refetch | the component's `loading` prop |
| Failed load | `LoadError` (error UAlert with Retry). Never only `console.error`. |
| Failed action | `toast.add(errorToast(error, 'Couldn't …'))` |
| Primary action | `UButton`, theme default (solid primary) |
| Secondary action | `color="neutral" variant="outline"` |
| Utility and icon-only action | `color="neutral" variant="ghost"`. Icon-only buttons always carry `aria-label`. |
| Destructive action | `color="error"`: solid in the confirm dialog, `variant="soft"` elsewhere |
| Inline text action | `variant="link"` |
| Button size | the theme default for toolbar, table-toolbar and overlay actions; `size="sm"` for actions beside a card title (`CardHeader`) and in rows |
| Sub-pages of a hub | `PageTabs`: `UNavigationMenu variant="pill"` in the toolbar, themed in app.config to the same segmented look as the pill `UTabs` (real links, so pages open in new tabs and prefetch) |
| Views inside a card | `UTabs variant="pill" size="xs" :content="false"` |
| Form | `UForm` + `UFormField` (`label`, `description`, `required`, `error`) for every field; no hand-built labels or error text. Vertical in modals and drawers, horizontal in settings. Inputs are `w-full` in vertical forms. |
| Confirm | `useConfirm()` (ConfirmModal) |
| Short form | `UModal`; footer right-aligned by the theme: Cancel (neutral outline), then the primary button |
| Multi-step or long form | right `UDrawer` (`WizardDrawer`), with its `close` prop |
| Secret shown once | `CopyField` in an overlay with `:dismissible="false"` until it is closed with its button |
| Meter | `UProgress`; a split bar is `UProgressGroup` with segments from the status table |
| Avatar | `UAvatar` with initials; no colour palette |
| Split panes | `USplitter` |
| Tooltip | `UTooltip`, never `title`. Data-viz tooltips set `:delay-duration="0"`. |
| Chart | ECharts with `useChartTheme()` (family, sizes and colours resolved from the tokens) |
| Toast | success: `{ title: 'Source created', color: 'success' }`; errors through `errorToast` |

## 8. Copy

- **Sentence case** for every label, title, button, menu item, column header and toast: "New source", "Create destination", "Exit admin".
- **British spelling**, as the codebase: "Organisation". Route paths keep their slugs.
- **Ellipsis** is "…" (U+2026), never "...": "Search…", "Loading…".
- **Verbs**
  - "New X" opens a creation flow; "Create X" submits it.
  - "Save" saves edits; "Delete" deletes; "Cancel" dismisses.
  - "Run now" runs; "Retry" re-runs or reloads.
- **Dates and numbers** go through `utils/time.ts` only, which honours the display time zone:
  - absolute: `formatDate` / `formatDay` / `formatDayTime`
  - relative: `relativeTime` ("2h ago")
  - durations: `formatElapsed`
- **Ids:** `shortId()` everywhere a UUID is shown; the full id goes in a tooltip or a copy action.

## 9. Bespoke areas

**The graph** (`components/graph/`) and **the agent panel** (`components/agent/`) keep their own layouts, but follow every rule above:
- type, spacing and radius scales
- semantic colours and the status table
- icon maps

The agent panel slides in from the right below the navbar, which keeps the full width; the page's toolbar and body make room for it: the theme marks them `agent-room`, and the panel sizes `agent-room`, itself and its handle from a small stylesheet it owns. A resize then re-renders nothing and restyles only those few elements; no size that changes on every pointer move goes into a template binding, a cookie or a variable on a shared ancestor. The panel sits `--agent-inset` (the page body's own padding) from the navbar and the window's edges, so it lines up with the page's content; inside, it is padded like a card (`p-4 sm:px-6`). The graph's own colours and shadows are `--graph-*` tokens in main.css, defined from the palette, never inline. The agent's code blocks and tool payloads are the code that `font-mono` exists for.
