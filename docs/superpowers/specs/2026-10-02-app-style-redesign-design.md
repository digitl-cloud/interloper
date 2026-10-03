# App style redesign

Bring the interloper app to a consistent dashboard style (reference: the Nuxt UI dashboard
template look) and, on the way, return the frontend to default Nuxt UI components and layouts
wherever our bespoke chrome only re-implements them.

## Principles

- **Two surface tones, measured from the reference.** The sidebar and every container share one
  tone; the panel is lighter. Every element of the main content lives in a container; nothing floats on the panel.
- **One page skeleton.** Navbar (title or crumb), optional toolbar (views left, page actions
  right), body of cards.
- **One card shape.** Header inside the card: title + one-line description on the left, the
  section's controls on the right, no divider. Tables always sit in a card, borderless, with their
  search, filters and bulk actions in the card header and the pager in the card footer.
- **Defaults first.** Use Nuxt UI components, slots and default spacing; keep only what is truly
  ours (colors, fonts, graph tokens, id-chip, wizard drawer, stepper, pill tabs).

## 1. Tokens and theming

Neutrals are Tailwind `neutral` (pure greys, sampled from the reference screenshots); the accent stays blue.

| Surface | Token | Light | Dark |
|---|---|---|---|
| Sidebar and containers | `--ui-bg-muted` | `#fafafa` | `#171717` |
| Panel | `--ui-bg` | `#ffffff` | `#1c1c1c` |
| Line | `--ui-border` | `#e5e5e5` | `#262626` |
| Body text | `--ui-text` | `#0a0a0a` | `#f5f5f5` |
| Muted text | `--ui-text-muted` | `#525252` | `#a3a3a3` |

- Active states stay low-contrast: the sidebar's active entry is Nuxt UI's `bg-elevated` pill with
  the line ring, and the active pill tab takes the panel tone on the grey track. A hub highlights
  only while its views are hidden; otherwise its active view does.
- `app.config.ts`:
  - `card`: outline variant = container tone + ring, no header/body divider.
  - `table`: borderless globally (no outer frame, no header band): rows sit on the card tone,
    divided by the line color, first and last columns flush with the card padding
    (`first:ps-0 last:pe-0`). Tables outside a card (run events) set their own sticky tone.
  - `dashboardSidebar` (root and mobile `content`) uses `bg-muted`.
- Kept bespoke: color scales, Inter + JetBrains Mono, graph tokens, `id-chip`, scrollbar, wizard
  drawer, stepper, pill-tabs compound, colored soft status badges.

## 2. Primitives

- **Sections are plain `UCard`s.** `<UCard title description>` uses the card's own header. A
  section with controls overrides `#header` with **`CardHeader`** (new,
  `components/ui/CardHeader.vue`): `title` and `description` on the left, default slot for the
  controls on the right, rendered with the card theme's own `title` / `description` classes so
  both forms look identical. A card whose body is rows (settings, key/value, activity lists) takes
  its `CardHeader` *above* the card instead, in a `<section class="flex flex-col gap-3">`: a title
  inside such a card reads as its first row.
- **`DataTable`**: always rendered inside a card.
  - Card header: search then `#filters` on the left; bulk delete then `#actions` (the list's main
    action, e.g. "New source") on the right.
  - Card footer: `TableFooter` (row count + pager), which loses its rule and `--launcher-inset`
    padding. The other table components (runs, backfills, collection, backfill detail) take the
    same card shape: their search/filter row in `#header`, `TableFooter` in `#footer`.
  - `FILL_CARD_UI` (`utils/card.ts`) holds the fill classes, shared by every single-table page.
  - `fill` prop for single-table pages: the card stretches to the panel, header sticky, pager
    pinned. Without it the card flows with the page.
  - `bordered` is removed: every table is the same borderless table.
- **KPI tiles**: the overview's `HealthTile` becomes a plain `UCard` in the reference shape
  (label, corner badge, number, caption). Overview-local, no shared wrapper.
- **Typography**: hand-tuned pixel sizes (`text-[13.5px]` …) move to the Tailwind scale as files
  are touched; one size is kept for KPI numbers. The eyebrow page header goes; `.eyebrow` stays
  only where it labels something (separators).

## 3. Skeleton and chrome

- **Layouts** (`default`, `admin`, `settings`, `agent`): `UDashboardGroup` + `UDashboardSidebar`.
  Only `default` also mounts `UDashboardSearch` and `AgentPanel`. No panel, no navbar, no
  meta-driven page header.
- **Sidebar**: header = logo; top of nav = `UDashboardSearchButton` (default layout); footer = org
  switcher, then user menu (version moves into the user menu).
- **Pages** each render a `UDashboardPanel`:
  - `#header`: `AppNavbar` (title, or crumb + status on detail pages via its `#title` slot), then
    an optional `UDashboardToolbar` with the hub's `UNavigationMenu` left and page actions right.
    A list's main action lives in its table card header, not the toolbar. Other page actions live in
    the toolbar's `#right`, never in the navbar; filters and view controls sit on the toolbar's
    `#left`. A page with neither views nor toolbar actions renders no toolbar.
  - `#body`: the panel's default padding and gap, full width.
- **`AppNavbar`** (new, thin wrapper over `UDashboardNavbar`): collapse button leading, agent
  toggle trailing, page content through the navbar's own slots. Avoids repeating both on every page.
  The agent pages render no `AppNavbar` (as before this redesign).
- **In-page views** (authentication tabs, admin organisation tabs) are routed through `?tab=` and
  rendered as a highlighted `UNavigationMenu` in the toolbar, like the hub views.
- **Form pages** (profile, admin config) cap their column at `max-w-3xl`, centered.
- **Timeline**: a card with the window's run breakdown by status (`ExecutionsRunStatusBar`, shared
  with run detail; its chips filter the bars), then a full-height canvas card (`CANVAS_CARD_UI`,
  `utils/card.ts`) whose header carries the window controls. Run bars keep their solid status fill:
  neutral blocks with a status edge broke down on dense lanes (adjacent runs chained into pills,
  short runs shrank to grey capsules).
- **Graph**: a row on the page body (`GraphToolbar`: status and group-by pill tabs left, main
  action right; no toolbar strip) above a header-less full-height canvas card. The asset side
  panel (`GraphAssetPanel`) is its own card, 12px from its neighbour across a thin resize handle,
  and slides in from the right: `useSlidingPane` (`composables/slidingPane.ts`) tweens the splitter
  pane's size so the neighbouring card resizes in step, with the content held at its open width
  while it moves; collection shows the same card beside its table card.
  Its sections (description, materialization, destinations, …) are inner cards in the panel tone
  (`bg-default` + line ring) on the card's container tone, and their item tiles drop back to the
  container tone: the two tones alternate at every level.
- **Run detail**: a summary card (meta line + `ExecutionsRunStatusBar`), then the resizable,
  collapsible rail card (Attempts / Assets as two-tone inner section cards) beside a vertical split
  of the timeline/graph canvas card (rail toggle + view tabs in its header) and the events card
  (category tabs + count in its header). The splitter handles are the 12px gaps between cards.
- **Split pages** (collection, agent chat) zero the body padding
  (`:ui="{ body: 'p-0 sm:p-0' }"`, plus the gap where the body stacks) so their splitters run
  edge to edge; any card sits inside its own padded pane.
- **Removed**: `NavActions`, `PageFrame`, `PageNav`, `PageTabs`, `PanelCard`, `OverviewSection`,
  `PageBreadcrumb` (already unused), the floating agent launcher and `--launcher-inset`, the `pageHeader` /
  `fullBleed` / `customNavbar` route meta, the `#navbar-title` / `#navbar-right` teleports, the
  hand-built command palette modal (replaced by `UDashboardSearch`).

## 4. Scope and verification

- One branch (`feat/app-style-redesign`), one PR carrying this spec.
- Order: foundation (sections 1 to 3), then hub by hub: Components, Executions, Overview,
  Collection / Graph / Timeline, Organization / Settings, Admin, agent, auth and
  onboarding pages. Each old primitive is deleted with its last user.
- Out of scope: the sidebar `inset` variant (exists on `USidebar` only, not `UDashboardSidebar`;
  adopting it later means a sidebar swap or emulation) and chart restyling.
- Verification: `make check-typescript`; headless before/after screenshots of every page in light
  and dark on a `:3100` dev instance.
