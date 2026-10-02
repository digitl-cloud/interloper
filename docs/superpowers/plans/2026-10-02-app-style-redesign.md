# App Style Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restyle the interloper app to one consistent dashboard style (three surface tones, panel per page, every section and table in a card) while returning the frontend to default Nuxt UI components.

**Architecture:** Theme tokens and `app.config.ts` carry the look (card, framed table, sidebar tone). Layouts shrink to `UDashboardGroup` + sidebar; every page renders its own `UDashboardPanel` with `AppNavbar` and an optional `UDashboardToolbar`. Sections are plain `UCard`s (with `CardHeader` when they carry controls); tables are cards with search/filters in the header and `TableFooter` in the footer.

**Tech Stack:** Nuxt 4, Nuxt UI 4.11 (`~4.11.1`), Tailwind CSS 4, TanStack Table via `UTable`, Pinia.

**Spec:** [docs/superpowers/specs/2026-10-02-app-style-redesign-design.md](../specs/2026-10-02-app-style-redesign-design.md)

## Global Constraints

- All app paths below are relative to `packages/interloper-app/app/app/` (written `APP/`). Frontend commands run from `packages/interloper-app/app/`.
- Surface tokens, verbatim: sidebar `#f4f4f5` / `#0c0c0e`; panel `#ffffff` / `#18181b`; container `#fafafa` / `#131316`; line `#e4e4e7` / `#27272a` (light / dark).
- Kept bespoke, never removed: color scales, Inter + JetBrains Mono, graph tokens, `.id-chip`, scrollbar, wizard drawer theme, stepper theme, pill-tabs compound, colored soft status badges.
- `@nuxt/ui` stays pinned `~4.11.x` (see memory: 4.9+ tiptap peers already handled; do not bump).
- Buttons in navbars and toolbars take no `size` prop (app.config default). Section links ("All jobs", "Open collection") become `UButton color="neutral" variant="outline" size="sm" :to>` in the card header.
- Typography: every file a task touches drops hand-tuned pixel sizes for the Tailwind scale: ≤12.5px → `text-xs`, 13–14.5px → `text-sm`, 15–16px → `text-base`, 17–20px → `text-lg`, 22–24px → `text-2xl`; KPI numbers use `text-3xl font-semibold tabular-nums`. Graph (`components/graph/`), chart (`components/chart/`) and wizard internals are not touched.
- `bg-(--ui-bg-band)` becomes `bg-elevated/50` wherever it appears.
- Comments: sparse, only a non-obvious *why* scoped to the code it sits on (AGENTS.md). Component doc comments in the existing `/** … */` style.
- Commits: each task ends with one local WIP commit on `feat/app-style-redesign` (Conventional Commit subject `feat(app): <task summary>`, message ending with the line `By Digitl`). Never push, amend or rebase; the controller squashes the branch into one commit before hand-off (one PR carrying the spec and this plan).
- No frontend unit-test runner exists. Every task's test cycle is: `pnpm run lint`, `pnpm exec nuxt typecheck`, then headless screenshots of the touched pages in light and dark via the `/verify` skill on a `:3100` instance (`INTERLOPER_SERVER_PORT=3100 make dev-up`), compared against the Task 0 baseline.

## Page skeleton reference

Every migrated dashboard page has exactly this shape (fill in the per-page values the task gives):

```vue
<template>
    <UDashboardPanel id="<page-id>">
        <template #header>
            <AppNavbar title="<Title>" />
            <!-- only when the page has views or actions -->
            <UDashboardToolbar>
                <template #left>
                    <UNavigationMenu :items="<views>"
                                     highlight
                                     class="-mx-1 flex-1" />
                </template>
                <template #right>
                    <!-- page actions -->
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <!-- cards; overlays (drawers, modals) last -->
        </template>
    </UDashboardPanel>
</template>
```

- A page without views but with actions keeps the toolbar, with only `#right`.
- A page with neither omits the toolbar.
- `definePageMeta` loses `title`, `pageHeader`, `fullBleed` and `customNavbar`; keeps `layout`, `middleware`, `validate`, `redirect`, `orgSwitchTarget`.
- Canvas pages pass `:ui="{ body: 'p-0 sm:p-0 gap-0 sm:gap-0' }"` to `UDashboardPanel`.

---

### Task 0: Baseline screenshots

**Files:** none (output to the session scratchpad).

- [ ] **Step 1:** Start the instance: `INTERLOPER_SERVER_PORT=3100 make dev-up` (repo root, background). Reuse the `:3000` session per AGENTS.md.
- [ ] **Step 2:** Using the `/verify` skill, screenshot every route below in light and dark at 1440×900, saving as `baseline/<route-slug>-<theme>.png`:
  `/`, `/timeline`, `/graph`, `/collection`, `/components/sources`, `/components/destinations`, `/components/connections`, `/components/jobs`, `/components/hooks`, `/executions/runs`, one `/executions/runs/<id>`, `/executions/backfills`, one `/executions/backfills/<id>` (if any exist), `/organization`, `/settings/profile`, `/settings/authentication?tab=signin`, `/settings/authentication?tab=tokens`, `/admin`, `/admin/organisations`, one `/admin/organisations/<id>` for each `?tab=` value, `/admin/users`, `/admin/config`, `/agent`, `/login` (logged out).
- [ ] **Step 3:** Record the route list with the ids used in `baseline/ROUTES.md` so every later task re-shoots the same URLs.

---

### Task 1: Theme foundation

**Files:**
- Modify: `APP/assets/css/main.css` (surface tokens, both themes)
- Modify: `APP/app.config.ts` (`card`, `table`, `dashboardSidebar`, `navigationMenu`)
- Create: `APP/components/ui/CardHeader.vue`
- Create: `APP/utils/card.ts`
- Modify: `APP/components/ui/TableFooter.vue`

**Interfaces:**
- Produces: `<CardHeader title description?>` with a default slot for controls; `FILL_CARD_UI` (auto-imported const `{ root: string, body: string }`); `--ui-bg-sidebar` token; card theme where `useAppConfig().ui.card.slots.title` / `.description` hold the header text classes.

- [ ] **Step 1: Tokens.** In `main.css`, light `:root` surface block becomes:

```css
    /* surfaces: sidebar darkest, panel lightest, containers between */
    --ui-bg: #ffffff;
    --ui-bg-sidebar: var(--color-gray-100);
    --ui-bg-muted: var(--color-gray-50);
    --ui-bg-elevated: var(--color-gray-100);
    --ui-bg-accented: var(--color-gray-200);
    --ui-bg-inverted: var(--color-gray-950);
```

Delete the `--ui-bg-band` line and its comment in both themes. In `.dark`:

```css
    --ui-bg: var(--color-gray-900);
    --ui-bg-sidebar: #0c0c0e;
    --ui-bg-muted: #131316;
    --ui-bg-elevated: var(--color-gray-800);
    --ui-bg-accented: var(--color-gray-700);
    --ui-bg-inverted: #ffffff;
```

Update the stale comments (`surfaces: white content/cards/header · sidebar (muted) #FAFAFA`, the `.dark` "dark surfaces stay bespoke" lead) to describe the three tones; keep `--ui-border*` values as they are (`#e4e4e7` / `#27272a` already).

- [ ] **Step 2: Replace every `bg-(--ui-bg-band)`** with `bg-elevated/50` in: `components/ui/InlineEmptyState.vue:13`, `components/catalog/Card.vue:60,79`, `components/agent/Panel.vue:177`, `components/agent/Summary.vue:22`, `components/agent/ConnectCard.vue:152,168`, `pages/components/[kind].vue:257`. Then `grep -rn "ui-bg-band" APP` must return nothing.

- [ ] **Step 3: app.config.** Replace the `card`, `dashboardSidebar`, `navigationMenu` and `table` entries with:

```ts
    card: {
        // Section cards: container tone, no rules between header, body and
        // footer. CardHeader reads `title` / `description` from here so a card
        // with controls reads the same as one using the title props.
        slots: {
            title: 'text-highlighted font-semibold',
            description: 'mt-1 text-muted text-sm',
            body: '[[data-slot=header]+&]:pt-0 sm:[[data-slot=header]+&]:pt-0',
            footer: '[[data-slot=body]+&]:pt-0'
        },
        variants: {
            variant: {
                outline: {
                    root: 'bg-muted ring ring-default divide-y-0'
                }
            }
        },
        defaultVariants: {
            variant: 'outline'
        }
    },
    dashboardSidebar: {
        slots: {
            root: 'bg-(--ui-bg-sidebar)'
        }
    },
    navigationMenu: {
        slots: {
            label: 'text-dimmed'
        },
        variants: {
            orientation: {
                vertical: {
                    link: 'py-2'
                }
            }
        },
        // The sidebar tone is bg-elevated's in light mode, so the sidebar's
        // active and hover pills take the panel tone instead.
        compoundVariants: [
            {
                orientation: 'vertical',
                variant: 'pill',
                active: true,
                highlight: false,
                class: {
                    link: 'before:bg-default before:ring before:ring-default'
                }
            },
            {
                orientation: 'vertical',
                variant: 'pill',
                active: false,
                disabled: false,
                class: {
                    link: 'hover:before:bg-default/70'
                }
            }
        ]
    },
    table: {
        // Framed tables: rounded outer line, tinted header band. Sticky headers
        // take the card tone they scroll inside.
        slots: {
            root: 'rounded-lg ring ring-default',
            thead: '[&>tr]:bg-elevated/50',
            th: 'py-2.5',
            td: 'py-2.5'
        },
        variants: {
            sticky: {
                true: {
                    thead: 'bg-muted'
                },
                header: {
                    thead: 'bg-muted'
                }
            }
        }
    },
```

- [ ] **Step 4: CardHeader.** Create `components/ui/CardHeader.vue`:

```vue
<script setup lang="ts">
/**
 * Header of a UCard section that carries controls: the card theme's title
 * and description on the left, the controls (default slot) on the right.
 * Goes in a UCard's #header only.
 */
defineProps<{
    title: string
    description?: string
}>()

const slots = useAppConfig().ui.card.slots
</script>

<template>
    <div class="flex flex-wrap items-center gap-3">
        <div class="min-w-0 flex-1">
            <div :class="slots.title">{{ title }}</div>
            <div v-if="description"
                 :class="slots.description">{{ description }}</div>
        </div>
        <div class="flex shrink-0 items-center gap-2">
            <slot />
        </div>
    </div>
</template>
```

If `nuxt typecheck` reports `slots` possibly undefined, type the read as `const slots = useAppConfig().ui.card.slots as { title: string, description: string }`.

- [ ] **Step 5: FILL_CARD_UI.** Create `utils/card.ts`:

```ts
/** UCard `ui` for a single-table page: the card fills the panel and its table scrolls inside it. */
export const FILL_CARD_UI = {
    root: 'flex-1 min-h-0 flex flex-col',
    body: 'flex-1 min-h-0 flex flex-col',
}
```

- [ ] **Step 6: TableFooter.** In `components/ui/TableFooter.vue`, replace the doc comment with `/** Table caption (default slot) and pager, for a table card's #footer. */` and the root class with `flex items-center justify-between gap-3 text-sm text-muted`.

- [ ] **Step 7: Verify.** Lint + typecheck pass. Screenshot `/` and `/components/sources` in both themes and check: sidebar visibly darker than the panel in both themes; sidebar active item is a panel-tone pill with a ring and hover is visible (including a hub child such as Components → Sources). If the hub child's active state is invisible, add `variants: { active: { true: { childLink: 'before:bg-default before:ring before:ring-default' } } }` to `navigationMenu` and re-check. Existing `UCard`s (catalog cards, login) render container tone with no header rule.

---

### Task 2: Table cards

**Files:**
- Modify: `APP/components/ui/DataTable.vue`
- Modify: `APP/components/executions/RunsTable.vue`, `APP/components/executions/BackfillsTable.vue`, `APP/components/collection/Table.vue`, `APP/components/organization/MembersTable.vue`
- Modify: every `bordered` DataTable consumer: `APP/pages/settings/authentication.vue:182` (and any other `bordered` hit of `grep -rn "bordered" APP`)

**Interfaces:**
- Consumes: `FILL_CARD_UI`, `TableFooter` (Task 1).
- Produces: `DataTable` prop `fill?: boolean` (replaces `bordered`); `RunsTable`, `BackfillsTable`, `CollectionTable` always render as fill cards; `MembersTable` flows.

- [ ] **Step 1: DataTable props.** Replace the `bordered` prop and its doc with:

```ts
    /**
     * Single-table page: the card fills the panel, its header row sticks and
     * the pager stays pinned while rows scroll. Otherwise the card flows with
     * the page.
     */
    fill?: boolean
```

- [ ] **Step 2: DataTable template.** Replace the whole `<template>` with:

```vue
<template>
    <div class="w-full flex flex-col gap-4"
         :class="fill && 'flex-1 min-h-0'">
        <UAlert v-if="error"
                color="error"
                icon="i-lucide-triangle-alert"
                title="Couldn't load this list"
                :description="errorDetail(error) ?? GENERIC_ERROR"
                :actions="[{
                    label: 'Try again',
                    icon: 'i-lucide-refresh-cw',
                    color: 'neutral',
                    variant: 'outline',
                    onClick: () => emit('retry'),
                }]" />

        <div v-if="showEmpty"
             class="w-full max-w-[1040px] mx-auto">
            <slot name="empty" />
        </div>

        <UCard v-if="showTable"
               :ui="fill ? FILL_CARD_UI : undefined">
            <template #header>
                <div class="flex flex-wrap items-center gap-3">
                    <UInput v-model="globalFilter"
                            :placeholder="searchPlaceholder ?? 'Search...'"
                            icon="i-lucide-search"
                            class="max-w-sm"
                            @update:model-value="tableRef?.tableApi?.setGlobalFilter($event)" />

                    <div class="ml-auto flex items-center gap-2">
                        <slot name="filters" />
                        <slot name="toolbar" />

                        <UButton v-if="selectedCount > 0"
                                 color="error"
                                 icon="i-lucide-trash-2"
                                 :label="`Delete (${selectedCount})`"
                                 @click="requestBulkDelete" />
                    </div>
                </div>
            </template>

            <UTable ref="table"
                    v-model:pagination="pagination"
                    :data="rows"
                    :columns="columnsWithActions"
                    :loading="loading"
                    :global-filter="globalFilter"
                    :pagination-options="{ getPaginationRowModel: getPaginationRowModel() }"
                    :sticky="fill"
                    :ui="{ tr: noRowClick ? '' : 'cursor-pointer' }"
                    :class="fill && 'flex-1 min-h-0'"
                    @select="(_e: Event, row: any) => emit('edit', row.original)"
                    @contextmenu="onRowContextMenu">
                <template #actions-cell="{ row }">
                    <div class="flex justify-end">
                        <UDropdownMenu :items="buildRowActions(row.original)">
                            <UButton icon="i-lucide-ellipsis-vertical"
                                     color="neutral"
                                     variant="ghost"
                                     size="sm" />
                        </UDropdownMenu>
                    </div>
                </template>
            </UTable>

            <template #footer>
                <TableFooter :page="pagination.pageIndex + 1"
                             :total="totalCount"
                             :page-size="PAGE_SIZE"
                             @update:page="(p: number) => pagination = { ...pagination, pageIndex: p - 1 }">
                    <template v-if="selectedCount > 0">
                        {{ selectedCount }} of {{ totalCount }} row(s) selected.
                    </template>
                    <template v-else>
                        {{ totalCount }} row(s) total.
                    </template>
                </TableFooter>
            </template>
        </UCard>

        <!-- Right-click context menu -->
        <UDropdownMenu v-model:open="ctxMenuOpen"
                       :items="ctxMenuItems"
                       :modal="false"
                       :content="{ reference: ctxMenuVirtual, side: 'bottom', align: 'start', sideOffset: 4 }">
            <div class="hidden" />
        </UDropdownMenu>
    </div>
</template>
```

- [ ] **Step 3: RunsTable.** Keep both empty states outside the card; wrap the search row, table and footer in a fill card. Template becomes:

```vue
<template>
    <div class="flex flex-col flex-1 min-h-0 gap-4">
        <div v-if="!loading && runs.length === 0 && !filtered"
             class="w-full max-w-[1040px] mx-auto">
            <!-- existing "No executions yet" EmptyState, unchanged -->
        </div>

        <UCard v-else
               :ui="FILL_CARD_UI">
            <template #header>
                <div class="flex flex-wrap items-center gap-3">
                    <!-- existing UInput (search) -->
                    <div class="ml-auto flex items-center gap-2">
                        <!-- existing KindFilter, TypeFilter, StatusFilter -->
                    </div>
                </div>
            </template>

            <div v-if="!loading && runs.length === 0"
                 class="flex flex-col items-center gap-3 py-16 text-sm text-muted">
                No runs match these filters.
                <UButton variant="ghost"
                         icon="i-lucide-x"
                         label="Clear filters"
                         @click="clearFilters" />
            </div>
            <UTable v-else
                    v-model:expanded="expanded"
                    :data="rows"
                    :columns="columns"
                    :get-row-id="getRowId"
                    :get-sub-rows="getSubRows"
                    :expanded-options="expandedOptions"
                    :loading="loading"
                    :sorting="[{ id: 'created_at', desc: true }]"
                    sticky
                    class="flex-1 min-h-0"
                    :ui="tableUi"
                    @select="(_e: Event, row: any) => navigateTo(`/executions/runs/${row.original.id}`)" />

            <template #footer>
                <TableFooter :page="pageIndex + 1"
                             :total="total"
                             :page-size="pageSize"
                             @update:page="onPageChange">
                    {{ total }} run(s) total.
                </TableFooter>
            </template>
        </UCard>
    </div>
</template>
```

The HTML comments above mark where the existing, unchanged elements move; paste them verbatim (from the current file) and drop the comments. This reorders the two empty branches: "no runs at all" (unfiltered) renders the onboarding outside the card; "no match" renders inside it so the filters stay reachable. Check the existing `filtered` computed matches that meaning (it is truthy when any filter is set).

- [ ] **Step 4: BackfillsTable.** Same shape: EmptyState branch outside, `UCard :ui="FILL_CARD_UI"` with `#header` holding the search `UInput`, then a `ml-auto flex items-center gap-2` group with the `UCheckbox` (drop its `class="ml-auto"`) and `StatusFilter`; the `UTable` (unchanged props) as body; `TableFooter` (minus `class="shrink-0"`) in `#footer`.

- [ ] **Step 5: CollectionTable.** Root `div.w-full.flex.flex-col.gap-4.flex-1.min-h-0` becomes `UCard :ui="FILL_CARD_UI"`: the first child row (search + "Expand all") moves into `#header` (its `div.flex.items-center.gap-3` gets `flex-wrap`), the `UTable` stays the body, the `TableFooter` at the end (`~L561`) moves to `#footer` without `class="shrink-0"`.

- [ ] **Step 6: MembersTable.** Root `div.flex.flex-col.flex-1.min-h-0` becomes a plain `<div>`; drop `bordered` from the `DataTable`. Remove `bordered` from every other consumer found by `grep -rn " bordered" APP`.

- [ ] **Step 7: Verify.** Lint + typecheck. Screenshot `/components/sources`, `/executions/runs`, `/executions/backfills`, `/collection`, `/organization`, `/settings/authentication?tab=tokens` in both themes: tables framed (ring + header band), search/filters in the card header, pager in the card footer; on sources/runs/backfills/collection the card fills the panel height and rows scroll under a sticky header. Pages still have the old chrome; that's expected until Task 3+.

---

### Task 3: Shell and the Executions hub

**Files:**
- Create: `APP/components/ui/AppNavbar.vue`
- Modify: `APP/composables/agentPanel.ts` (add `AGENT_PANEL_HOST`)
- Modify: `APP/composables/commandPalette.ts` (drop its `meta_k` shortcut)
- Modify: `APP/components/nav/User.vue` (version label)
- Modify: `APP/layouts/default.vue`, `APP/layouts/admin.vue`, `APP/layouts/settings.vue`, `APP/layouts/agent.vue`
- Modify: `APP/pages/executions/runs/index.vue`, `APP/pages/executions/backfills/index.vue`, `APP/pages/executions/runs/[run].vue`, `APP/pages/executions/backfills/[backfill].vue`

**Interfaces:**
- Consumes: `FILL_CARD_UI`, table cards (Task 2).
- Produces: `<AppNavbar title?>` with slots `#title` and `#right`; `AGENT_PANEL_HOST: InjectionKey<boolean>`. From here on, layouts render no panel: any page not yet migrated renders bare until its task.

- [ ] **Step 1: Agent host key.** Append to `composables/agentPanel.ts`:

```ts
/** Provided by the layout that mounts the agent panel, so navbars only offer the toggle where it works. */
export const AGENT_PANEL_HOST: InjectionKey<boolean> = Symbol('agent-panel-host')
```

(add `import type { InjectionKey } from 'vue'` at the top if not auto-imported).

- [ ] **Step 2: AppNavbar.** Create `components/ui/AppNavbar.vue`:

```vue
<script setup lang="ts">
/**
 * Page navbar: sidebar toggle, the page title (or the page's own #title
 * content, such as a crumb and status), the page's #right controls, then
 * the agent toggle wherever the layout hosts the agent panel.
 */
defineProps<{
    title?: string
}>()

const agentHost = inject(AGENT_PANEL_HOST, false)
const userStore = useUserStore()
const { open: agentOpen } = useAgentPanel()
</script>

<template>
    <UDashboardNavbar :title="title">
        <template #leading>
            <UDashboardSidebarCollapse />
        </template>
        <template v-if="$slots.title"
                  #title>
            <slot name="title" />
        </template>
        <template #right>
            <slot name="right" />
            <UButton v-if="agentHost && userStore.agentAvailable"
                     icon="i-lucide-sparkles"
                     label="Agent"
                     color="neutral"
                     :variant="agentOpen ? 'soft' : 'outline'"
                     aria-label="Toggle agent panel"
                     @click="agentOpen = !agentOpen" />
        </template>
    </UDashboardNavbar>
</template>
```

- [ ] **Step 3: Command palette.** In `composables/commandPalette.ts` delete the `defineShortcuts({ meta_k: … })` block (L54-61); `UDashboardSearch` owns ⌘K now. Leave the refresh-on-open `watch(open, …)` and the returned `{ open, searchTerm, loading, groups }` unchanged.

- [ ] **Step 4: User menu version.** In `components/nav/User.vue`, add `const appVersion = useRuntimeConfig().public.version` and, before `return groups`, `if (appVersion) groups.push([{ label: \`Interloper v${appVersion}\`, type: 'label' }])`.

- [ ] **Step 5: Default layout.** Rewrite `layouts/default.vue`. Script: keep `route`, `userStore`, `destinations`, `openMenus` + its `watch`, `items`; keep `useCommandPalette()` destructure (rename to `searchOpen`, `searchTerm`, `searchLoading`, `searchGroups`) and `useAgentPanel()` (`open`, `width`, `dragging`); add `provide(AGENT_PANEL_HOST, true)`; delete `appVersion`, `PageHeaderMeta`, `pageHeader`, `pageTitle`, `customNavbar`. Template:

```vue
<template>
    <div>
        <UDashboardGroup storage-key="dashboard-data"
                         :style="{ right: agentOpen && userStore.agentAvailable ? `${agentWidth}px` : '0px' }"
                         :ui="{ base: `fixed top-0 bottom-0 left-0 flex overflow-hidden ${agentDragging ? '' : 'transition-[right] duration-300'}` }">
            <UDashboardSidebar collapsible
                               resizable
                               :ui="{ footer: 'border-t border-default' }">
                <template #header="{ collapsed }">
                    <NavLogo v-if="!collapsed" />
                    <LogoIcon v-else
                              class="mx-auto h-6 w-auto text-primary" />
                </template>

                <template #default="{ collapsed }">
                    <UDashboardSearchButton :collapsed="collapsed"
                                            class="bg-transparent ring-default" />
                    <UNavigationMenu v-model="openMenus"
                                     :collapsed="collapsed"
                                     :items="items"
                                     type="multiple"
                                     popover
                                     color="neutral"
                                     orientation="vertical" />
                </template>

                <template #footer="{ collapsed }">
                    <div class="flex flex-col gap-1 w-full">
                        <NavOrganisation :collapsed="collapsed" />
                        <NavUser :collapsed="collapsed" />
                    </div>
                </template>
            </UDashboardSidebar>

            <slot />

            <UDashboardSearch v-model:open="searchOpen"
                              v-model:search-term="searchTerm"
                              :groups="searchGroups"
                              :loading="searchLoading"
                              :color-mode="false"
                              :fuse="{ fuseOptions: { keys: ['label', 'suffix', 'keywords'] } }"
                              placeholder="Search..." />
        </UDashboardGroup>

        <AgentPanel v-if="userStore.agentAvailable" />
    </div>
</template>
```

`:color-mode="false"` because the palette's own `actions` group already carries the color-mode command. The floating launcher and `--launcher-inset` are gone with this template.

- [ ] **Step 6: Admin and settings layouts.** In both, delete `appVersion`, `PageHeaderMeta`, `pageHeader`, `pageTitle`, `customNavbar`, the version `<span>`, and the whole `<UDashboardPanel>`; put `<slot />` after `</UDashboardSidebar>`. Sidebars (eyebrow badge, items, "Exit Admin" / "Back to app", `NavUser`) stay.

- [ ] **Step 7: Agent layout.** Replace its `<UDashboardPanel :ui="{ body: '!p-0 !gap-0 overflow-hidden' }"><template #body><slot /></template></UDashboardPanel>` with `<slot />`. The two agent pages already render their own `UDashboardPanel` and become top-level panels.

- [ ] **Step 8: Runs list** (`pages/executions/runs/index.vue`). Meta: none left (delete the `definePageMeta` call if empty). Template:

```vue
<template>
    <UDashboardPanel id="runs">
        <template #header>
            <AppNavbar title="Executions" />
            <UDashboardToolbar>
                <template #left>
                    <UNavigationMenu :items="EXECUTION_VIEWS"
                                     highlight
                                     class="-mx-1 flex-1" />
                </template>
            </UDashboardToolbar>
        </template>
        <template #body>
            <ExecutionsRunsTable />
        </template>
    </UDashboardPanel>
</template>
```

- [ ] **Step 9: Backfills list** (`pages/executions/backfills/index.vue`): identical to Step 8 with `id="backfills"` and `<ExecutionsBackfillsTable />`.

- [ ] **Step 10: Run detail** (`pages/executions/runs/[run].vue`). Meta keeps `orgSwitchTarget`. Root becomes `UDashboardPanel id="run" :ui="{ body: 'p-0 sm:p-0 gap-0 sm:gap-0' }"`; `#header` holds `AppNavbar` whose `#title` slot gets the former `NavTitle` contents (Runs `ULink` / mono run id / `StatusPill`) and whose `#right` slot gets the former `NavActions` contents ("Retry failed", "Retry all", same `v-if="retryable"` on a wrapping `<template>`, drop `size="sm"`). `#body` holds the existing `OrganizationGate` tree unchanged (meta strip, status bar, splitters: this is a canvas page). Delete the `<NavTitle>` and `<NavActions>` elements. Normalise pixel font sizes in this file only.

- [ ] **Step 11: Backfill detail** (`pages/executions/backfills/[backfill].vue`). Meta keeps `orgSwitchTarget`. `UDashboardPanel id="backfill"`; `AppNavbar #title` = former `NavTitle` contents (Backfills link / short id / `StatusPill`); `#right` = "Cancel" (`v-if="cancellable"`, drop `size="sm"`). `#body`: `OrganizationGate` holding (a) a `UCard title="Backfill"` whose body is the existing meta row (target, range, partitions, fail-fast), and (b) a `UCard :ui="FILL_CARD_UI"` whose body is the existing `UTable` (`sticky`, `class="flex-1 min-h-0"`) and whose `#footer` is the existing `TableFooter`. Fix the broken indentation around the former L172-214 while moving it. The `OrganizationGate` content wrapper needs `class="flex flex-1 min-h-0 flex-col gap-4"`.

- [ ] **Step 12: Verify.** Lint + typecheck. On `:3100`, both themes: sidebar has the search button (opens the palette; ⌘K toggles it; results and actions work; no duplicate palette); agent toggle in the navbar opens/closes the panel and the dashboard slides; user menu shows the version; `/executions/runs`, `/executions/backfills`, a run detail and a backfill detail match the skeleton; admin and settings sidebars still render (their pages are bare until Tasks 7-8).

---

### Task 4: Components hub

**Files:**
- Modify: `APP/pages/components/sources.vue`, `destinations.vue`, `hooks.vue`, `jobs.vue`, `[kind].vue`

**Interfaces:**
- Consumes: `AppNavbar`, `DataTable fill`, `useComponentViews()` (returns `ComputedRef<NavPage[]>`; `NavPage` items are valid `NavigationMenuItem`s).

- [ ] **Step 1: Shared shape.** In each page's script add `const views = useComponentViews()`. Meta: drop `title` and `fullBleed`, keep `validate` on `[kind].vue`. Template root:

```vue
<UDashboardPanel id="<kind>">
    <template #header>
        <AppNavbar title="Components" />
        <UDashboardToolbar>
            <template #left>
                <UNavigationMenu :items="views"
                                 highlight
                                 class="-mx-1 flex-1" />
            </template>
            <template #right>
                <!-- former NavActions button, unchanged except no size prop -->
            </template>
        </UDashboardToolbar>
    </template>
    <template #body>
        <DriftBanner />
        <DataTable fill
                   ...existing props and slots... />
        <!-- existing WizardDrawer / ExecutionsRunModal -->
    </template>
</UDashboardPanel>
```

Remove the outer `<div>`, `<NavActions>` and `<NavComponentsHub>` wrappers.

- [ ] **Step 2: Per page.** Ids and actions: `sources` → "New source" (`@click="handleCreate"`); `destinations` → "New destination"; `hooks` → "New hook"; `jobs` → "New job"; `[kind]` → `id="kind"`, "New {kind}" exactly as the current `NavActions` content at L208-212.
- [ ] **Step 3: Empty states.** In `destinations.vue` and `[kind].vue`, the `#empty` slot's hand-rolled h2 headings and the connection info strip (`[kind].vue` ~L257) normalise to the typography scale; keep their structure.
- [ ] **Step 4: Verify.** Lint + typecheck; screenshot each of the five pages (both themes) including one empty kind if the seed has one: hub nav highlights the active view, action on the right, banner + filling table card below.

---

### Task 5: Overview

**Files:**
- Modify: `APP/pages/index.vue`
- Modify: `APP/components/overview/HealthStrip.vue`, `HealthTile.vue`, `AttentionList.vue`, `TimelineSection.vue`, `CoverageCalendar.vue`, `UpcomingList.vue`, `RecentList.vue`, `ComponentsInventory.vue`

**Interfaces:**
- Consumes: `AppNavbar`, `CardHeader`, card theme.

- [ ] **Step 1: Page.** `pages/index.vue`: drop `definePageMeta`; `UDashboardPanel id="overview"` with `AppNavbar title="Overview"` and a toolbar `#right` holding the refresh `UButton` (`icon="i-lucide-refresh-cw" color="neutral" variant="outline" :loading="loading" aria-label="Refresh"`, no size). `#body`: the error `UAlert` and the existing components in the same order; remove the `max-w-[1360px]` wrapper (the panel body's gap spaces them). Keep the two-column grid as `grid grid-cols-1 gap-4 sm:gap-6 lg:grid-cols-2`.

- [ ] **Step 2: KPI tiles.** `HealthTile.vue` becomes a linked UCard:

```vue
<script setup lang="ts">
/** Overview KPI tile: label, headline number with inline detail, footer visual pinned to the bottom. */
defineProps<{
    label: string
    to: string
    headline: string | number
    headlineClass?: string
}>()
</script>

<template>
    <UCard :as="resolveComponent('NuxtLink')"
           :to="to"
           class="transition-colors hover:bg-elevated/50"
           :ui="{ body: 'flex h-full min-h-32 flex-col gap-3' }">
        <div class="text-sm text-muted">{{ label }}</div>
        <div class="flex items-baseline gap-2">
            <span class="text-3xl font-semibold tabular-nums text-highlighted"
                  :class="headlineClass">{{ headline }}</span>
            <span class="text-sm text-muted"><slot name="detail" /></span>
        </div>
        <div class="mt-auto flex flex-col gap-1.5">
            <slot name="footer" />
        </div>
    </UCard>
</template>
```

If `resolveComponent` inside the template misbehaves (see memory `nuxt-table-resolvecomponent-gotcha`), import `NuxtLink` from `#components` in the script and pass `:as="NuxtLink"`. In `HealthStrip.vue` change the grid to `grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4` and the footers' `text-[10.5px]` to `text-xs`.

- [ ] **Step 3: Sections with controls.** `TimelineSection.vue` and `CoverageCalendar.vue`: replace `<OverviewSection title meta link…>` with:

```vue
<UCard>
    <template #header>
        <CardHeader title="Timeline"
                    :description="`${timezone} · ${rangeLabel}`">
            <!-- the former #actions content (Window label + pill UTabs) -->
            <UButton label="All executions"
                     to="/executions/runs"
                     color="neutral"
                     variant="outline"
                     size="sm" />
        </CardHeader>
    </template>
    <!-- former body: drop the inner `rounded-lg border border-default` frame
         around the chart (the card frames it now); keep the fixed height -->
</UCard>
```

For `CoverageCalendar.vue` use its current title, meta (as `description`) and its source `USelect` + window pill `UTabs` as controls; same frame removal.

- [ ] **Step 4: List sections.** `AttentionList.vue`, `UpcomingList.vue`, `RecentList.vue`: `UCard` with `:ui="{ body: 'p-0 sm:p-0' }"`; header via `CardHeader` (title, former `meta` as `description`, former link as the outline `UButton`), or `title`/`description` props when there is no link (`AttentionList`: `title="Needs attention"`, `description` = the item-count meta). The list's outer `overflow-hidden rounded-lg border border-default divide-y divide-default` becomes `divide-y divide-default border-t border-default`; rows get `px-4 sm:px-6` and `hover:bg-elevated/50` (was `hover:bg-muted`, now the card's own tone). AttentionList's "All clear" box loses its own border/background and sits in the card body with `p-4 sm:p-6`.

- [ ] **Step 5: Components inventory → UTable.** Replace the hand-rolled grid in `ComponentsInventory.vue` with a framed `UTable` inside `UCard` (header: `CardHeader title="Components" :description="summary"` + "Open collection" outline button). Columns (keep the script's `table` computed as `data`):

```ts
const columns: TableColumn<(typeof table.value)[number]>[] = [
    {
        id: 'kind',
        header: 'Kind',
        cell: ({ row }) => h('span', { class: 'flex items-center gap-2.5 font-medium text-highlighted' }, [
            h(UIcon, { name: row.original.meta.icon, class: 'size-4 shrink-0 text-dimmed' }),
            row.original.meta.label,
        ]),
    },
    {
        accessorKey: 'total',
        header: 'Count',
        meta: { class: { th: 'text-right', td: 'text-right font-semibold tabular-nums text-highlighted' } },
    },
    {
        id: 'state',
        header: 'State',
        meta: { class: { td: 'w-full' } },
        cell: ({ row }) => h('div', { class: 'flex h-2 gap-0.5 overflow-hidden rounded-full bg-accented' },
            row.original.segments.map(s => h('div', { key: s.key, class: s.class, style: { width: `${s.pct}%` }, title: s.title }))),
    },
    {
        id: 'issues',
        header: 'Issues',
        meta: { class: { th: 'text-right', td: 'text-right text-xs' } },
        cell: ({ row }) => h('span', { class: row.original.issuesClass }, row.original.issues),
    },
]
```

with `import { h } from 'vue'`, `import type { TableColumn } from '@nuxt/ui'` and `import { UIcon } from '#components'`. Render `<UTable :data="table" :columns="columns" :ui="{ tr: 'cursor-pointer' }" @select="(_e: Event, row: any) => navigateTo(row.original.meta.to)" />`, then the legend row (`flex items-center gap-4 text-xs text-muted`) in the card `#footer`.

- [ ] **Step 6: Verify.** Lint + typecheck; `/` in both themes against baseline: four equal KPI cards, every section a card with header inside, no section header floating on the panel, inventory is a framed table.

---

### Task 6: Collection, Graph, Timeline

**Files:**
- Modify: `APP/pages/collection.vue`, `APP/pages/graph.vue`, `APP/pages/timeline.vue`, `APP/components/graph/Toolbar.vue`

- [ ] **Step 1: Collection.** Drop meta; `UDashboardPanel id="collection" :ui="{ body: 'p-0 sm:p-0 gap-0 sm:gap-0' }"`; `AppNavbar title="Collection"`; toolbar `#right` = "New source" (former NavActions). `#body` unchanged structurally (empty state, or splitter with left panel `DriftBanner` + `CollectionTable`, right `GraphAssetPanel`; wizard drawer last), except the left panel's `p-4` becomes `flex flex-col gap-4 p-4 sm:p-6 min-h-0` so the fill card from Task 2 stretches.
- [ ] **Step 2: Graph toolbar.** In `components/graph/Toolbar.vue`, replace the root strip (`border-b px-4 py-2` element at ~L34) with `<UDashboardToolbar>` whose `#left` holds the current left-side controls and `#right` the right-side ones (Status / Group-by pill tabs keep their props).
- [ ] **Step 3: Graph page.** Drop meta; canvas panel `id="graph"` (`p-0` body ui as in the skeleton reference). `#header`: `AppNavbar title="Graph"` with `#right` = "New source", then `<GraphToolbar …existing props…/>` (now itself a `UDashboardToolbar`). `#body`: the existing `SplitterGroup` and `WizardDrawer`.
- [ ] **Step 4: Timeline.** Drop meta; panel `id="timeline"` with `p-0` body ui. `#header`: `AppNavbar title="Timeline"`, then `UDashboardToolbar` with `#left` = the former inline strip's "Window" label (`text-sm text-muted`) + pill `UTabs`, `#right` = the former NavActions (truncation badge or run count, refresh button without size). `#body`: the `EmptyState` / `ChartExecutionTimeline` branch unchanged. Delete the inline strip at former L53-61.
- [ ] **Step 5: Verify.** Lint + typecheck; screenshot the three pages, both themes; graph and timeline canvases still fill the panel, splitters still resize, the asset panel still slides in on collection and graph.

---

### Task 7: Organization and settings

**Files:**
- Modify: `APP/pages/organization.vue`, `APP/pages/settings/profile.vue`, `APP/pages/settings/authentication.vue`

- [ ] **Step 1: Organization.** Drop `title` + `pageHeader` meta. `UDashboardPanel id="organization"`, `AppNavbar title="Organization"`. `#body`: `OrganizationMembersTable` (flow), then `<UCard title="Access levels" description="What each role can do in this workspace.">` whose body is the existing role grid; the role items lose their own `border rounded-lg bg-default` look for `rounded-lg ring ring-default bg-default p-4` (panel tone inside the container tone) and their pixel sizes normalise. Delete the eyebrow + h2 heading above the grid. Invite modal last.
- [ ] **Step 2: Profile.** `UDashboardPanel id="profile"`, `AppNavbar title="Profile"`. `#body`: `<div class="mx-auto flex w-full max-w-3xl flex-col gap-4 sm:gap-6">` with `UCard title="Account"` and `UCard title="Time settings"` replacing the hand-rolled section headers and `bg-elevated/25` row boxes; each card body holds the existing rows separated by `divide-y divide-default` (card `:ui="{ body: 'p-0 sm:p-0' }"`, rows `px-4 sm:px-6 py-4`). The Save row stays below the cards, right-aligned.
- [ ] **Step 3: Authentication.** Drop meta. Replace `PageTabs` with a routed toolbar nav:

```ts
const tabs = computed<NavigationMenuItem[]>(() => [
    { label: 'Sign in', icon: 'i-lucide-log-in', to: { query: { tab: 'signin' } }, active: activeTab.value === 'signin' },
    { label: 'Personal Access Tokens', icon: 'i-lucide-key', badge: tokenCount.value, to: { query: { tab: 'tokens' } }, active: activeTab.value === 'tokens' },
])
```

where `activeTab` is the existing `?tab=`-synced ref and `tokenCount` is the value the current PageTabs badge shows (reuse its expression). `UDashboardPanel id="authentication"`, `AppNavbar title="Authentication"`, `UDashboardToolbar #left` = `UNavigationMenu :items="tabs" highlight class="-mx-1 flex-1"`. `#body`: `signin` → `UCard title="Sign in"` holding the existing Google row (drop its own border/background); `tokens` → the `DataTable` (flow, its `#toolbar` "New token" unchanged). Token create modal last.
- [ ] **Step 4: Verify.** Lint + typecheck; screenshot the four URLs (both themes); the settings sidebar's Authentication children and the toolbar nav both switch tabs and agree on the active one.

---

### Task 8: Admin

**Files:**
- Modify: `APP/pages/admin/index.vue`, `APP/pages/admin/config.vue`, `APP/pages/admin/organisations/index.vue`, `APP/pages/admin/users/index.vue`, `APP/pages/admin/organisations/[id].vue`

**Interfaces:**
- Consumes: `AppNavbar` (no agent toggle here: the admin layout provides no `AGENT_PANEL_HOST`), `CardHeader`, `DataTable fill`, `FILL_CARD_UI`.

- [ ] **Step 1: Admin overview.** Drop `title` (keep `layout`, `middleware`). `UDashboardPanel id="admin"`, `AppNavbar title="Overview"`. Fused stat grid (`gap-px bg-(--ui-border)` ~L234) → `grid grid-cols-2 gap-4 sm:gap-6 xl:grid-cols-4` of `UCard`s with the KPI anatomy from Task 5 Step 2 (label `text-sm text-muted`, value `text-3xl font-semibold tabular-nums`, caption `text-sm text-muted`). Each `PanelCard` → `UCard` (`title`, `description`; icon dropped; a `badge` count becomes the `description`, e.g. "3 organisations"; a `linkLabel/linkTo` becomes a `CardHeader` outline button). The hand-rolled "Top organisations by usage" section (~L309-341) → `UCard` + framed `UTable` with columns matching its fake header row.
- [ ] **Step 2: Config.** `UDashboardPanel id="admin-config"`, `AppNavbar title="Instance configuration"`. `#body`: `max-w-3xl mx-auto w-full flex flex-col gap-4 sm:gap-6`; intro paragraph `text-sm text-muted`; each section's `PanelCard` → `UCard :title :ui="{ body: 'p-0 sm:p-0' }"` with the label/value rows as `divide-y divide-default`, rows `px-4 sm:px-6 py-3`.
- [ ] **Step 3: Organisations list.** `UDashboardPanel id="admin-organisations"`, `AppNavbar title="Organisations"`, toolbar `#right` = "New organisation". `#body`: `DataTable fill` (existing props) + create modal.
- [ ] **Step 4: Users.** `UDashboardPanel id="admin-users"`, `AppNavbar title="Users"`. `#body`: `DataTable fill` (existing props, `#toolbar` org `USelect`); drop the `max-w-[1040px]` wrapper.
- [ ] **Step 5: Organisation detail.** Drop `title`, `customNavbar`, `fullBleed`. Route the tabs through `?tab=` (values `members`, `usage`, `activity`, `settings`; default `members`): replace the local tab ref with `const tab = computed(() => (route.query.tab as string) || 'members')` and the hand-rolled `UTabs` (~L300-305) with a toolbar `UNavigationMenu` built like Task 7 Step 3 (labels from the existing items at L77-82, Members keeps its count). `AppNavbar #title` = former NavTitle (Organisations link / org name); `#right` = "Open workspace" (`v-if="isMember"`). `#body`: `div.mx-auto.w-full.max-w-5xl.flex.flex-col.gap-4.sm:gap-6` (replaces the own scroll frame `max-w-[1040px] px-6 py-8`), then per tab:
  - members: `OrganizationMembersTable` (flow) + invite modal;
  - usage: KPI `UCard` grid replacing the fused strip + period bar (period bar moves into a `CardHeader` of a "Usage" card if it is a control, else stays as the first card's body), Ledger `UCard`, Limits `UCard` + framed `UTable` replacing the hand-rolled table (~L393-427);
  - activity: `UCard title="Activity"`;
  - settings: `UCard title="General"` and `UCard title="Danger zone" :ui="{ root: 'ring-error/40', title: 'text-error' }"` + delete modal.
  `AdminQuotaDrawer` last.
- [ ] **Step 6: Verify.** Lint + typecheck; screenshot all admin URLs including each `?tab=` (both themes); deep-linking `?tab=usage` opens that tab; the org switch target and "Exit Admin" still work.

---

### Task 9: Agent, standalone pages, leftovers

**Files:**
- Modify: `APP/pages/agent/index.vue`, `APP/pages/agent/chat/[id].vue`, `APP/pages/invite/[token].vue`
- Modify: `.eyebrow` users: `APP/components/graph/GraphCanvas.vue:845` (leave), `APP/components/wizard/TypeSelect.vue:69` (leave), `APP/components/agent/Panel.vue:173`, `APP/components/agent/Tool.vue:66,74`

- [ ] **Step 1: Agent pages.** Their own `UDashboardPanel`s are now top-level (Task 3 Step 7). Remove any `!`-prefixed padding overrides that compensated for the old nesting; keep `body: 'p-0 sm:p-0'`. Check the "New chat" landing and a conversation render full height with the sticky prompt.
- [ ] **Step 2: Invite.** Drop `title` and `fullBleed` from `pages/invite/[token].vue`; wrap its content in `UDashboardPanel id="invite"` with `#body` only (no navbar: it is an interstitial).
- [ ] **Step 3: Eyebrow audit.** Keep `.eyebrow` only on labels: sidebar badges (admin, settings), separator labels (app.config), wizard tag groups, graph overlay, agent panel "Suggested", agent tool Input/Output. Any other hit of `grep -rn "eyebrow" APP` that styles a page or section title is removed in favour of the card title.
- [ ] **Step 4: Verify.** Lint + typecheck; screenshot `/agent`, a chat, `/login` (logged out) and an invite link (if one can be minted) in both themes.

---

### Task 10: Deletions and final verification

**Files:**
- Delete: `APP/components/nav/Actions.vue`, `APP/components/nav/Title.vue`, `APP/components/nav/ComponentsHub.vue`, `APP/components/nav/ExecutionsHub.vue`, `APP/components/ui/PageFrame.vue`, `APP/components/ui/PageNav.vue`, `APP/components/ui/PageTabs.vue`, `APP/components/ui/PanelCard.vue`, `APP/components/overview/Section.vue`, `APP/components/ui/PageBreadcrumb.vue`, `APP/utils/breadcrumb.ts`

- [ ] **Step 1: Prove they are unused.** For each name run `grep -rnE "<(NavActions|NavTitle|NavComponentsHub|NavExecutionsHub|PageFrame|PageNav|PageTabs|PanelCard|OverviewSection|PageBreadcrumb)\b|titleCrumb|entityCrumb" APP`; it must print nothing. Then delete the files.
- [ ] **Step 2: Prove the meta and hooks are gone.** `grep -rnE "pageHeader|fullBleed|customNavbar|navbar-right|navbar-title|launcher-inset|ui-bg-band|first:pl-0" APP` prints nothing. `grep -rn "definePageMeta" APP/pages | grep "title:"` prints nothing.
- [ ] **Step 3: Typography sweep.** `grep -rnoE "text-\[[0-9.]+px\]" APP --include=*.vue | grep -vE "components/(graph|chart|wizard)/"`; every remaining hit is in a file this plan did not touch. List them in the PR description as known leftovers rather than expanding scope.
- [ ] **Step 4: Full checks.** From the repo root: `make check-typescript`. Fix anything it reports.
- [ ] **Step 5: Full screenshot pass.** Re-shoot every route in `baseline/ROUTES.md` in both themes; compare pairwise with the baseline; send Guillaume a before/after contact sheet (SendUserFile) and list any page that deviates from the skeleton reference with the reason.
- [ ] **Step 6: Hand-off.** Report status (WIP commits on `feat/app-style-redesign`, spec and plan included). Squash and open the PR only on Guillaume's explicit ask; the PR title would be `feat(app): restyle to one dashboard skeleton on default Nuxt UI components`, body ending with `By Digitl`.
