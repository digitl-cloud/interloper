<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

const modeItems = computed<NavigationMenuItem[]>(() => [
    { label: 'Data', icon: 'i-lucide-database', to: '/', active: true },
    { label: 'Analytics', icon: 'i-lucide-chart-column', to: '/analytics' },
    { label: 'Agent', icon: 'i-lucide-sparkles', to: '/agent' },
])

const catalogStore = useCatalogStore()
const { open: commandPaletteOpen, groups: commandPaletteGroups } = useCommandPalette()

/** Icons for resource kinds — fallback to generic box. */
const RESOURCE_KIND_ICONS: Record<string, string> = {
    connection: 'i-lucide-key-round',
    config: 'i-lucide-settings',
}

/** Capitalize and pluralize a kind string: "connection" → "Connections". */
function kindLabel(kind: string): string {
    return kind.charAt(0).toUpperCase() + kind.slice(1) + 's'
}

const items = computed<NavigationMenuItem[]>(() => {
    const nav: NavigationMenuItem[] = [
        {
            label: 'Overview',
            type: 'label',
        },
        {
            label: 'Graph',
            icon: 'i-lucide-workflow',
            to: '/graph',
            active: route.path === '/graph',
        },
        {
            label: 'Catalog',
            icon: 'i-lucide-library',
            to: '/catalog',
            active: route.path === '/catalog',
        },
        {
            label: 'Entities',
            type: 'label',
            class: 'mt-2',
        },
        {
            label: 'Sources',
            icon: 'i-lucide-plug',
            to: '/sources',
            active: route.path === '/sources',
        },
        {
            label: 'Destinations',
            icon: 'i-lucide-database',
            to: '/destinations',
            active: route.path === '/destinations',
        },
    ]

    // Dynamic resource kinds from catalog
    if (catalogStore.resourceKinds.length > 0) {
        for (const kind of catalogStore.resourceKinds) {
            nav.push({
                label: kindLabel(kind),
                icon: RESOURCE_KIND_ICONS[kind] ?? 'i-lucide-box',
                to: `/resources/${kind}`,
                active: route.path === `/resources/${kind}`,
            })
        }
    }

    // Scheduling — always visible
    nav.push(
        {
            label: 'Scheduling',
            type: 'label',
            class: 'mt-2',
        },
        {
            label: 'Jobs',
            icon: 'i-lucide-calendar-clock',
            to: '/jobs',
            active: route.path === '/jobs',
        },
        {
            label: 'Executions',
            icon: 'i-lucide-activity',
            to: '/executions',
            active: route.path.startsWith('/executions'),
        },
    )

    return nav
})
</script>

<template>
    <div>
        <UDashboardGroup storage-key="dashboard-data"
                         :ui="{ base: 'fixed inset-0 flex flex-col overflow-hidden' }">
            <UDashboardNavbar>
                <template #leading>
                    <UDashboardSidebarCollapse />
                </template>
                <template #left>
                    <NuxtLink to="/"
                              class="flex items-center gap-2">
                        <div
                             class="bg-primary text-primary-foreground flex aspect-square size-6 items-center justify-center rounded-md">
                            <img src="/favicon.ico"
                                 alt="Interloper"
                                 class="size-3.5">
                        </div>
                        <span class="font-semibold text-sm">Interloper</span>
                    </NuxtLink>
                </template>

                <UNavigationMenu :items="modeItems" />
            </UDashboardNavbar>

            <div class="flex flex-1 min-h-0 overflow-hidden">
                <UDashboardSidebar collapsible
                                   resizable
                                   :ui="{ root: 'relative hidden lg:flex flex-col min-h-full min-w-16 w-(--width) shrink-0 border-e border-default', footer: 'border-t border-default' }">
                    <template #default="{ collapsed }">
                        <UButton :label="collapsed ? undefined : 'Search...'"
                                 icon="i-lucide-search"
                                 color="secondary"
                                 block
                                 class="mt-2"
                                 :square="collapsed"
                                 @click="commandPaletteOpen = true">
                            <template v-if="!collapsed"
                                      #trailing>
                                <div class="flex items-center gap-0.5 ms-auto">
                                    <UKbd value="meta" />
                                    <UKbd value="K" />
                                </div>
                            </template>
                        </UButton>

                        <UNavigationMenu :collapsed="collapsed"
                                         :items="items"
                                         orientation="vertical" />
                    </template>

                    <template #footer="{ collapsed }">
                        <div class="flex flex-col gap-1 w-full">
                            <NavOrganisation :collapsed="collapsed" />
                            <NavUser :collapsed="collapsed" />
                        </div>
                    </template>
                </UDashboardSidebar>

                <UDashboardPanel
                                 :ui="{ root: 'relative flex flex-col min-w-0 min-h-full lg:not-last:border-e lg:not-last:border-default shrink-0 flex-1', body: '!p-0 !gap-0 overflow-hidden [&>*]:flex-1 [&>*]:flex [&>*]:flex-col [&>*]:min-h-0' }">
                    <template #body>
                        <slot />
                    </template>
                </UDashboardPanel>
                <UModal v-model:open="commandPaletteOpen">
                    <template #content>
                        <UCommandPalette :groups="commandPaletteGroups"
                                         placeholder="Search..."
                                         close
                                         @update:open="commandPaletteOpen = $event" />
                    </template>
                </UModal>
            </div>
        </UDashboardGroup>
    </div>
</template>
