<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

const catalogStore = useCatalogStore()
const userStore = useUserStore()
const { open: commandPaletteOpen, groups: commandPaletteGroups } = useCommandPalette()
const { open: agentOpen, width: agentWidth, dragging: agentDragging } = useAgentPanel()

/** Design page header rendered by the layout, declared via definePageMeta({ pageHeader }). */
interface PageHeaderMeta {
    eyebrow?: string
    title: string
    description?: string
}
const pageHeader = computed(() => route.meta.pageHeader as PageHeaderMeta | undefined)

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
            label: 'Collection',
            icon: 'i-lucide-library',
            to: '/collection',
            active: route.path === '/collection',
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
            label: 'Hooks',
            icon: 'i-carbon-lightning',
            to: '/hooks',
            active: route.path === '/hooks',
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
                         :style="{ right: agentOpen && userStore.agentAvailable ? `${agentWidth}px` : '0px' }"
                         :ui="{ base: `fixed top-0 bottom-0 left-0 flex flex-col overflow-hidden ${agentDragging ? '' : 'transition-[right] duration-300'}` }">
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

            </UDashboardNavbar>

            <div class="flex flex-1 min-h-0 overflow-hidden">
                <UDashboardSidebar collapsible
                                   resizable
                                   :ui="{ root: 'relative hidden lg:flex flex-col min-h-full min-w-16 w-(--width) shrink-0 border-e border-default', footer: 'border-t border-default' }">
                    <template #default="{ collapsed }">
                        <UButton :label="collapsed ? undefined : 'Search...'"
                                 icon="i-lucide-search"
                                 color="neutral"
                                 variant="outline"
                                 block
                                 class="mt-2 bg-default text-dimmed"
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
                        <!-- Full-bleed pages (canvas/split views) manage their own frame. -->
                        <slot v-if="route.meta.fullBleed" />
                        <div v-else
                             class="flex-1 min-h-0 w-full overflow-y-auto">
                            <div class="p-4 w-full"
                                 :class="pageHeader && 'max-w-[1040px] mx-auto'">
                                <div v-if="pageHeader"
                                     class="mb-6">
                                    <div v-if="pageHeader.eyebrow"
                                         class="eyebrow text-primary">
                                        {{ pageHeader.eyebrow }}
                                    </div>
                                    <h1 class="text-[28px] font-bold tracking-[-0.022em] leading-tight text-highlighted"
                                        :class="pageHeader.eyebrow ? 'mt-2.5' : ''">
                                        {{ pageHeader.title }}
                                    </h1>
                                    <p v-if="pageHeader.description"
                                       class="text-[15px] text-muted leading-relaxed max-w-[660px] mt-2.5">
                                        {{ pageHeader.description }}
                                    </p>
                                </div>
                                <slot />
                            </div>
                        </div>
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

            <!-- Floating agent launcher. Positioned inside the dashboard group so it
                 slides left with the layout when the panel opens, instead of hiding
                 under the panel. -->
            <UButton v-if="userStore.agentAvailable"
                     icon="i-lucide-sparkles"
                     size="xl"
                     aria-label="Toggle agent panel"
                     class="absolute bottom-6 right-6 z-30 rounded-full shadow-lg"
                     @click="agentOpen = !agentOpen" />
        </UDashboardGroup>

        <AgentPanel v-if="userStore.agentAvailable" />
    </div>
</template>
