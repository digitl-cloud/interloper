<script setup lang="ts">
import type { NavigationMenuItem } from '@nuxt/ui'

const route = useRoute()

const userStore = useUserStore()
const {
    open: commandPaletteOpen,
    searchTerm: commandPaletteSearchTerm,
    loading: commandPaletteLoading,
    groups: commandPaletteGroups,
} = useCommandPalette()
const { open: agentOpen, width: agentWidth, dragging: agentDragging } = useAgentPanel()
const appVersion = useRuntimeConfig().public.version

/** Design page header rendered by the layout, declared via definePageMeta({ pageHeader }). */
interface PageHeaderMeta {
    title: string
    description?: string
    eyebrow?: string
}
const pageHeader = computed(() => route.meta.pageHeader as PageHeaderMeta | undefined)

/** Navbar title; pages with a bespoke navbar (run/backfill) teleport into it instead. */
const pageTitle = computed(() => pageHeader.value?.title ?? (route.meta.title as string | undefined))
/** Pages that fill the navbar themselves (crumb + id + status) via #navbar-title. */
const customNavbar = computed(() => !!route.meta.customNavbar)

const navSections = useNavSections()

const items = computed<NavigationMenuItem[]>(() => navSections.value.flatMap((section, index) => [
    {
        label: section.label,
        type: 'label' as const,
        class: index > 0 ? 'mt-2' : undefined,
    },
    ...section.pages.map(page => ({
        label: page.label,
        icon: page.icon,
        to: page.to,
        active: route.path === page.to || route.path.startsWith(`${page.to}/`),
    })),
]))
</script>

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
                    <UButton :label="collapsed ? undefined : 'Search...'"
                             icon="i-lucide-search"
                             color="neutral"
                             variant="outline"
                             block
                             class="bg-default text-dimmed"
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
                                     color="neutral"
                                     orientation="vertical" />
                </template>

                <template #footer="{ collapsed }">
                    <div class="flex flex-col gap-1 w-full">
                        <NavOrganisation :collapsed="collapsed" />
                        <NavUser :collapsed="collapsed" />
                        <span v-if="!collapsed && appVersion"
                              class="px-2.5 text-[10px] text-dimmed">
                            v{{ appVersion }}
                        </span>
                    </div>
                </template>
            </UDashboardSidebar>

            <UDashboardPanel
                             :ui="{ body: '!p-0 !gap-0 overflow-hidden [&>*]:flex-1 [&>*]:flex [&>*]:flex-col [&>*]:min-h-0' }">
                <template #header>
                    <UDashboardNavbar :title="customNavbar ? undefined : pageTitle"
                                      :ui="{ root: 'sm:px-4', title: 'text-[15px]' }">
                        <template #leading>
                            <UDashboardSidebarCollapse />
                        </template>
                        <template v-if="customNavbar"
                                  #title>
                            <!-- Detail pages teleport their crumb + status here. -->
                            <div id="navbar-title"
                                 class="flex min-w-0 items-center gap-2.5" />
                        </template>
                        <template #right>
                            <div id="navbar-right"
                                 class="flex items-center gap-2" />
                        </template>
                    </UDashboardNavbar>
                </template>
                <template #body>
                    <!-- Full-bleed pages (canvas/split views) manage their own frame. -->
                    <slot v-if="route.meta.fullBleed" />
                    <div v-else
                         class="flex-1 min-h-0 w-full overflow-y-auto">
                        <div class="p-6 w-full">
                            <div v-if="pageHeader"
                                 class="mb-6 max-w-[660px]">
                                <p v-if="pageHeader.eyebrow"
                                   class="eyebrow text-primary">{{ pageHeader.eyebrow }}</p>
                                <h1 class="mt-2.5 text-[28px] font-bold leading-tight tracking-[-0.022em]">{{ pageHeader.title }}</h1>
                                <p v-if="pageHeader.description"
                                   class="mt-2.5 text-[15px] leading-relaxed text-muted">{{ pageHeader.description }}</p>
                            </div>
                            <slot />
                        </div>
                    </div>
                </template>
            </UDashboardPanel>
            <UModal v-model:open="commandPaletteOpen">
                <template #content>
                    <UCommandPalette v-model:search-term="commandPaletteSearchTerm"
                                     :groups="commandPaletteGroups"
                                     :loading="commandPaletteLoading"
                                     :fuse="{ fuseOptions: { keys: ['label', 'suffix', 'keywords'] } }"
                                     placeholder="Search..."
                                     close
                                     @update:open="commandPaletteOpen = $event" />
                </template>
            </UModal>

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
