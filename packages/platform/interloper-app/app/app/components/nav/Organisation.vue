<script setup lang="ts">
import type { DropdownMenuItem } from '@nuxt/ui'

/**
 * The organisation's menu, at the sidebar's foot: its own pages first, then
 * the organisations to switch to. It is ringed while one of its pages shows,
 * as the sidebar's entries are.
 */
defineProps<{
    collapsed?: boolean
}>()

const route = useRoute()
const appConfig = useAppConfig()
const userStore = useUserStore()
const orgStore = useOrganisationStore()
const organizationViews = useOrganizationViews()

/** Every organisation lives at its own address, so switching is following a link; another tab keeps its own. */
const orgItems = computed<DropdownMenuItem[]>(() => {
    const memberships = userStore.user?.memberships ?? []
    if (memberships.length === 0) return [{ label: 'No organisations', disabled: true }]

    return memberships.map((membership) => {
        if (membership.id === orgStore.organisation?.id) {
            return {
                label: membership.name,
                icon: appConfig.icons.organisation,
                type: 'checkbox' as const,
                checked: true,
            }
        }
        return {
            label: membership.name,
            icon: appConfig.icons.organisation,
            to: `/${membership.key}`,
        }
    })
})

const items = computed<DropdownMenuItem[][]>(() => [
    organizationViews.value.map(view => ({ label: view.label, icon: view.icon, to: view.to })),
    [{ label: 'Switch organisation', type: 'label' as const }, ...orgItems.value],
])

const onOrganisationPages = computed(() => organizationViews.value.some(view => isNavActive(view, route.path)))
</script>

<template>
    <UDropdownMenu :items="items"
                   :content="{ align: 'start', side: 'top' }"
                   :ui="{ content: 'w-(--reka-dropdown-menu-trigger-width)' }">
        <UButton :avatar="{ icon: appConfig.icons.organisation }"
                 :label="collapsed ? undefined : (orgStore.organisation?.name || 'Organisation')"
                 :aria-label="collapsed ? (orgStore.organisation?.name || 'Organisation') : undefined"
                 class="w-full justify-start"
                 variant="soft"
                 color="neutral"
                 :active="onOrganisationPages"
                 active-variant="subtle"
                 :square="collapsed"
                 truncate>
            <template v-if="!collapsed"
                      #trailing>
                <UIcon :name="appConfig.icons.switcher"
                       class="ms-auto size-4 text-dimmed" />
            </template>
        </UButton>
    </UDropdownMenu>
</template>
