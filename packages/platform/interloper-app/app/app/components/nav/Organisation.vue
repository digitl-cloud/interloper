<script setup lang="ts">
import type { DropdownMenuItem } from '@nuxt/ui'

defineProps<{
    collapsed?: boolean
}>()

const route = useRoute()
const appConfig = useAppConfig()
const userStore = useUserStore()
const orgStore = useOrganisationStore()

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
    orgItems.value,
    [{
        label: 'Manage',
        icon: appConfig.icons.members,
        to: orgStore.path('/organization'),
    }],
])

/** The organisation's own pages light the switcher up, as the sidebar does its destinations. */
const onOrganisationPages = computed(() => route.path.startsWith(orgStore.path('/organization')))
</script>

<template>
    <UDropdownMenu :items="items"
                   :content="{ align: 'start', side: 'top' }"
                   :ui="{ content: 'w-(--reka-dropdown-menu-trigger-width)' }">
        <UButton :avatar="{ icon: appConfig.icons.organisation }"
                 :label="collapsed ? undefined : (orgStore.organisation?.name || 'Organisation')"
                 :aria-label="collapsed ? (orgStore.organisation?.name || 'Organisation') : undefined"
                 class="w-full justify-start"
                 variant="ghost"
                 color="neutral"
                 :active="onOrganisationPages"
                 active-variant="soft"
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
