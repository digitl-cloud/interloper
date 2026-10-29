<script setup lang="ts">
import type { DropdownMenuItem } from '@nuxt/ui'

defineProps<{
    collapsed?: boolean
}>()

const route = useRoute()
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
                icon: 'i-lucide-building-2',
                type: 'checkbox' as const,
                checked: true,
            }
        }
        return {
            label: membership.name,
            icon: 'i-lucide-building-2',
            to: `/${membership.key}`,
        }
    })
})

const items = computed<DropdownMenuItem[][]>(() => [
    orgItems.value,
    [{
        label: 'Manage',
        icon: 'i-lucide-users',
        to: orgStore.path('/organization'),
    }],
])
</script>

<template>
    <UDropdownMenu :items="items"
                   :content="{ align: 'start', side: 'top' }"
                   :ui="{ content: 'w-(--reka-dropdown-menu-trigger-width)' }">
        <UButton :avatar="{ icon: 'i-lucide-building-2' }"
                 :label="collapsed ? undefined : (orgStore.organisation?.name || 'Organization')"
                 class="w-full justify-start data-[state=open]:bg-elevated"
                 variant="ghost"
                 color="neutral"
                 :square="collapsed"
                 :class="{ 'text-highlighted': route.path.startsWith(orgStore.path('/organization')) }"
                 truncate>
            <template v-if="!collapsed"
                      #trailing>
                <UIcon name="i-lucide-chevrons-up-down"
                       class="size-4 ms-auto text-dimmed" />
            </template>
        </UButton>
    </UDropdownMenu>
</template>
