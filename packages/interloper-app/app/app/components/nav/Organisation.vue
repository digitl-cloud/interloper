<script setup lang="ts">
import type { DropdownMenuItem } from '@nuxt/ui'
import type { Organisation } from '~/types/organisation'

defineProps<{
    collapsed?: boolean
}>()

const route = useRoute()
const orgStore = useOrganisationStore()

const userOrgs = ref<Organisation[]>([])
const orgsLoaded = ref(false)

async function loadUserOrgs() {
    if (orgsLoaded.value) return
    userOrgs.value = await orgStore.fetchOrganisations()
    orgsLoaded.value = true
}

const orgItems = computed<DropdownMenuItem[]>(() => {
    if (!orgsLoaded.value) return [{ label: 'Loading...', disabled: true }]
    if (userOrgs.value.length === 0) return [{ label: 'No organisations', disabled: true }]

    const currentId = orgStore.organisation?.id
    return userOrgs.value.map((org) => {
        if (org.id === currentId) {
            return {
                label: org.name,
                icon: 'i-lucide-building-2',
                type: 'checkbox' as const,
                checked: true,
            }
        }
        return {
            label: org.name,
            icon: 'i-lucide-building-2',
            onSelect: () => {
                orgStore.switchOrg(org.id)
                orgsLoaded.value = false
            },
        }
    })
})

const items = computed<DropdownMenuItem[][]>(() => [
    orgItems.value,
    [{
        label: 'Manage',
        icon: 'i-lucide-users',
        onSelect: () => navigateTo('/organization'),
    }],
])
</script>

<template>
    <UDropdownMenu :items="items"
                   :content="{ align: 'start', side: 'top' }"
                   :ui="{ content: 'w-(--reka-dropdown-menu-trigger-width)' }"
                   @update:open="(open: boolean) => { if (open) loadUserOrgs() }">
        <UButton :avatar="{ icon: 'i-lucide-building-2' }"
                 :label="collapsed ? undefined : (orgStore.organisation?.name || 'Organization')"
                 class="w-full justify-start data-[state=open]:bg-elevated"
                 variant="ghost"
                 color="neutral"
                 :square="collapsed"
                 :class="{ 'text-highlighted': route.path === '/organization' }"
                 truncate>
            <template v-if="!collapsed"
                      #trailing>
                <UIcon name="i-lucide-chevrons-up-down"
                       class="size-4 ms-auto text-dimmed" />
            </template>
        </UButton>
    </UDropdownMenu>
</template>
