<script setup lang="ts">
import type { DropdownMenuItem } from '@nuxt/ui'

defineProps<{
    collapsed?: boolean
}>()

const userStore = useUserStore()
const colorMode = useColorMode()

function toggleColorMode() {
    colorMode.preference = colorMode.value === 'dark' ? 'light' : 'dark'
}

const displayName = computed(() => userStore.user?.name || userStore.user?.email || 'Account')
const initials = computed(() => {
    const name = userStore.user?.name?.trim()
    if (name && name.length > 0) {
        const parts = name.split(/\s+/)
        const first = parts[0]?.[0] ?? ''
        const last = parts.length > 1 ? (parts[parts.length - 1]?.[0] ?? '') : ''
        return last ? (first + last).toUpperCase() : first.toUpperCase()
    }
    return userStore.user?.email?.charAt(0).toUpperCase() ?? '?'
})

const items = computed<DropdownMenuItem[][]>(() => {
    const groups: DropdownMenuItem[][] = [
        [{
            label: displayName.value,
            avatar: { text: initials.value },
            type: 'label',
        }],
    ]

    if (userStore.isSuperAdmin) {
        groups.push([{
            label: 'Platform admin',
            icon: 'i-lucide-shield',
            onSelect: () => navigateTo('/admin'),
        }])
    }

    groups.push(
        [
            {
                label: 'Settings',
                icon: 'i-lucide-sliders-horizontal',
                onSelect: () => navigateTo('/settings'),
            },
            {
                label: colorMode.value === 'dark' ? 'Light Mode' : 'Dark Mode',
                icon: colorMode.value === 'dark' ? 'i-lucide-sun' : 'i-lucide-moon',
                onSelect: toggleColorMode,
            },
        ],
        [{
            label: 'Log out',
            icon: 'i-lucide-log-out',
            onSelect: () => userStore.signOut(),
        }],
    )

    return groups
})
</script>

<template>
    <UDropdownMenu :items="items"
                   :content="{ align: 'end', side: 'right' }">
        <UButton :avatar="{ text: initials }"
                 :label="collapsed ? undefined : displayName"
                 class="w-full justify-start"
                 variant="ghost"
                 color="neutral"
                 :square="collapsed"
                 truncate>
            <template v-if="!collapsed"
                      #trailing>
                <UIcon name="i-lucide-chevron-right"
                       class="size-4 ms-auto text-muted" />
            </template>
        </UButton>
    </UDropdownMenu>
</template>
