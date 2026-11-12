<script setup lang="ts">
import type { DropdownMenuItem } from '@nuxt/ui'

defineProps<{
    collapsed?: boolean
}>()

const userStore = useUserStore()
const appConfig = useAppConfig()
const colorMode = useColorMode()
const appVersion = useRuntimeConfig().public.version

function toggleColorMode() {
    colorMode.preference = colorMode.value === 'dark' ? 'light' : 'dark'
}

const displayName = computed(() => userStore.user?.name || userStore.user?.email || 'Account')

const items = computed<DropdownMenuItem[][]>(() => {
    const groups: DropdownMenuItem[][] = [
        [{
            label: displayName.value,
            avatar: { src: userStore.user?.avatar_url ?? undefined, alt: displayName.value },
            type: 'label',
        }],
    ]

    groups.push([{
        label: 'Profile',
        icon: appConfig.icons.user,
        to: '/settings/profile',
    }])

    if (userStore.isSuperAdmin) {
        groups.push([{
            label: 'Platform admin',
            icon: appConfig.icons.admin,
            to: '/admin',
        }])
    }

    groups.push(
        [{
            label: colorMode.value === 'dark' ? 'Light mode' : 'Dark mode',
            icon: colorMode.value === 'dark' ? appConfig.ui.icons.light : appConfig.ui.icons.dark,
            onSelect: toggleColorMode,
        }],
        [{
            label: 'Sign out',
            icon: appConfig.icons.signOut,
            onSelect: () => userStore.signOut(),
        }],
    )

    if (appVersion) groups.push([{ label: `Interloper v${appVersion}`, type: 'label' }])

    return groups
})
</script>

<template>
    <UDropdownMenu :items="items"
                   :content="{ align: 'start', side: 'top' }"
                   :ui="{ content: 'w-(--reka-dropdown-menu-trigger-width)' }">
        <UButton :avatar="{ src: userStore.user?.avatar_url ?? undefined, alt: displayName }"
                 :label="collapsed ? undefined : displayName"
                 :aria-label="collapsed ? displayName : undefined"
                 class="w-full justify-start"
                 variant="ghost"
                 color="neutral"
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
