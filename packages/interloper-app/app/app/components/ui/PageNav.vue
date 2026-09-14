<script setup lang="ts">
/**
 * Strip of a hub's routed views, flush under the navbar: the dashboard
 * template's toolbar carrying a highlighted navigation menu. Views are
 * routes, so link semantics and the sidebar's active state come for free.
 */
import type { NavigationMenuItem } from '@nuxt/ui'
import type { NavPage } from '~/composables/navigation'

const props = defineProps<{
    views: NavPage[]
}>()

const items = computed<NavigationMenuItem[]>(() => props.views.map(({ label, icon, to }) => ({ label, icon, to })))
</script>

<template>
    <UDashboardToolbar>
        <UNavigationMenu :items="items"
                         highlight
                         class="-mx-1 flex-1" />
    </UDashboardToolbar>
</template>
