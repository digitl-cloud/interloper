<script setup lang="ts">
/**
 * A hub's sub-pages, in the page toolbar. A view without an explicit `active`
 * is active when its route is, so path views and `?tab=` views read the same
 * way; a path view returns to its page with the filters it was left with.
 */
import type { NavigationMenuItem } from '@nuxt/ui'

const props = defineProps<{ items: PageTab[] }>()

const withLastQuery = useLastQuery()

const navItems = computed<NavigationMenuItem[]>(() => props.items.map(item => ({
    label: item.label,
    icon: item.icon,
    to: withLastQuery(item.to),
    active: item.active,
    badge: item.badge,
})))
</script>

<template>
    <UNavigationMenu :items="navItems"
                     variant="pill"
                     color="neutral" />
</template>
