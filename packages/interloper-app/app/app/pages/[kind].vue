<script setup lang="ts">
// Component lists moved under /components (/sources → /components/sources); keep old bookmarks working.
import { RESOURCE_KINDS } from '~/types/catalog'

const LEGACY_SLUGS = ['source', 'destination', 'job', 'hook', ...RESOURCE_KINDS].map(kindSlug)

definePageMeta({
    validate: route => LEGACY_SLUGS.includes(route.params.kind as string),
    middleware: [to => navigateTo(`/components/${to.params.kind}`, { redirectCode: 301 })],
})
</script>

<template>
    <div />
</template>
