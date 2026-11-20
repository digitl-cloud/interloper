<script setup lang="ts">
// The graph is the Assets page's default view now; old links land there with their query, an
// asset they selected (`?select=`) as the page's selection.
definePageMeta({
    redirect: (to) => {
        const { select, ...query } = to.query
        return { path: `/${to.params.org}/assets`, query: select ? { ...query, asset: select } : query }
    },
})
</script>

<template>
    <div />
</template>
