<script setup lang="ts">
// The collection is the Assets page's table now; old links land there with their query.
definePageMeta({ redirect: to => ({ path: `/${to.params.org}/assets`, query: { ...to.query, view: 'table' } }) })
</script>

<template>
    <div />
</template>
