<script setup lang="ts">
// Resource lists moved under /components (/resources/connection → /components/connections); keep old bookmarks working.
definePageMeta({ middleware: [to => navigateTo(kindPath(to.params.kind as string), { redirectCode: 301 })] })
</script>

<template>
    <div />
</template>
