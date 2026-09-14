<script setup lang="ts">
/** Frame of a Components view: the hub's view strip, then the page content in the standard frame. */
const views = useComponentViews()
</script>

<template>
    <PageNav :views="views" />
    <PageFrame>
        <slot />
    </PageFrame>
</template>
