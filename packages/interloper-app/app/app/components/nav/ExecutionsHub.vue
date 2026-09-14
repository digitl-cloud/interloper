<script setup lang="ts">
/** Frame of an Executions view: the hub's view strip, then the page content in the standard frame. */
</script>

<template>
    <PageNav :views="EXECUTION_VIEWS" />
    <PageFrame>
        <slot />
    </PageFrame>
</template>
