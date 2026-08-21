<script setup lang="ts">
/**
 * Renders its content into the page navbar's right side (the layout's
 * #navbar-right outlet) — the design keeps page-level actions in the 56px
 * header, not in the content body. The outlet belongs to the layout, which
 * can re-render in the same flush as a client-side navigation, so wait one
 * tick for it to exist before teleporting.
 */
const ready = ref(false)
onMounted(async () => {
    await nextTick()
    ready.value = true
})
</script>

<template>
    <Teleport v-if="ready"
              to="#navbar-right">
        <slot />
    </Teleport>
</template>
