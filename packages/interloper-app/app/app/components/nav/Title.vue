<script setup lang="ts">
/**
 * Renders its content into the page navbar's title area (the layout's
 * #navbar-title outlet, present when the page sets meta customNavbar).
 * Same one-tick wait as NavActions, for client-side navigations.
 */
const ready = ref(false)
onMounted(async () => {
    await nextTick()
    ready.value = true
})
</script>

<template>
    <Teleport v-if="ready"
              to="#navbar-title">
        <slot />
    </Teleport>
</template>
