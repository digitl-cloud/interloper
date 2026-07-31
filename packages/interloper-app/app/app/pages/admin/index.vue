<script setup lang="ts">
definePageMeta({
    title: 'Admin overview',
    layout: 'admin',
    middleware: 'super-admin',
    pageHeader: {
        eyebrow: 'Admin portal',
        title: 'Overview',
        description: 'Platform-wide health and activity at a glance.',
    },
})
</script>

<template>
    <div class="flex flex-col items-center justify-center gap-3 rounded-lg border border-dashed border-default py-24 text-center">
        <UIcon name="i-lucide-layout-dashboard"
               class="size-8 text-dimmed" />
        <p class="text-sm text-muted">
            Nothing here yet — platform metrics will land on this page.
        </p>
    </div>
</template>
