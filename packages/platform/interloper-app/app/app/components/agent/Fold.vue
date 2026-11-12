<script setup lang="ts">
/**
 * A folded row of the conversation, closed by default: an icon, a label
 * (shimmering while `live`), an optional dimmed `suffix` and the chevron,
 * with what unfolds in the default slot. The `leading` slot opens the row
 * (the message's avatar). The steps trail and the summary of earlier turns
 * both fold behind it.
 */
withDefaults(defineProps<{
    icon: string
    text: string
    suffix?: string
    live?: boolean
    /** Something in the fold failed: the icon reads as an error. */
    error?: boolean
}>(), { suffix: undefined, live: false, error: false })

defineSlots<{
    default?: () => unknown
    leading?: () => unknown
}>()

const appConfig = useAppConfig()
</script>

<template>
    <UCollapsible class="w-full">
        <button type="button"
                class="group flex min-h-6 w-full cursor-pointer items-center gap-1.5 text-left text-sm text-muted transition-colors hover:text-highlighted">
            <slot name="leading" />
            <UIcon :name="icon"
                   class="size-4 shrink-0"
                   :class="error && 'text-error'" />
            <UChatShimmer v-if="live"
                          :text="text" />
            <span v-else>{{ text }}</span>
            <span v-if="suffix"
                  class="text-dimmed">{{ suffix }}</span>
            <UIcon :name="appConfig.ui.icons.chevronDown"
                   class="size-4 shrink-0 transition-transform duration-200 group-data-[state=open]:rotate-180" />
        </button>

        <template #content>
            <slot />
        </template>
    </UCollapsible>
</template>
