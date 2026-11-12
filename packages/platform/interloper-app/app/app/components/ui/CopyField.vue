<script setup lang="ts">
/**
 * A value to copy, such as a secret shown once: a read-only input with a copy
 * button that confirms the copy for a moment. Goes inside a `UFormField`,
 * which labels it. Focusing the input selects the whole value, so it can be
 * copied by hand too.
 */
const props = defineProps<{
    /** The value shown and copied. */
    value: string
}>()

const appConfig = useAppConfig()
const { copy, copied } = useClipboard({ copiedDuring: 2000 })

function selectAll(event: FocusEvent) {
    (event.target as HTMLInputElement).select()
}
</script>

<template>
    <UInput :model-value="props.value"
            readonly
            class="w-full"
            @focus="selectAll">
        <template #trailing>
            <UTooltip :text="copied ? 'Copied' : 'Copy'">
                <UButton :icon="copied ? appConfig.ui.icons.copyCheck : appConfig.ui.icons.copy"
                         :color="copied ? 'success' : 'neutral'"
                         variant="link"
                         size="sm"
                         :aria-label="copied ? 'Copied' : 'Copy'"
                         @click="copy(props.value)" />
            </UTooltip>
        </template>
    </UInput>
</template>
