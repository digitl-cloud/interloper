<script setup lang="ts">
/**
 * Inline selection card rendered in the agent chat.
 *
 * Triggered by the agent's `request_user_selection` tool: the wizard's
 * SelectionCard rows, compacted for the agent panel — checkbox rows for
 * multi-select, plain accent-border rows for single. On confirm the parent
 * reports the selection back into the chat as the user's message so the
 * agent continues with it.
 */
import type { SelectionRequest } from '~/types/agent'

const props = defineProps<{
    request: SelectionRequest
}>()

const emit = defineEmits<{
    selected: [labels: string[], values: string[]]
}>()

const picked = ref<string[]>([])
const submitted = ref(false)

function toggle(value: string) {
    if (submitted.value) return
    if (!props.request.multi) {
        picked.value = [value]
        return
    }
    picked.value = picked.value.includes(value)
        ? picked.value.filter(v => v !== value)
        : [...picked.value, value]
}

const allSelected = computed(() => picked.value.length === props.request.options.length)

function toggleAll() {
    picked.value = allSelected.value ? [] : props.request.options.map(o => o.value)
}

const selectedLabels = computed(() =>
    props.request.options.filter(o => picked.value.includes(o.value)).map(o => o.label))

function confirm() {
    if (!picked.value.length) return
    submitted.value = true
    emit('selected', selectedLabels.value, [...picked.value])
}
</script>

<template>
    <div class="border border-default rounded-[13px] p-4 my-2 w-full min-w-72 max-w-md flex flex-col gap-3">
        <div class="flex items-center justify-between gap-2">
            <span class="text-[13px] font-semibold text-highlighted">{{ request.prompt }}</span>
            <UButton v-if="request.multi && request.options.length > 3 && !submitted"
                     :label="allSelected ? 'Deselect all' : 'Select all'"
                     variant="link"
                     size="xs"
                     class="shrink-0"
                     @click="toggleAll" />
        </div>

        <!-- Answered: locked summary -->
        <div v-if="submitted"
             class="flex items-center gap-2 text-[13px]">
            <UIcon name="i-lucide-check-circle-2"
                   class="size-4 text-success shrink-0" />
            <span class="text-muted truncate">{{ selectedLabels.join(', ') }}</span>
        </div>

        <template v-else>
            <div class="flex flex-col gap-1.5 max-h-64 overflow-y-auto">
                <SelectionCard v-for="option in request.options"
                               :key="option.value"
                               :as="request.multi ? 'div' : 'button'"
                               :selected="picked.includes(option.value)"
                               class="shrink-0"
                               @select="toggle(option.value)">
                    <div class="flex items-center gap-2.5 px-3 py-2"
                         :class="request.multi ? 'cursor-pointer' : ''">
                        <UCheckbox v-if="request.multi"
                                   :model-value="picked.includes(option.value)"
                                   @click.stop
                                   @update:model-value="toggle(option.value)" />
                        <span class="text-[13px] font-medium truncate">{{ option.label }}</span>
                        <UIcon v-if="!request.multi && picked.includes(option.value)"
                               name="i-lucide-check"
                               class="size-3.5 text-primary ml-auto shrink-0" />
                    </div>
                </SelectionCard>
            </div>

            <UButton :label="request.multi ? `Confirm selection (${picked.length})` : 'Confirm'"
                     :disabled="!picked.length"
                     block
                     @click="confirm" />
        </template>
    </div>
</template>
