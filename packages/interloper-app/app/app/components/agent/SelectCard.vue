<script setup lang="ts">
/**
 * Inline selection card rendered in the agent chat.
 *
 * Triggered by the agent's `request_user_selection` tool: renders the
 * choices as checkboxes (multi) or radios (single). On confirm the parent
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

const checked = ref<string[]>([])
const single = ref<string | undefined>(undefined)
const submitted = ref(false)

const radioItems = computed(() => props.request.options.map(o => ({ label: o.label, value: o.value })))

const selectedValues = computed(() => props.request.multi ? checked.value : (single.value ? [single.value] : []))

const selectedLabels = computed(() =>
    props.request.options.filter(o => selectedValues.value.includes(o.value)).map(o => o.label))

function confirm() {
    if (!selectedValues.value.length) return
    submitted.value = true
    emit('selected', selectedLabels.value, [...selectedValues.value])
}
</script>

<template>
    <div class="border border-default rounded-[14px] p-5 my-2 w-full min-w-72 max-w-md">
        <div class="flex flex-col gap-4">
            <div class="text-sm font-semibold">
                {{ request.prompt }}
            </div>

            <!-- Answered: locked summary -->
            <div v-if="submitted"
                 class="flex items-center gap-2 text-sm">
                <UIcon name="i-lucide-check-circle-2"
                       class="size-4 text-success shrink-0" />
                <span class="text-muted">{{ selectedLabels.join(', ') }}</span>
            </div>

            <template v-else>
                <UCheckboxGroup v-if="request.multi"
                                v-model="checked"
                                :items="radioItems" />
                <URadioGroup v-else
                             v-model="single"
                             :items="radioItems" />

                <UButton :label="request.multi ? `Confirm selection (${selectedValues.length})` : 'Confirm'"
                         :disabled="!selectedValues.length"
                         block
                         @click="confirm" />
            </template>
        </div>
    </div>
</template>
