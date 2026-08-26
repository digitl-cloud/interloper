<script setup lang="ts">
import type { CreatedToken } from '~/types/token'

const open = defineModel<boolean>('open', { default: false })

const { apiFetch } = useApi()
const toast = useToast()

const emit = defineEmits<{
    created: []
}>()

const name = ref('')
const expiry = ref('90')
const submitting = ref(false)
const created = ref<CreatedToken | null>(null)
const copied = ref(false)

const expiryOptions = [
    { label: '30 days', value: '30' },
    { label: '90 days', value: '90' },
    { label: '1 year', value: '365' },
    { label: 'No expiry', value: 'never' },
]

watch(open, (isOpen) => {
    if (!isOpen) {
        name.value = ''
        expiry.value = '90'
        created.value = null
        copied.value = false
    }
})

const canSubmit = computed(() => name.value.trim().length > 0 && !submitting.value)

async function submit() {
    if (!canSubmit.value) return
    submitting.value = true
    try {
        created.value = await apiFetch<CreatedToken>('/tokens', {
            method: 'POST',
            body: {
                name: name.value.trim(),
                expires_in_days: expiry.value === 'never' ? null : Number(expiry.value),
            },
        })
        emit('created')
    }
    catch (err) {
        toast.add(errorToast(err, 'Failed to create token'))
    }
    finally {
        submitting.value = false
    }
}

async function copy() {
    if (!created.value) return
    await navigator.clipboard.writeText(created.value.token)
    copied.value = true
}
</script>

<template>
    <UModal v-model:open="open"
            title="New token"
            :ui="{ footer: 'justify-end' }">
        <template #body>
            <div v-if="!created"
                 class="flex flex-col gap-4">
                <UFormField label="Name"
                            help="What this token is for, e.g. “MCP — laptop”.">
                    <UInput v-model="name"
                            placeholder="Token name"
                            class="w-full"
                            autofocus
                            @keydown.enter="submit" />
                </UFormField>
                <UFormField label="Expiration">
                    <USelect v-model="expiry"
                             :items="expiryOptions"
                             value-key="value"
                             class="w-40" />
                </UFormField>
            </div>

            <div v-else
                 class="flex flex-col gap-3">
                <UAlert color="warning"
                        variant="subtle"
                        icon="i-lucide-triangle-alert"
                        title="Copy your token now"
                        description="It is shown only once and cannot be recovered later." />
                <div class="flex items-center gap-2">
                    <UInput :model-value="created.token"
                            readonly
                            class="min-w-0 flex-1"
                            :ui="{ base: 'font-mono text-[13px]' }" />
                    <UButton :icon="copied ? 'i-lucide-check' : 'i-lucide-copy'"
                             :label="copied ? 'Copied' : 'Copy'"
                             color="neutral"
                             variant="outline"
                             @click="copy" />
                </div>
            </div>
        </template>

        <template #footer>
            <template v-if="!created">
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="open = false" />
                <UButton label="Create token"
                         icon="i-lucide-key"
                         :disabled="!canSubmit"
                         :loading="submitting"
                         @click="submit" />
            </template>
            <UButton v-else
                     label="Done"
                     @click="open = false" />
        </template>
    </UModal>
</template>
