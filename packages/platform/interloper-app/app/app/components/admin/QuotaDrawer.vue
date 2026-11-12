<script setup lang="ts">
/**
 * Right drawer for editing an organisation's quota overrides.
 * The quota set comes from the API's field descriptors (key + label +
 * instance default), so a new quota needs no change here. String-typed
 * fields so an emptied input means "inherit the instance default"; each
 * field hints whether it inherits or overrides.
 */
import type { AdminQuotaField, AdminQuotaLimits } from '~/types/admin'

const open = defineModel<boolean>('open', { required: true })

const props = defineProps<{
    orgId: string
    orgName: string
    /** The org's current overrides (nulls where inherited). */
    limits: AdminQuotaLimits | null
    /** The quotas to edit, with the defaults the empty fields fall back to. */
    fields: AdminQuotaField[]
}>()

const emit = defineEmits<{ saved: [] }>()

const adminStore = useAdminStore()
const toast = useToast()

const form = ref<Record<string, string>>({})
const saving = ref(false)

watch(open, (opened) => {
    if (!opened) return
    form.value = Object.fromEntries(
        props.fields.map(field => [field.key, props.limits?.[field.key]?.toString() ?? '']),
    )
})

function defaultLabel(field: AdminQuotaField): string {
    return field.default != null ? field.default.toLocaleString() : 'unlimited'
}

function hint(field: AdminQuotaField): string {
    return form.value[field.key] === ''
        ? `Inheriting ${defaultLabel(field)}`
        : `Overrides the instance default of ${defaultLabel(field)}`
}

async function submit() {
    saving.value = true
    try {
        await adminStore.updateOrgQuota(props.orgId, Object.fromEntries(
            props.fields.map(field => [field.key, form.value[field.key] === '' ? null : Number(form.value[field.key])]),
        ))
        toast.add({ title: `Quota limits updated for ${props.orgName}`, color: 'success' })
        open.value = false
        emit('saved')
    }
    catch (err) {
        toast.add(errorToast(err, "Couldn't update quota limits"))
    }
    finally {
        saving.value = false
    }
}
</script>

<template>
    <UDrawer v-model:open="open"
             direction="right"
             :handle="false"
             :handle-only="true"
             close
             title="Edit quotas"
             :description="orgName">
        <template #body>
            <div class="flex flex-col gap-4">
                <p class="text-sm text-muted">
                    Leave a field empty to inherit the instance default.
                </p>
                <UFormField v-for="field in fields"
                            :key="field.key"
                            :label="field.label"
                            :help="hint(field)">
                    <UInput v-model="form[field.key]"
                            type="number"
                            min="0"
                            :placeholder="`Instance default (${defaultLabel(field)})`"
                            class="w-full"
                            @keydown.enter="submit" />
                </UFormField>
            </div>
        </template>
        <template #footer>
            <div class="flex w-full justify-between gap-2">
                <UButton label="Cancel"
                         color="neutral"
                         variant="outline"
                         @click="open = false" />
                <UButton label="Save"
                         :disabled="saving"
                         :loading="saving"
                         @click="submit" />
            </div>
        </template>
    </UDrawer>
</template>
