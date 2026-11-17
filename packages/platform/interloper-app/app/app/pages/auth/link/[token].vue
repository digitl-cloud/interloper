<script setup lang="ts">
import type { Identity } from '~/types/user'

definePageMeta({
    layout: false,
})

// Confirming waits for a click, never runs on load: mail scanners open the
// links in an email, and opening this page must change nothing.
const route = useRoute()
const { apiFetch } = useApi()
const appConfig = useAppConfig()

const status = ref<'ready' | 'confirming' | 'done' | 'error'>('ready')
const added = ref<Identity | null>(null)
const errorMessage = ref('')

async function confirmLink() {
    status.value = 'confirming'
    try {
        added.value = await apiFetch<Identity>('/auth/link/confirm', {
            method: 'POST',
            body: { token: route.params.token },
        })
        status.value = 'done'
    }
    catch (err) {
        errorMessage.value = errorDetail(err) ?? GENERIC_ERROR
        status.value = 'error'
    }
}
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6">
        <UCard class="w-full max-w-sm">
            <template #header>
                <div class="flex flex-col items-center gap-3 text-center">
                    <LogoIcon class="h-8 text-primary" />
                    <h1 class="text-2xl font-semibold text-highlighted">
                        Add a way to sign in
                    </h1>
                </div>
            </template>

            <div v-if="status === 'done'"
                 class="flex flex-col gap-4">
                <UAlert color="success"
                        :icon="appConfig.ui.icons.success"
                        :title="`${added?.provider?.label ?? 'The account'} now signs in to your Interloper account`" />
                <UButton label="Sign in"
                         block
                         to="/login" />
            </div>

            <div v-else-if="status === 'error'"
                 class="flex flex-col gap-4">
                <UAlert color="error"
                        :icon="appConfig.ui.icons.error"
                        title="Couldn't add the sign-in method"
                        :description="errorMessage" />
                <p class="text-sm text-muted">
                    Sign in again with the new account to get a fresh link.
                </p>
                <UButton label="Sign in again"
                         color="neutral"
                         variant="outline"
                         block
                         to="/login" />
            </div>

            <div v-else
                 class="flex flex-col gap-4">
                <p class="text-sm text-muted">
                    Someone signed in with a new account using your address. Confirm only if it was you:
                    that account will then sign in to your Interloper account. If it wasn't you, close this
                    page and nothing changes.
                </p>
                <UButton label="Confirm"
                         block
                         :loading="status === 'confirming'"
                         @click="confirmLink" />
            </div>
        </UCard>
    </div>
</template>
