<script setup lang="ts">
import type { ConsentRequest } from '~/types/token'

definePageMeta({
    layout: false,
})

const route = useRoute()
const userStore = useUserStore()
const { apiFetch } = useApi()

const RESOURCES: Record<ConsentRequest['resource'], string> = {
    mcp: 'Interloper MCP server',
    api: 'Interloper API',
}

const sealed = typeof route.query.request === 'string' ? route.query.request : ''
const consent = ref<ConsentRequest | null>(null)
const errorMessage = ref('')
const deciding = ref<'approve' | 'deny' | null>(null)

/** The message a refused consent call carries: the API's `detail`, or the OAuth error's description. */
function refusal(err: unknown): string {
    const description = (err as { data?: { error_description?: unknown } })?.data?.error_description
    if (typeof description === 'string' && description) return description
    return errorDetail(err) ?? 'This authorisation request can no longer be answered; start again from the application.'
}

/** Handle a failed consent call: log in again on 401, show the reason otherwise. */
function fail(err: unknown) {
    const { status, statusCode } = err as { status?: number, statusCode?: number }
    if ((status ?? statusCode) === 401) {
        userStore.signIn(route.fullPath)
        return
    }
    errorMessage.value = refusal(err)
}

onMounted(async () => {
    if (!sealed) {
        errorMessage.value = 'This authorisation request is incomplete; start again from the application.'
        return
    }
    try {
        consent.value = await apiFetch<ConsentRequest>('/auth/oauth/consent', { query: { request: sealed } })
    }
    catch (err) {
        fail(err)
    }
})

async function decide(approve: boolean) {
    deciding.value = approve ? 'approve' : 'deny'
    try {
        const { redirect_to } = await apiFetch<{ redirect_to: string }>('/auth/oauth/consent', {
            method: 'POST',
            body: { request: sealed, approve },
        })
        // Only ever the API's answer: the server validated this URI when the client registered.
        window.location.href = redirect_to
    }
    catch (err) {
        deciding.value = null
        fail(err)
    }
}
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6 md:p-10">
        <div class="w-full max-w-md">
            <UCard>
                <template #header>
                    <div class="text-center">
                        <LogoIcon class="h-8 mx-auto text-primary" />
                        <h2 class="text-2xl font-semibold tracking-[-0.01em] mt-3">
                            Authorise application
                        </h2>
                    </div>
                </template>

                <div v-if="errorMessage"
                     class="flex flex-col gap-4">
                    <UAlert color="error"
                            title="Unable to authorise"
                            :description="errorMessage"
                            icon="i-lucide-alert-circle" />
                    <UButton label="Go Home"
                             color="neutral"
                             variant="outline"
                             block
                             to="/" />
                </div>

                <div v-else-if="!consent"
                     class="flex justify-center py-6">
                    <UIcon name="i-lucide-loader-2"
                           class="size-8 animate-spin text-primary" />
                </div>

                <div v-else
                     class="flex flex-col gap-5">
                    <p class="text-sm wrap-break-word">
                        <span class="font-semibold text-highlighted">{{ consent.client_name }}</span>
                        wants to act as you in every organisation you belong to, with your role in each.
                        It will return you to
                        <span class="font-mono font-semibold text-highlighted break-all">{{ consent.redirect_host }}</span>.
                    </p>

                    <dl class="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 rounded-md bg-elevated p-3 text-sm">
                        <dt class="text-muted">Signed in as</dt>
                        <dd class="truncate text-highlighted">{{ userStore.user?.email }}</dd>
                        <dt class="text-muted">Access to</dt>
                        <dd class="text-highlighted">{{ RESOURCES[consent.resource] }}</dd>
                    </dl>

                    <p class="text-xs text-muted wrap-break-word">
                        Approve only if you started this from {{ consent.client_name }}. You can revoke it at any time
                        in Settings, under Authorised apps.
                    </p>

                    <div class="flex gap-3">
                        <UButton label="Deny"
                                 color="neutral"
                                 variant="outline"
                                 block
                                 class="flex-1"
                                 :loading="deciding === 'deny'"
                                 :disabled="deciding !== null"
                                 @click="decide(false)" />
                        <UButton label="Approve"
                                 block
                                 class="flex-1"
                                 :loading="deciding === 'approve'"
                                 :disabled="deciding !== null"
                                 @click="decide(true)" />
                    </div>
                </div>
            </UCard>
        </div>
    </div>
</template>
