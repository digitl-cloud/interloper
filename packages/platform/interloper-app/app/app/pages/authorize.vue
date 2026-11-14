<script setup lang="ts">
import type { ConsentRequest } from '~/types/token'

definePageMeta({
    layout: false,
})

const route = useRoute()
const userStore = useUserStore()
const appConfig = useAppConfig()
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

/** Handle a failed consent call: sign in again on 401, show the reason otherwise. */
function fail(err: unknown) {
    const { status, statusCode } = err as { status?: number, statusCode?: number }
    if ((status ?? statusCode) === 401) {
        navigateTo({ path: '/login', query: { redirect: route.fullPath } })
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
        consent.value = await apiFetch<ConsentRequest>('/oauth/consent', { query: { request: sealed } })
    }
    catch (err) {
        fail(err)
    }
})

async function decide(approve: boolean) {
    deciding.value = approve ? 'approve' : 'deny'
    try {
        const { redirect_to } = await apiFetch<{ redirect_to: string }>('/oauth/consent', {
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
    <div class="flex min-h-svh w-full items-center justify-center p-6">
        <UCard class="w-full max-w-sm">
            <template #header>
                <div class="flex flex-col items-center gap-3 text-center">
                    <LogoIcon class="h-8 text-primary" />
                    <h1 class="text-2xl font-semibold text-highlighted">
                        Authorise application
                    </h1>
                </div>
            </template>

            <div v-if="errorMessage"
                 class="flex flex-col gap-4">
                <UAlert color="error"
                        :icon="appConfig.ui.icons.error"
                        title="Couldn't authorise"
                        :description="errorMessage" />
                <UButton label="Go home"
                         color="neutral"
                         variant="outline"
                         block
                         to="/" />
            </div>

            <div v-else-if="!consent"
                 class="flex justify-center py-6">
                <UIcon :name="appConfig.ui.icons.loading"
                       class="size-5 animate-spin text-dimmed" />
            </div>

            <div v-else
                 class="flex flex-col gap-4">
                <p class="text-sm wrap-break-word">
                    <span class="font-semibold text-highlighted">{{ consent.client_name }}</span>
                    <template v-if="consent.client_host">
                        (from <span class="font-semibold text-highlighted break-all">{{ consent.client_host }}</span>)
                    </template>
                    wants to act as you in every organisation you belong to, with your role in each.
                    It will return you to
                    <span class="font-semibold text-highlighted break-all">{{ consent.redirect_host }}</span>.
                </p>

                <div class="flex flex-col gap-2 rounded-lg bg-elevated p-3">
                    <UFormField label="Signed in as"
                                orientation="horizontal">
                        <span class="truncate text-highlighted">{{ userStore.user?.email }}</span>
                    </UFormField>
                    <UFormField label="Access to"
                                orientation="horizontal">
                        <span class="text-highlighted">{{ RESOURCES[consent.resource] }}</span>
                    </UFormField>
                </div>

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
</template>
