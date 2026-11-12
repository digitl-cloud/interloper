<script setup lang="ts">
definePageMeta({
    layout: false,
    auth: false,
})

const route = useRoute()
const appConfig = useAppConfig()
const { exchangeCode, reportResult } = useOAuthPopup()

const provider = route.params.provider?.toString()
if (!provider) {
    throw createError({
        statusCode: 404,
        statusMessage: 'Provider not found',
    })
}

const code = route.query.code as string | undefined
const providerError = route.query.error as string | undefined
const state = route.query.state as string | undefined

const status = ref<OAuthPopupStatus>(OAuthPopupStatus.Loading)
const errorDetail = ref('')

function fail(error: string, failureStatus = OAuthPopupStatus.Failure) {
    errorDetail.value = error
    status.value = failureStatus
    reportResult({ type: OAuthPopupStatus.Failure, state, error })
}

// Every terminal state reports back to the opener, so its pending sign-in
// settles instead of spinning forever.
onMounted(async () => {
    if (providerError) {
        // Provider redirected back with an error (e.g. the user denied consent).
        const description = route.query.error_description as string | undefined
        fail(description ? `${providerError}: ${description}` : providerError)
    }
    else if (!code) {
        fail('Authorisation code is missing', OAuthPopupStatus.MissingCode)
    }
    else {
        try {
            const tokens = await exchangeCode(provider, code)
            status.value = OAuthPopupStatus.Success
            reportResult({ type: OAuthPopupStatus.Success, state, tokens })
        }
        catch (error) {
            fail(oauthErrorDetail(error))
        }
    }
})
</script>

<template>
    <div class="flex min-h-svh w-full items-center justify-center p-6">
        <UCard class="w-full max-w-sm">
            <template #header>
                <div class="flex flex-col items-center gap-3 text-center">
                    <LogoIcon class="h-8 text-primary" />
                    <h1 class="text-2xl font-semibold text-highlighted">
                        Authentication
                    </h1>
                </div>
            </template>

            <UAlert v-if="status === OAuthPopupStatus.Loading"
                    color="info"
                    :icon="appConfig.ui.icons.loading"
                    title="Authenticating"
                    :description="`Connecting to ${provider}…`"
                    :ui="{ icon: 'animate-spin' }" />
            <UAlert v-else-if="status === OAuthPopupStatus.Success"
                    color="success"
                    :icon="appConfig.ui.icons.success"
                    :title="`Connected to ${provider}`"
                    description="You can close this window." />
            <UAlert v-else-if="status === OAuthPopupStatus.MissingCode"
                    color="error"
                    :icon="appConfig.ui.icons.error"
                    title="Couldn't authenticate"
                    description="The authorisation code is missing. You can close this window." />
            <UAlert v-else
                    color="error"
                    :icon="appConfig.ui.icons.error"
                    title="Couldn't authenticate">
                <template #description>
                    <p>Something went wrong during authentication. You can close this window.</p>
                    <UCollapsible v-if="errorDetail"
                                  class="mt-2">
                        <UButton label="Details"
                                 color="error"
                                 variant="link"
                                 size="xs"
                                 :trailing-icon="appConfig.ui.icons.chevronDown"
                                 class="group"
                                 :ui="{ trailingIcon: 'transition-transform duration-200 group-data-[state=open]:rotate-180' }" />
                        <template #content>
                            <pre class="mt-1.5 max-h-64 overflow-auto rounded-md bg-error/10 p-2 font-mono text-xs whitespace-pre-wrap break-words">{{ errorDetail }}</pre>
                        </template>
                    </UCollapsible>
                </template>
            </UAlert>
        </UCard>
    </div>
</template>
