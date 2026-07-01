<script setup lang="ts">
definePageMeta({
    title: 'Auth',
    layout: false,
    auth: false,
})

const route = useRoute()
const { exchangeCode } = useOAuthPopup()

const provider = route.params.provider?.toString()
if (!provider) {
    throw createError({
        statusCode: 404,
        statusMessage: 'Provider not found',
    })
}

const code = route.query.code as string | undefined
const status = ref<OAuthPopupStatus>(
    code ? OAuthPopupStatus.Loading : OAuthPopupStatus.MissingCode,
)
const errorDetail = ref('')

onMounted(async () => {
    if (code) {
        try {
            await exchangeCode(provider, code)
            status.value = OAuthPopupStatus.Success
        }
        catch (error) {
            errorDetail.value = oauthErrorDetail(error)
            status.value = OAuthPopupStatus.Failure
        }
    }
})
</script>

<template>
    <div class="h-screen flex items-center justify-center p-8">
        <UAlert v-if="status === OAuthPopupStatus.Loading"
                color="info"
                variant="subtle"
                icon="i-lucide-loader-circle"
                title="Authenticating"
                :description="`Connecting to ${provider}...`" />
        <UAlert v-else-if="status === OAuthPopupStatus.Success"
                color="success"
                variant="subtle"
                icon="i-lucide-circle-check"
                :title="`Successfully connected to ${provider}`"
                description="You can close this window." />
        <UAlert v-else-if="status === OAuthPopupStatus.MissingCode"
                color="error"
                variant="subtle"
                icon="i-lucide-circle-x"
                title="Authentication Error"
                description="Authorization code is missing." />
        <UAlert v-else
                color="error"
                variant="subtle"
                icon="i-lucide-circle-x"
                title="Authentication Failed"
                class="max-w-md">
            <template #description>
                <p>Something went wrong during authentication. You can close this window.</p>
                <UCollapsible v-if="errorDetail"
                              class="mt-2">
                    <button class="flex items-center gap-1 group cursor-pointer text-xs font-medium">
                        <UIcon name="i-lucide-chevron-right"
                               class="size-3.5 shrink-0 group-data-[state=open]:rotate-90 transition-transform duration-200" />
                        Details
                    </button>
                    <template #content>
                        <pre class="mt-1.5 max-h-64 overflow-auto whitespace-pre-wrap break-words rounded bg-error/10 p-2 text-xs">{{ errorDetail }}</pre>
                    </template>
                </UCollapsible>
            </template>
        </UAlert>
    </div>
</template>
