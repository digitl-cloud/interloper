<template>
    <UApp :toaster="{ position: 'bottom-center' }">
        <NuxtLayout>
            <NuxtPage />
        </NuxtLayout>
    </UApp>
</template>

<style>
html,
body,
#__nuxt {
    height: 100%;
    margin: 0;
    padding: 0;
    overflow: hidden;
}

html {
    font-family: 'Inter', sans-serif;
}
</style>
