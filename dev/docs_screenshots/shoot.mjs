import { chromium } from 'playwright-core'
import fs from 'node:fs'
import path from 'node:path'

const [base, tokenFile, outDir, only] = process.argv.slice(2)
const token = fs.readFileSync(tokenFile, 'utf8').trim()
fs.mkdirSync(outDir, { recursive: true })

const dialog = (page) => page.locator('[role="dialog"]').last()
const next = async (page) => {
    await dialog(page).getByRole('button', { name: /^next/i }).first().click()
    await page.waitForTimeout(700)
}
const openWizard = async (page, label) => {
    await page.getByRole('button', { name: label }).first().click()
    await page.waitForTimeout(800)
}
const pickTile = async (page, name) => {
    await dialog(page).getByText(name, { exact: true }).first().click()
    await page.waitForTimeout(400)
}

const shots = [
    { name: 'timeline', path: '/timeline' },
    { name: 'graph', path: '/graph', actions: async (page) => {
        await page.getByText('Asset', { exact: true }).last().click().catch(() => {})
        await page.waitForTimeout(1500)
        await page.getByRole('button', { name: /fit/i }).first().click().catch(() => {})
        await page.waitForTimeout(800)
    } },
    { name: 'collection', path: '/collection', actions: async (page) => {
        await page.getByRole('button', { name: /expand all/i }).first().click().catch(() => {})
        await page.waitForTimeout(800)
    } },
    { name: 'sources', path: '/components/sources' },
    { name: 'destinations', path: '/components/destinations' },
    { name: 'connections', path: '/components/connections' },
    { name: 'jobs', path: '/components/jobs' },
    { name: 'hooks', path: '/components/hooks' },
    { name: 'executions', path: '/executions/runs' },
    { name: 'backfills', path: '/executions/backfills' },
    { name: 'run', path: '__best_run__' },
    { name: 'source-wizard-types', path: '/components/sources', actions: async (page) => { await openWizard(page, /new source/i) } },
    { name: 'source-wizard-assets', path: '/components/sources', actions: async (page) => {
        await openWizard(page, /new source/i); await pickTile(page, 'Facebook Ads')
        await dialog(page).getByText('Select all', { exact: true }).click(); await page.waitForTimeout(500)
    } },
    { name: 'source-wizard-connection', path: '/components/sources', actions: async (page) => {
        await openWizard(page, /new source/i); await pickTile(page, 'Facebook Ads')
        await dialog(page).getByText('Select all', { exact: true }).click(); await page.waitForTimeout(400); await next(page)
    } },
    { name: 'source-wizard-details', path: '/components/sources', actions: async (page) => {
        await openWizard(page, /new source/i); await pickTile(page, 'Demo Source')
        await dialog(page).getByText('Select all', { exact: true }).click(); await page.waitForTimeout(400); await next(page); await next(page)
    } },
    { name: 'connection-form', path: '/components/connections', actions: async (page) => {
        await openWizard(page, /new connection/i); await pickTile(page, 'Facebook Ads'); await page.waitForTimeout(800)
    } },
]

const browser = await chromium.launch({ channel: 'chrome', headless: true })

/** The seeded organisation: its id addresses the API, its slug the app's pages. */
async function organisation(page) {
    const res = await page.request.get(`${base}/api/auth/me`)
    const [membership] = (await res.json()).memberships
    return membership
}

async function bestRunPath(page, orgId) {
    const res = await page.request.get(`${base}/api/organisations/${orgId}/runs`)
    const runs = (await res.json()).items
    const name = (r) => r.target?.name ?? r.target_name ?? r.component_name ?? ''
    const pick = runs.find(r => r.status === 'success' && name(r) === 'Demo Data') ?? runs.find(r => r.status === 'success') ?? runs[0]
    return pick ? `/executions/runs/${pick.id}` : '/executions'
}

for (const scheme of ['light', 'dark']) {
    const ctx = await browser.newContext({ viewport: { width: 1440, height: 900 }, deviceScaleFactor: 2, colorScheme: scheme })
    await ctx.addCookies([{ name: 'session_token', value: token, domain: 'localhost', path: '/' }])
    await ctx.addInitScript((scheme) => { try { localStorage.setItem('nuxt-color-mode', scheme) } catch {} }, scheme)
    const page = await ctx.newPage()
    const org = await organisation(page)
    for (const shot of shots) {
        if (only && !only.split(',').includes(shot.name)) continue
        const target = `/${org.slug}${shot.path === '__best_run__' ? await bestRunPath(page, org.id) : shot.path}`
        try {
            await page.goto(`${base}${target}`, { waitUntil: 'networkidle', timeout: 60000 })
            await page.waitForTimeout(1200)
            if (shot.actions) await shot.actions(page)
            await page.screenshot({ path: path.join(outDir, `${shot.name}-${scheme}.png`) })
            console.log('ok', shot.name, scheme, target)
        } catch (e) {
            console.log('FAIL', shot.name, scheme, e.message.split('\n')[0])
            await page.screenshot({ path: path.join(outDir, `${shot.name}-${scheme}-FAILED.png`) }).catch(() => {})
        }
    }
    await ctx.close()
}
await browser.close()
