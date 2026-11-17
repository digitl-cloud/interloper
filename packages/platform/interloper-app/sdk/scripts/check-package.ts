/**
 * Check `@interloper/ui` as npm installs it: packed as it is published, installed with npm
 * into a copy of `examples/extension` outside the pnpm workspace, and the copy's UI
 * type-checked and built. The SDK is built first (`pnpm --filter @interloper/ui build`).
 *
 * The temporary project is removed when every step passes, and kept, its path printed,
 * when one fails.
 */
import { execFileSync } from 'node:child_process'
import { cpSync, existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, relative, resolve, sep } from 'node:path'

const SDK = resolve(import.meta.dirname, '..')
const EXAMPLE = resolve(SDK, '../../../../examples/extension')
const WORK = mkdtempSync(join(tmpdir(), 'interloper-ui-'))
const EXTENSION = join(WORK, 'extension')
const UI = join(EXTENSION, 'ui')
const MANIFEST = join(EXTENSION, 'src/interloper_example/static/.vite/manifest.json')

try {
    run('pnpm', ['pack', '--pack-destination', WORK], SDK)
    cpSync(EXAMPLE, EXTENSION, { recursive: true, filter: source => !generated(source) })
    dependOn(tarball())
    run('npm', ['install', '--no-audit', '--no-fund'], UI)
    exportsExist()
    run('npm', ['run', 'typecheck'], UI)
    run('npm', ['run', 'build'], UI)
    if (!existsSync(MANIFEST))
        throw new Error(`the build wrote no Vite manifest: ${MANIFEST} is missing`)
}
catch (error) {
    console.error(`\n${(error as Error).message}`)
    console.error(`@interloper/ui as npm installs it fails; the project is kept in ${WORK}`)
    process.exit(1)
}
rmSync(WORK, { recursive: true, force: true })
console.log('\n@interloper/ui as npm installs it: the example extension type-checks and builds.')

/**
 * Run a command, its output shown as it runs.
 *
 * @param command The executable.
 * @param args Its arguments.
 * @param cwd Where it runs.
 */
function run(command: string, args: string[], cwd: string): void {
    execFileSync(command, args, { cwd, stdio: 'inherit' })
}

/**
 * Whether a path of the example is the output of an install or a build, left out of the copy.
 *
 * @param source A path inside `examples/extension`.
 * @returns True for `ui/node_modules` and a package's `static/`, and what they contain.
 */
function generated(source: string): boolean {
    const [top, second, third] = relative(EXAMPLE, source).split(sep)
    return (top === 'ui' && second === 'node_modules') || (top === 'src' && third === 'static')
}

/**
 * The tarball `pnpm pack` wrote.
 *
 * @returns Its path.
 */
function tarball(): string {
    const name = readdirSync(WORK).find(entry => entry.endsWith('.tgz'))
    if (!name) throw new Error(`pnpm pack wrote no tarball into ${WORK}`)
    return join(WORK, name)
}

/**
 * Fail unless every file the installed package's `exports` names is in it. The build and
 * the type check reach only some of them: the example type-checks `src/`, not the
 * `vite.config.ts` that imports the preset's declarations.
 */
function exportsExist(): void {
    const installed = join(UI, 'node_modules/@interloper/ui')
    const { exports } = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'))
    const missing = targets(exports).filter(target => !existsSync(join(installed, target)))
    if (missing.length)
        throw new Error(`@interloper/ui's exports name files the package lacks: ${missing.join(', ')}`)
}

/**
 * Every file an `exports` value names, through its conditions.
 *
 * @param value An `exports` field, a condition map or a target.
 * @returns The targets, relative to the package.
 */
function targets(value: unknown): string[] {
    if (typeof value === 'string') return [value]
    return Object.values(value as Record<string, unknown>).flatMap(targets)
}

/**
 * Make the copied UI depend on the tarball instead of the workspace's SDK.
 *
 * @param path The tarball.
 */
function dependOn(path: string): void {
    const file = join(UI, 'package.json')
    const manifest = JSON.parse(readFileSync(file, 'utf8'))
    manifest.devDependencies['@interloper/ui'] = `file:${path}`
    writeFileSync(file, `${JSON.stringify(manifest, null, 2)}\n`)
}
