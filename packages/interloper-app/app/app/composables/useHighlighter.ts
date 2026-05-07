import { createHighlighter, type Highlighter } from 'shiki'

let highlighterPromise: Promise<Highlighter> | null = null

function getHighlighter(): Promise<Highlighter> {
    if (!highlighterPromise) {
        highlighterPromise = createHighlighter({
            themes: ['github-dark'],
            langs: ['python'],
        })
    }
    return highlighterPromise
}

export function useHighlighter() {
    const html = ref('')

    async function highlight(code: string, lang: string = 'python') {
        const highlighter = await getHighlighter()
        html.value = highlighter.codeToHtml(code, {
            lang,
            theme: 'github-dark',
        })
    }

    return { html, highlight }
}
