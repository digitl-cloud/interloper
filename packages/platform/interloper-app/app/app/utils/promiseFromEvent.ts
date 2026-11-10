export default function promiseFromEvent<T extends Event>(
    target: EventTarget,
    event: string,
    options?: AddEventListenerOptions
): Promise<T> {
    return new Promise((resolve, reject) => {
        const handler = (e: Event) => {
            target.removeEventListener(event, handler, options);
            resolve(e as T);
        };
        target.addEventListener(event, handler, options);
    });
}