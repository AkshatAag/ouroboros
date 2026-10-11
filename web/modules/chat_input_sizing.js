// Height of the Main/Project message field (DESIGN "message field"): it grows with
// its wrapped text up to the stylesheet's one max-height, then scrolls, and shrinks
// again. `data-sizing` is the one selected mode, read by CSS and script alike:
//   content — the engine sizes the field (`field-sizing: content`); no height writes.
//   fit / capped — an engine without it: the measured height in
//     `--chat-input-height`, below the cap (`fit`, overflow hidden, because
//     scrollHeight is a rounded integer and must not leave a sub-pixel scrollbar)
//     or at it (`capped`, the text scrolls).

export function chatInputSizingMode(css = globalThis.CSS) {
    return css?.supports?.('field-sizing', 'content') ? 'content' : 'measured';
}

const px = (value) => parseFloat(value) || 0;

export function attachChatInputSizing(input) {
    const mode = chatInputSizingMode();
    const measured = mode !== 'content';
    let disposed = false;
    // clientWidth the last measurement wrapped at; 0 when it could not measure.
    let width = 0;
    // A font finished loading: the same width may now wrap differently.
    let stale = false;
    let queued = false;
    let frame = 0;
    input.dataset.sizing = measured ? 'fit' : 'content';

    // Where an edit leaves the text: a caret at the end keeps the last line in view, an
    // empty field shows its placeholder from the top, any other caret stays where it was.
    const scrollAfterEdit = (previous) => !input.value ? 0
        : input.selectionEnd >= input.value.length - 1 ? input.scrollHeight : previous;

    function measure(edited) {
        // Hidden (a Project panel is built before it is shown): no width to wrap at,
        // so nothing is pinned now and the reveal measures.
        if (!(input.clientWidth > 0)) { width = 0; return; }
        stale = false;
        const style = getComputedStyle(input);
        const cap = parseFloat(style.maxHeight);  // NaN for `none`: never capped
        const box = style.boxSizing === 'border-box'
            ? px(style.borderTopWidth) + px(style.borderBottomWidth)
            : -(px(style.paddingTop) + px(style.paddingBottom));
        const scrollTop = input.scrollTop;
        // Measured at its natural one-row height without a scrollbar, which would
        // narrow the text it measures.
        input.dataset.sizing = 'fit';
        input.style.removeProperty('--chat-input-height');
        const needed = input.scrollHeight + box;
        const capped = needed > cap;
        input.style.setProperty('--chat-input-height', `${capped ? cap : needed}px`);
        input.dataset.sizing = capped ? 'capped' : 'fit';
        input.scrollTop = !capped ? 0 : edited ? scrollAfterEdit(scrollTop) : scrollTop;
        width = input.clientWidth;
    }

    // Without an edit, only a changed wrapping width (a resize, the busy Send label, a
    // reveal) or a loaded font re-measures: the height this module writes reports back
    // and is ignored. The write waits for the next frame, because inside the observer it
    // would resize the composer that chat.js observes higher in the same tree.
    const remeasure = () => {
        if (queued) return;
        queued = true;
        frame = requestAnimationFrame(() => {
            queued = false;
            if (!disposed && (stale || input.clientWidth !== width)) measure(false);
        });
    };
    // `loading` starts a loading period in every engine (WebKit sends no `loadingdone`);
    // `ready` settles once its faces are in.
    const fonts = measured ? globalThis.document?.fonts : null;
    const fontsLoading = () => fonts.ready.then(() => {
        if (disposed) return;
        stale = true;
        remeasure();
    });
    let observer = null;
    if (measured && typeof ResizeObserver === 'function') {
        observer = new ResizeObserver(remeasure);
        observer.observe(input);
    }
    fonts?.addEventListener('loading', fontsLoading);

    return {
        mode,
        // After a value change: typing, paste, a restored draft, a recalled or sent message.
        sync() {
            if (disposed) return;
            if (measured) measure(true);
            else input.scrollTop = scrollAfterEdit(input.scrollTop);
        },
        dispose() {
            disposed = true;
            observer?.disconnect();
            observer = null;
            fonts?.removeEventListener('loading', fontsLoading);
            if (queued) cancelAnimationFrame(frame);
            queued = false;
        },
    };
}
