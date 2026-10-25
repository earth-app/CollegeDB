(() => {
	const MERMAID = 'https://cdn.jsdelivr.net/npm/mermaid@12.1.0/dist/mermaid.esm.min.mjs';
	const KATEX = 'https://cdn.jsdelivr.net/npm/katex@0.18.11/dist/';
	const blocks = [...document.querySelectorAll('pre > code.mermaid, pre > code.math')];
	const isMath = (code) => code.classList.contains('math');
	const dark = () =>
		document.documentElement.dataset.theme === 'dark' ||
		(document.documentElement.dataset.theme !== 'light' && matchMedia('(prefers-color-scheme: dark)').matches);
	let count = 0;

	async function draw(code, mermaid, katex) {
		const pre = code.parentElement;
		const view = document.createElement('div');
		view.className = 'docs-render';
		try {
			if (isMath(code)) katex.render(code.textContent, view, { displayMode: true, throwOnError: true });
			else {
				await mermaid.parse(code.textContent);
				view.innerHTML = (await mermaid.render(`docs-mermaid-${count++}`, code.textContent)).svg;
			}
			pre.nextElementSibling?.matches('.docs-render') && pre.nextElementSibling.remove();
			pre.after(view);
			pre.hidden = true;
		} catch (error) {
			pre.hidden = false;
			console.warn('docs block left as source', error);
		}
	}

	async function run() {
		const mermaid = blocks.some((code) => !isMath(code)) ? (await import(MERMAID)).default : null;
		const katex = blocks.some(isMath) ? (await import(`${KATEX}katex.min.mjs`)).default : null;
		if (katex) document.head.append(Object.assign(document.createElement('link'), { rel: 'stylesheet', href: `${KATEX}katex.min.css` }));
		const render = () => {
			mermaid?.initialize({
				startOnLoad: false,
				securityLevel: 'strict',
				suppressErrorRendering: true,
				theme: dark() ? 'dark' : 'default',
				// narrower sequence layout keeps three-participant diagrams at 0.55 scale or better at 390 px
				sequence: { width: 100, diagramMarginX: 20 }
			});
			return Promise.all(blocks.map((code) => draw(code, mermaid, katex)));
		};
		await render();
		new MutationObserver(render).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
		matchMedia('(prefers-color-scheme: dark)').addEventListener('change', render);
	}

	if (blocks.length) run().catch((error) => console.warn('docs render unavailable', error));
})();
