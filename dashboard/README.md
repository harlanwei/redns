# redns Dashboard

This is a static web app for the embedded `redns` dashboard.

Project layout:

- `src/index.html`: app shell
- `src/app.js`: client app and web component implementation
- `src/input.css`: Tailwind entrypoint plus component layers
- `dist/`: generated static assets embedded by the Rust server

Commands:

- `npm install`
- `npm run build`
- `npm run dev`

The Rust runtime embeds the generated files from `dist/` and serves them from:

- `/assets/dashboard.css`
- `/assets/dashboard.js`
- `/upstreams`, `/logs`, `/clients`
