/** @type {import('tailwindcss').Config} */
export default {
  content: ["./src/index.html", "./src/app.js"],
  theme: {
    extend: {
      colors: {
        ink: {
          50: "#f8fafc",
          100: "#eef3fb",
          400: "#5f7391",
          500: "#41556f",
          700: "#203247",
          900: "#09111d"
        },
        tide: {
          300: "#6fd0df",
          500: "#1596aa",
          700: "#0f6e7d"
        },
        ember: {
          300: "#f3bb7a",
          500: "#dc7d2f",
          700: "#9f5014"
        }
      },
      fontFamily: {
        sans: ["IBM Plex Sans", "Source Sans 3", "Noto Sans", "sans-serif"],
        mono: ["IBM Plex Mono", "Fira Code", "monospace"]
      },
      boxShadow: {
        panel: "0 18px 40px rgba(9, 17, 29, 0.10)",
        glow: "0 10px 30px rgba(21, 150, 170, 0.18)"
      }
    }
  },
  plugins: []
};
