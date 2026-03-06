/** @type {import('tailwindcss').Config} */
export default {
  content: ["./src/index.html", "./src/app.js"],
  theme: {
    extend: {
      colors: {
        ink: {
          50: "#f4f7fb",
          100: "#e8eef7",
          400: "#64748b",
          500: "#42536b",
          700: "#17314f",
          900: "#0a1f3d"
        },
        tide: {
          50: "#eef3f9",
          100: "#d8e3f0",
          300: "#8da8c5",
          500: "#2b527f",
          700: "#173355"
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
        panel: "0 20px 48px rgba(10, 31, 61, 0.08)",
        glow: "0 18px 42px rgba(23, 51, 85, 0.12)"
      }
    }
  },
  plugins: []
};
