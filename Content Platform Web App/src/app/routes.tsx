import { createBrowserRouter } from "react-router";
import { LoginPage } from "./pages/LoginPage";
import { MainLayout } from "./layouts/MainLayout";
import { PhrasesPage } from "./pages/PhrasesPage";
import { PhrasesImportPage } from "./pages/PhrasesImportPage";
import { GeneratePostPage } from "./pages/GeneratePostPage";
import { ParablesPage } from "./pages/ParablesPage";
import { MoviesPage } from "./pages/MoviesPage";
import { PublicationsPage } from "./pages/PublicationsPage";
import { SettingsPage } from "./pages/SettingsPage";
import { NotFound } from "./pages/NotFound";

export const router = createBrowserRouter([
  {
    path: "/login",
    Component: LoginPage,
  },
  {
    path: "/",
    Component: MainLayout,
    children: [
      { index: true, Component: PhrasesPage },
      { path: "phrases", Component: PhrasesPage },
      { path: "phrases/import", Component: PhrasesImportPage },
      { path: "phrases/generate", Component: GeneratePostPage },
      { path: "parables", Component: ParablesPage },
      { path: "movies", Component: MoviesPage },
      { path: "publications", Component: PublicationsPage },
      { path: "settings", Component: SettingsPage },
      { path: "*", Component: NotFound },
    ],
  },
]);
