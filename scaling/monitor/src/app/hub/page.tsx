import { redirect } from "next/navigation";

// Preserve existing bookmarks while keeping a single workflow browser.
// Use the Hub filter on the unified Workflow runs page.
export default function HubShortcut() {
  redirect("/workflows?hub=hub");
}
