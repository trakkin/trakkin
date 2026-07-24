import { redirect } from "@sveltejs/kit";

export function load({ params }: { params: { workspaceId: string } }) {
  redirect(307, `/w/${params.workspaceId}/catalog`);
}
