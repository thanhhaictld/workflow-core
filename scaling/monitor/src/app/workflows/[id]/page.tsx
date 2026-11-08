import { HubWorkflowTrace } from "@/components/hub-workflow-trace";

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <HubWorkflowTrace workflowId={id} />;
}
