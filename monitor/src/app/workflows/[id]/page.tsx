import { WorkflowTrace } from "@/components/workflow-trace";

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <WorkflowTrace workflowId={id} />;
}
