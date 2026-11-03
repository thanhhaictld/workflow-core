import { WorkflowTrace } from "@/components/workflow-trace";
import { HubWorkflowTrace } from "@/components/hub-workflow-trace";

export default async function Page({
  params, searchParams,
}: {
  params: Promise<{ id: string }>;
  searchParams: Promise<{ source?: string }>;
}) {
  const { id } = await params;
  const { source } = await searchParams;
  return source === "hub"
    ? <HubWorkflowTrace workflowId={id}/>
    : <WorkflowTrace workflowId={id}/>;
}
