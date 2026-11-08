import { HubDefinitionDetail } from "@/components/hub-definition-detail";

export default async function Page({
  params,
}: {
  params: Promise<{ hub: string; id: string }>;
}) {
  const { hub, id } = await params;
  return <HubDefinitionDetail hubId={hub} definitionId={id} />;
}
