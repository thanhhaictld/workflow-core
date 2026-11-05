import { DefinitionDetail } from "@/components/definition-detail";

export default async function Page({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <DefinitionDetail id={id} />;
}
