import { type Connection, type ConnectionSource, listConnectionSources, listConnections } from "$lib/api/connections";
import { getWorkspace, type Workspace, type WorkspaceSource } from "$lib/api/workspaces";

export type WorkspaceSourceDirectoryEntry = {
  attachment: WorkspaceSource;
  connection: Connection;
  source: ConnectionSource;
};

export type WorkspaceSourceDirectory = {
  workspace: Workspace;
  sources: WorkspaceSourceDirectoryEntry[];
};

export async function loadWorkspaceSourceDirectory(
  workspaceId: string,
  signal?: AbortSignal,
): Promise<WorkspaceSourceDirectory> {
  const [workspace, connections] = await Promise.all([getWorkspace(workspaceId, signal), listConnections(signal)]);
  const sourceEntries = await Promise.all(
    connections.map(async (connection) => ({
      connection,
      sources: await listConnectionSources(connection.id, signal),
    })),
  );
  const sourceDirectory = new Map(
    sourceEntries.flatMap(({ connection, sources }) =>
      sources.map((source) => [source.id, { connection, source }] as const),
    ),
  );
  const sources = workspace.sources.flatMap((attachment): WorkspaceSourceDirectoryEntry[] => {
    const directory = sourceDirectory.get(attachment.sourceId);
    return directory ? [{ attachment, ...directory }] : [];
  });
  return { workspace, sources };
}
