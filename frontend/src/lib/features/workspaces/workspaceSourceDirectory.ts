import { type Connection, type ConnectionSource, listConnectionSources, listConnections } from "$lib/api/connections";
import { getWorkspace, type Workspace, type WorkspaceSource } from "$lib/api/workspaces";

export type WorkspaceSourceDirectoryEntry = {
  attachment: WorkspaceSource;
  connection: Connection;
  source: ConnectionSource;
};

export type WorkspaceSourceDirectoryIssue = {
  connectionId: string;
  connectionName: string;
  message: string;
};

export type WorkspaceSourceDirectory = {
  workspace: Workspace;
  sources: WorkspaceSourceDirectoryEntry[];
  issues: WorkspaceSourceDirectoryIssue[];
};

export async function loadWorkspaceSourceDirectory(
  workspaceId: string,
  signal?: AbortSignal,
): Promise<WorkspaceSourceDirectory> {
  const [workspace, connections] = await Promise.all([getWorkspace(workspaceId, signal), listConnections(signal)]);
  const sourceResults = await Promise.allSettled(
    connections.map(async (connection) => ({
      connection,
      sources: await listConnectionSources(connection.id, signal),
    })),
  );
  signal?.throwIfAborted();
  const sourceDirectory = new Map(
    sourceResults.flatMap((result) =>
      result.status === "fulfilled"
        ? result.value.sources.map((source) => [source.id, { connection: result.value.connection, source }] as const)
        : [],
    ),
  );
  const issues = sourceResults.flatMap((result, index): WorkspaceSourceDirectoryIssue[] => {
    if (result.status === "fulfilled") return [];
    const connection = connections[index];
    if (!connection) return [];
    return [
      {
        connectionId: connection.id,
        connectionName: connection.displayName,
        message: result.reason instanceof Error ? result.reason.message : "Sources could not be loaded",
      },
    ];
  });
  const sources = workspace.sources.flatMap((attachment): WorkspaceSourceDirectoryEntry[] => {
    const directory = sourceDirectory.get(attachment.sourceId);
    return directory ? [{ attachment, ...directory }] : [];
  });
  return { workspace, sources, issues };
}
