export type Session = { csrf: string; username?: string; role?: string; mfaEnabled: boolean; mfaRequired: boolean; secure: boolean; canReadPayload: boolean };
export type Profile = { id: string; name: string; groupId: string; managementEndpoints: string[]; certificateAlias?: string };
export type Counter = number | string;
export type NodeStatus = {
  nodeId: string; groupId: string; applicationVersion: string; role: string; term: Counter; leaderId?: string;
  commitIndex: Counter; lastLogIndex: Counter; lastApplied: Counter; configurationIndex: Counter;
  voters: string[]; learners: string[]; peers: { nodeId: string; matchIndex: Counter; nextIndex: Counter; replicationMode: string; inFlightRequests: number; inFlightBytes: Counter; snapshotOffset: Counter }[];
  uncommittedBytes: Counter; snapshotIndex?: Counter; fault?: string;
};
export type Resources = { hostId: string; volumeId?: string; observedAt: string; sampleIntervalSeconds?: number; workingSetBytes: Counter; privateBytes: Counter; gcHeapBytes: Counter; cachedPayloadBytes: Counter;
  hostTotalMemoryBytes?: Counter; hostAvailableMemoryBytes?: Counter; volumeTotalBytes?: Counter; volumeAvailableBytes?: Counter; logBytes?: Counter; messageBytes?: Counter; imageBytes?: Counter; filesObservedAt?: string };
export type Node = { status: NodeStatus; resources: Resources; successfulPublications: Counter; publishedBytes: Counter; failedRequests: Counter; coreMetrics?: Record<string, { samples: Counter; total: number }> };
export type Entity = { entity: { name: string; mode: number; entityId: number; lastMessageId: Counter; retainedMessages: Counter; retainedBytes: Counter; publishedBytes: Counter };
  settings: { writesPaused: boolean; maxPayloadBytes: number; maxRetainedBytes?: Counter }; messagesPerSecond?: number };
export type Operation = { operationId: string; request: { kind: string; nodeId?: string; add?: { peer: { nodeId: string } } }; state: string; acceptedIndex: Counter; actor: string; result?: string; error?: string; membershipStage?: string; cancelError?: string };
export type Point = { at: string; messagesPerSecond?: number; bytesPerSecond?: number; p99Milliseconds?: number; workingSetBytes?: Counter | null; physicalStorageBytes?: Counter };
export type Observation = { id: string; name: string; groupId: string; observedAt?: string; error?: string; status?: NodeStatus; nodes: Node[]; entities: Entity[]; operations: Operation[]; history: Point[]; credentialExpiresAt?: string; staleAfterSeconds?: number };
export type TokenInfo = { tokenId: string; grant: { name: string; permissions: number; entities: string[]; expiresAt: string }; revoked: boolean; revision: Counter };
