// Unique ID for each Claude Code instance (generated on registration)
export type PeerId = string;

export interface Peer {
  id: PeerId;
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
  registered_at: string; // ISO timestamp
  last_seen: string; // ISO timestamp
  // B1: subagent virtual peer support
  virtual_peer: boolean; // true if this peer is a virtual subagent peer
  parent_id: PeerId | null; // parent main session peer id (null for non-virtual peers)
  role: string | null; // role label used to identify the subagent (e.g. "coordinator-boost")
  // Issue #2: set by the broker on list_peers responses when the entry is the
  // requesting caller (matched via ListPeersRequest.caller_id). Optional so
  // that older callers / callers that don't opt in still get compatible rows.
  is_self?: boolean;
}

export interface Message {
  id: number;
  from_id: PeerId;
  to_id: PeerId;
  text: string;
  sent_at: string; // ISO timestamp
  delivered: boolean;
}

// --- Broker API types ---

export interface RegisterRequest {
  pid: number;
  cwd: string;
  git_root: string | null;
  tty: string | null;
  summary: string;
}

export interface RegisterResponse {
  id: PeerId;
}

export interface HeartbeatRequest {
  id: PeerId;
}

export interface SetSummaryRequest {
  id: PeerId;
  summary: string;
}

export interface ListPeersRequest {
  scope: "machine" | "directory" | "repo";
  // The requesting peer's context (used for filtering)
  cwd: string;
  git_root: string | null;
  exclude_id?: PeerId;
  // Issue #2: when supplied, the broker will keep this peer in the result
  // (instead of dropping it) and set is_self=true on the matching entry so
  // the caller can identify itself without a separate whoami round-trip.
  // If BOTH exclude_id and caller_id are set with the same value, exclude wins.
  caller_id?: PeerId;
}

export interface SendMessageRequest {
  from_id: PeerId;
  to_id: PeerId;
  text: string;
}

export interface PollMessagesRequest {
  id: PeerId;
}

export interface PollMessagesResponse {
  messages: Message[];
}

// Day56 ack-based delivery (Option 1'): /poll-messages-v2 returns undelivered
// WITHOUT marking delivered; the caller acks each message after a successful push.
export interface AckMessageRequest {
  id: number; // messages.id to mark delivered
}

export interface AckMessageResponse {
  ok: boolean;
}

// --- B1: Virtual peer (subagent) registration ---

export interface RegisterVirtualPeerRequest {
  parent_id: PeerId;
  role: string; // subagent role identifier, e.g. "coordinator-boost"
  summary?: string; // optional initial summary
}

export interface RegisterVirtualPeerResponse {
  id: PeerId;
  created: boolean; // true if newly created, false if existing (idempotent)
}

export interface UnregisterVirtualPeerRequest {
  parent_id: PeerId;
  role?: string; // if omitted, unregisters all virtual peers under parent
}
