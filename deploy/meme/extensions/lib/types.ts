export type MediaKind = "photo" | "video" | "animation";
export type SourceWeight = "high" | "normal" | "low";

export interface SourceEntry {
  username: string;
  weight: SourceWeight;
}

export interface PreviewMedia {
  kind: MediaKind;
  url: string;
  thumbnail?: string;
}

export interface PreviewPost {
  post_id: string;
  url: string;
  text: string;
  datetime?: string;
  views?: string;
  reactions?: string;
  media?: PreviewMedia;
}

export interface CandidateMedia {
  kind: MediaKind;
  path: string;
  sha256: string;
  bytes: number;
  mime: string;
}

export interface CandidateMetrics {
  views?: string;
  reactions?: string;
  datetime?: string;
}

export type CandidateStatus = "candidate" | "publishing" | "published" | "failed";

export interface Candidate {
  id: string;
  source: string;
  source_post_id: string;
  source_url: string;
  text: string;
  why_funny: string;
  media: CandidateMedia;
  metrics: CandidateMetrics;
  discovered_at: string;
  status: CandidateStatus;
  published_at?: string | null;
  target_chat_id?: number | null;
  telegram_message_id?: number | null;
  claimed_at?: string | null;
  error?: string | null;
}

export interface LedgerRecord {
  id: string;
  source: string;
  source_post_id: string;
  sha256: string;
  status: CandidateStatus | "duplicate";
  published_at?: string | null;
  first_seen: string;
}
