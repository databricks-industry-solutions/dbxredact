export interface Config {
  config_id: string;
  name: string;
  use_presidio: boolean;
  use_ai_query: boolean;
  use_gliner: boolean;
  endpoint?: string;
  score_threshold: number;
  gliner_model: string;
  gliner_threshold: number;
  redaction_strategy: string;
  alignment_mode: string;
  extra_params?: Record<string, unknown>;
  created_at?: string;
  updated_at?: string;
}

export interface RunStatus {
  run_id: number;
  state?: string;
  result_state?: string;
  start_time?: number;
  end_time?: number;
  run_page_url?: string;
}

export interface JobHistoryItem {
  run_id: number;
  config_id: string;
  source_table: string;
  output_table: string;
  status: string;
  started_at?: string;
  completed_at?: string;
}

export interface Annotation {
  annotation_id?: string;
  doc_id: string;
  source_table: string;
  workflow: string;
  entity_text: string;
  entity_type: string;
  start: number;
  end_pos: number;
  action: string;
  corrected_type?: string;
  corrected_value?: string;
  detection_method?: string;
}

export interface ListEntry {
  entry_id?: string;
  value: string;
  is_pattern: boolean;
  entity_type?: string;
  notes?: string;
  list_type?: string;
}

export interface ABTest {
  test_id: string;
  name: string;
  config_a_id: string;
  config_b_id: string;
  source_table: string;
  sample_size: number;
  status: string;
  metrics_a?: Record<string, unknown>;
  metrics_b?: Record<string, unknown>;
  winner?: string;
  created_at?: string;
  completed_at?: string;
}

export interface ActiveLearnItem {
  doc_id: string;
  source_table: string;
  priority_score: number;
  status: string;
  assigned_to?: string;
  created_at?: string;
  reviewed_at?: string;
}

export interface ActiveLearnStats {
  total_queued: number;
  reviewed: number;
  pending: number;
  skipped: number;
  avg_priority?: number;
}
