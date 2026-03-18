USE oneapi;

DROP TABLE IF EXISTS abilities;
DROP TABLE IF EXISTS channels;
DROP TABLE IF EXISTS logs;

CREATE TABLE channels (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  `type` BIGINT NOT NULL DEFAULT 1,
  `key` LONGTEXT,
  openai_organization LONGTEXT,
  test_model LONGTEXT,
  status BIGINT NOT NULL DEFAULT 1,
  name VARCHAR(191) DEFAULT NULL,
  weight BIGINT NOT NULL DEFAULT 0,
  created_time BIGINT DEFAULT NULL,
  response_time BIGINT DEFAULT NULL,
  base_url VARCHAR(191) DEFAULT '',
  other LONGTEXT,
  model_mapping LONGTEXT,
  models LONGTEXT,
  `group` VARCHAR(64) DEFAULT 'default',
  priority BIGINT DEFAULT 0,
  auto_ban BIGINT DEFAULT 1,
  other_info LONGTEXT,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE abilities (
  `group` VARCHAR(64) NOT NULL,
  model VARCHAR(64) NOT NULL,
  channel_id BIGINT NOT NULL,
  enabled TINYINT(1) DEFAULT 1,
  priority BIGINT DEFAULT 0,
  weight BIGINT DEFAULT 0,
  tag VARCHAR(64) DEFAULT NULL,
  PRIMARY KEY (`group`, model, channel_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE logs (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  created_at BIGINT NOT NULL,
  type INT NOT NULL DEFAULT 2,
  content LONGTEXT,
  other LONGTEXT,
  channel_name LONGTEXT,
  is_stream TINYINT(1) DEFAULT NULL,
  username VARCHAR(128) DEFAULT NULL,
  user_id BIGINT DEFAULT NULL,
  token_name VARCHAR(128) DEFAULT NULL,
  token_id BIGINT DEFAULT NULL,
  model_name VARCHAR(128) DEFAULT NULL,
  quota BIGINT NOT NULL DEFAULT 0,
  prompt_tokens INT NOT NULL DEFAULT 0,
  completion_tokens INT NOT NULL DEFAULT 0,
  use_time INT NOT NULL DEFAULT 0,
  channel_id INT DEFAULT NULL,
  `group` VARCHAR(64) DEFAULT NULL,
  ip VARCHAR(64) DEFAULT NULL,
  request_id VARCHAR(128) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_created_at (created_at),
  KEY idx_token_id (token_id),
  KEY idx_user_id (user_id),
  KEY idx_model_name (model_name),
  KEY idx_group (`group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO channels (
  id, `type`, `key`, test_model, status, name, created_time, response_time, base_url, model_mapping, models, `group`, priority, auto_ban, other_info
) VALUES
  (1, 1, 'demo-key-1', 'gpt-4.1', 1, 'Demo OpenAI Channel', 1772300000, 800, 'https://api.openai.example', '{}', 'gpt-4.1,gpt-4.1-mini', 'coding', 10, 1, NULL),
  (2, 1, 'demo-key-2', 'claude-3.7-sonnet', 1, 'Demo Claude Channel', 1772300000, 920, 'https://api.anthropic.example', '{}', 'claude-3.7-sonnet,claude-3.5-haiku', 'coding', 20, 1, NULL),
  (3, 1, 'demo-key-3', 'gemini-2.0-flash', 1, 'Demo Gemini Channel', 1772300000, 740, 'https://api.gemini.example', '{}', 'gemini-2.0-flash,gemini-1.5-pro', 'default', 30, 1, NULL);

INSERT INTO abilities (`group`, model, channel_id, enabled, priority, weight, tag) VALUES
  ('coding', 'gpt-4.1', 1, 1, 10, 0, NULL),
  ('coding', 'gpt-4.1-mini', 1, 1, 10, 0, NULL),
  ('coding', 'claude-3.7-sonnet', 2, 1, 20, 0, NULL),
  ('coding', 'claude-3.5-haiku', 2, 1, 20, 0, NULL),
  ('default', 'gemini-2.0-flash', 3, 1, 30, 0, NULL),
  ('default', 'gemini-1.5-pro', 3, 1, 30, 0, NULL);

INSERT INTO logs (
  created_at, type, content, other, channel_name, is_stream, username, user_id, token_name, token_id, model_name,
  quota, prompt_tokens, completion_tokens, use_time, channel_id, `group`, ip, request_id
) VALUES
  (1772326800, 2, 'ok', '{}', 'Demo OpenAI Channel', 1, 'alice', 101, 'Team Alpha', 1001, 'gpt-4.1', 1800, 900, 300, 1120, 1, 'coding', '10.0.0.1', 'req-001'),
  (1772330400, 2, 'ok', '{}', 'Demo OpenAI Channel', 1, 'alice', 101, 'Team Alpha', 1001, 'gpt-4.1-mini', 2200, 1200, 350, 1240, 1, 'coding', '10.0.0.1', 'req-002'),
  (1772337600, 2, 'ok', '{}', 'Demo Claude Channel', 1, 'bob', 102, 'Team Beta', 1002, 'claude-3.7-sonnet', 2600, 1400, 420, 1320, 2, 'coding', '10.0.0.2', 'req-003'),
  (1772344800, 5, 'upstream did not return', '{"error_code":"channel:no_response","status_code":500}', 'Demo Gemini Channel', 0, 'carol', 103, 'Team Gamma', 1003, 'gemini-2.0-flash', 500, 200, 50, 340, 3, 'default', '10.0.0.3', 'req-004'),
  (1772348400, 2, 'ok', '{}', 'Demo Gemini Channel', 1, 'carol', 103, 'Team Gamma', 1003, 'gemini-1.5-pro', 1600, 850, 240, 920, 3, 'default', '10.0.0.3', 'req-005');
