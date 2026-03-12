# Instagram External Publisher (GitHub Actions)

## Как работает

1. РФ-сервер кладет задачи в `queue/instagram/*.json`.
2. GitHub Action (`.github/workflows/instagram-publisher.yml`) запускается каждые 15 минут или вручную.
3. Скрипт `publisher/instagram_publisher.py`:
- читает задачи,
- публикует в Instagram через Graph API (`/media` -> `/media_publish`),
- переносит успешные задачи в `queue/instagram_done/`,
- переносит ошибки в `queue/instagram_failed/`,
- сохраняет `.meta.json` с результатом.

## Формат задачи

```json
{
  "caption": "Текст поста",
  "image_url": "https://your-domain.com/posts/2026-03-12/post.jpg",
  "publish_at": "2026-03-12T15:00:00Z"
}
```

- `image_url` обязателен.
- `publish_at` опционален. Если время еще не наступило, задача остается в очереди.

## GitHub Secrets/Vars

- `IG_USER_ID` (Secret)
- `IG_ACCESS_TOKEN` (Secret)
- `IG_GRAPH_HOST` (Variable, опционально: `graph.facebook.com` или `graph.instagram.com`)
- `IG_GRAPH_VERSION` (Variable, опционально, по умолчанию `v22.0`)

## Важно

- `image_url` должен быть публично доступен из интернета по HTTPS.
- Не используйте публичные прокси для токена Meta.
