import { useEffect, useRef, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router';
import { ArrowLeft, ImageUp, Loader2, RefreshCw, Save, Send } from 'lucide-react';
import { toast } from 'sonner';
import { api, toDataUrl } from '../lib/api';
import { ImageWithFallback } from '../components/figma/ImageWithFallback';
import { Button } from '../components/ui/button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/card';
import { Input } from '../components/ui/input';
import { Label } from '../components/ui/label';
import { Textarea } from '../components/ui/textarea';
import { Badge } from '../components/ui/badge';

interface Post {
  id: number;
  title: string;
  text_body: string;
  status: string;
  source_kind: string;
  updated_at: string;
  scheduled_for?: string | null;
  final_image_url?: string | null;
  selected_scenario?: string | null;
  telegram_caption?: string | null;
}

export function PostDetailsPage() {
  const { postId } = useParams();
  const navigate = useNavigate();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [post, setPost] = useState<Post | null>(null);
  const [textBody, setTextBody] = useState('');
  const [regenInstruction, setRegenInstruction] = useState('');
  const [loading, setLoading] = useState(true);
  const [savingText, setSavingText] = useState(false);
  const [uploadingImage, setUploadingImage] = useState(false);
  const [publishing, setPublishing] = useState(false);
  const [regenMode, setRegenMode] = useState<'text' | 'image' | 'both' | null>(null);

  const numericPostId = Number(postId || 0);

  const loadPost = async () => {
    if (!numericPostId) return;
    setLoading(true);
    try {
      const row = await api<Post>(`/api/posts/${numericPostId}`);
      setPost(row);
      setTextBody(row.text_body || '');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadPost();
  }, [numericPostId]);

  const saveText = async () => {
    if (!post) return;
    setSavingText(true);
    try {
      const updated = await api<Post>(`/api/posts/${post.id}`, 'PUT', { text_body: textBody });
      setPost(updated);
      setTextBody(updated.text_body || '');
      toast.success('Текст сохранён');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setSavingText(false);
    }
  };

  const triggerUpload = () => fileInputRef.current?.click();

  const uploadImage = async (file: File) => {
    if (!post) return;
    setUploadingImage(true);
    try {
      const imageDataUrl = await toDataUrl(file, { maxSide: 1600, quality: 0.9 });
      const updated = await api<Post>(`/api/posts/${post.id}/upload-image`, 'POST', { image_data_url: imageDataUrl });
      setPost(updated);
      toast.success('Картинка заменена');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setUploadingImage(false);
      if (fileInputRef.current) fileInputRef.current.value = '';
    }
  };

  const regenerate = async (target: 'text' | 'image' | 'both') => {
    if (!post) return;
    setRegenMode(target);
    try {
      const instruction = regenInstruction.trim();
      const updated = await api<Post>(`/api/posts/${post.id}/regenerate`, 'POST', {
        target,
        instruction,
      });
      setPost(updated);
      setTextBody(updated.text_body || '');
      toast.success(
        target === 'text'
          ? 'Текст перегенерирован'
          : target === 'image'
            ? 'Картинка перегенерирована'
            : 'Текст и картинка перегенерированы',
      );
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setRegenMode(null);
    }
  };

  const publishNow = async () => {
    if (!post) return;
    setPublishing(true);
    try {
      const updated = await api<Post>(`/api/posts/${post.id}/publish`, 'POST', { mode: 'now' });
      setPost(updated);
      toast.success('Пост отправлен на публикацию');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setPublishing(false);
    }
  };

  const getStatusBadge = (status: string) => {
    const map: Record<string, string> = {
      published: 'bg-green-950/30 text-green-400 border-green-900/50',
      scheduled: 'bg-yellow-950/30 text-yellow-400 border-yellow-900/50',
      failed: 'bg-red-950/30 text-red-400 border-red-900/50',
      cancelled: 'bg-red-950/30 text-red-400 border-red-900/50',
      preview_ready: 'bg-blue-950/30 text-blue-400 border-blue-900/50',
      draft: 'bg-zinc-800 text-zinc-300 border-zinc-700',
    };
    return <Badge variant="outline" className={map[status] || map.draft}>{status}</Badge>;
  };

  if (!numericPostId) {
    return (
      <div className="space-y-4">
        <p className="text-zinc-400">Некорректный ID публикации.</p>
        <Button variant="outline" onClick={() => navigate('/publications')}>Назад к публикациям</Button>
      </div>
    );
  }

  if (loading && !post) {
    return (
      <div className="min-h-[40vh] flex items-center justify-center text-zinc-400">
        <Loader2 className="h-5 w-5 animate-spin mr-2" />
        Загружаю публикацию...
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400">
        <Link to="/publications" className="hover:text-zinc-200">Публикации</Link>
        <span>/</span>
        <span className="text-zinc-200">Пост #{post?.id}</span>
      </div>

      <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div className="flex items-center gap-3">
          <Button variant="ghost" size="sm" className="text-zinc-400 hover:text-zinc-200" onClick={() => navigate('/publications')}>
            <ArrowLeft className="h-4 w-4" />
          </Button>
          <div>
            <h1 className="text-3xl font-bold text-zinc-50">{post?.title || `Пост #${numericPostId}`}</h1>
            <div className="mt-2 flex items-center gap-3 text-sm text-zinc-400">
              {post ? getStatusBadge(post.status) : null}
              <span>Обновлён: {(post?.scheduled_for || post?.updated_at || '').replace('T', ' ').slice(0, 16)}</span>
            </div>
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button variant="outline" className="bg-zinc-900 border-zinc-700 text-zinc-100" onClick={loadPost}>
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
            Обновить
          </Button>
          <Button className="bg-blue-600 hover:bg-blue-700 text-white" onClick={publishNow} disabled={publishing || !post}>
            {publishing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            Опубликовать
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-[1.05fr_0.95fr] gap-6">
        <Card className="bg-zinc-900 border-zinc-800">
          <CardHeader className="border-b border-zinc-800">
            <CardTitle className="text-zinc-50">Картинка</CardTitle>
            <CardDescription className="text-zinc-400">Можно загрузить своё изображение. Для фраз поверх него будет собрана карточка с текстом.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-4 pt-6">
            <div className="overflow-hidden rounded-xl border border-zinc-800 bg-zinc-950">
              {post?.final_image_url ? (
                <ImageWithFallback
                  src={post.final_image_url}
                  alt={post.title}
                  className="h-auto w-full object-cover"
                />
              ) : (
                <div className="flex min-h-[420px] items-center justify-center text-zinc-500">Картинка пока не загружена</div>
              )}
            </div>
            <input
              ref={fileInputRef}
              type="file"
              accept="image/*"
              className="hidden"
              onChange={(e) => {
                const file = e.target.files?.[0];
                if (file) void uploadImage(file);
              }}
            />
            <div className="flex flex-wrap gap-2">
              <Button variant="outline" className="bg-zinc-800 border-zinc-700 text-zinc-100" onClick={triggerUpload} disabled={uploadingImage}>
                {uploadingImage ? <Loader2 className="h-4 w-4 animate-spin" /> : <ImageUp className="h-4 w-4" />}
                Загрузить
              </Button>
              <Button
                variant="outline"
                className="bg-zinc-800 border-zinc-700 text-zinc-100"
                onClick={() => void regenerate('image')}
                disabled={regenMode !== null || !post}
              >
                {regenMode === 'image' ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                Перегенерировать картинку
              </Button>
            </div>
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card className="bg-zinc-900 border-zinc-800">
            <CardHeader className="border-b border-zinc-800">
              <CardTitle className="text-zinc-50">Текст поста</CardTitle>
              <CardDescription className="text-zinc-400">Редактируй вручную и сохраняй без участия модели.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 pt-6">
              <div className="space-y-2">
                <Label htmlFor="post-text" className="text-zinc-300">Текст</Label>
                <Textarea
                  id="post-text"
                  value={textBody}
                  onChange={(e) => setTextBody(e.target.value)}
                  className="min-h-[320px] bg-zinc-950 border-zinc-700 text-zinc-100"
                />
              </div>
              <Button className="bg-emerald-600 hover:bg-emerald-700 text-white" onClick={saveText} disabled={savingText || !post}>
                {savingText ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                Сохранить текст
              </Button>
            </CardContent>
          </Card>

          <Card className="bg-zinc-900 border-zinc-800">
            <CardHeader className="border-b border-zinc-800">
              <CardTitle className="text-zinc-50">Перегенерация</CardTitle>
              <CardDescription className="text-zinc-400">Необязательная инструкция для модели. Если поле пустое, будет использован стандартный сценарий.</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 pt-6">
              <div className="space-y-2">
                <Label htmlFor="regen-instruction" className="text-zinc-300">Инструкция</Label>
                <Input
                  id="regen-instruction"
                  value={regenInstruction}
                  onChange={(e) => setRegenInstruction(e.target.value)}
                  placeholder="Например: сделай текст короче и спокойнее"
                  className="bg-zinc-950 border-zinc-700 text-zinc-100 placeholder:text-zinc-500"
                />
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="outline"
                  className="bg-zinc-800 border-zinc-700 text-zinc-100"
                  onClick={() => void regenerate('text')}
                  disabled={regenMode !== null || !post}
                >
                  {regenMode === 'text' ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                  Перегенерировать текст
                </Button>
                <Button
                  variant="outline"
                  className="bg-zinc-800 border-zinc-700 text-zinc-100"
                  onClick={() => void regenerate('both')}
                  disabled={regenMode !== null || !post}
                >
                  {regenMode === 'both' ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                  Перегенерировать всё
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card className="bg-zinc-900 border-zinc-800">
            <CardHeader className="border-b border-zinc-800">
              <CardTitle className="text-zinc-50">Служебная информация</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3 pt-6 text-sm text-zinc-300">
              <div><span className="text-zinc-500">ID:</span> {post?.id}</div>
              <div><span className="text-zinc-500">Источник:</span> {post?.source_kind}</div>
              <div><span className="text-zinc-500">Сценарий:</span> {post?.selected_scenario || 'нет'}</div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
