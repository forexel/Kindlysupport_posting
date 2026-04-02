import { useEffect, useState } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Textarea } from '../components/ui/textarea';
import { Label } from '../components/ui/label';
import { Card } from '../components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { Checkbox } from '../components/ui/checkbox';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '../components/ui/dialog';
import { Plus, Link as LinkIcon, Loader2, Image as ImageIcon, Send, Calendar, BookOpen, Trash2, ScanText } from 'lucide-react';
import { toast } from 'sonner';
import { api, mskIso, toDataUrl } from '../lib/api';

interface ParablePost {
  id: number;
  title: string;
  text_body: string;
  created_at: string;
  source_url?: string | null;
  category?: string | null;
}

interface PostPreview {
  id: number;
  title: string;
  text_body: string;
  final_image_url?: string;
  preview_payload?: {
    social_image_url?: string;
  } | null;
}

const initialChannels = {
  telegram: true,
  vk: false,
  vk_channel: false,
  max: false,
  ok: false,
  pinterest: false,
  instagram: false,
};

export function ParablesPage() {
  const [parables, setParables] = useState<ParablePost[]>([]);
  const [search, setSearch] = useState('');
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [generateDialogOpen, setGenerateDialogOpen] = useState(false);

  const [title, setTitle] = useState('');
  const [text, setText] = useState('');
  const [url, setUrl] = useState('');
  const [recognizing, setRecognizing] = useState(false);
  const [ocrFiles, setOcrFiles] = useState<File[]>([]);

  const [generatingImage, setGeneratingImage] = useState(false);
  const [savingPost, setSavingPost] = useState(false);
  const [postText, setPostText] = useState('');
  const [telegramImageUrl, setTelegramImageUrl] = useState('');
  const [socialImageUrl, setSocialImageUrl] = useState('');
  const [selectedParable, setSelectedParable] = useState<ParablePost | null>(null);
  const [selectedPostId, setSelectedPostId] = useState<number | null>(null);

  const [channels, setChannels] = useState(initialChannels);
  const [scheduleType, setScheduleType] = useState<'now' | 'scheduled'>('now');
  const [scheduleDate, setScheduleDate] = useState('');
  const [scheduleTime, setScheduleTime] = useState('');

  const loadParables = async (query = '') => {
    try {
      const qs = new URLSearchParams({ limit: '50' });
      if (query.trim()) qs.set('search', query.trim());
      const rows = await api<ParablePost[]>(`/api/parables?${qs.toString()}`);
      setParables(rows);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  useEffect(() => {
    const timer = window.setTimeout(() => {
      loadParables(search);
    }, 300);
    return () => window.clearTimeout(timer);
  }, [search]);

  useEffect(() => {
    loadParables('');
  }, []);

  const resetCreateForm = () => {
    setTitle('');
    setText('');
    setUrl('');
    setOcrFiles([]);
  };

  const syncPreviewState = (post: PostPreview, parable?: ParablePost | null) => {
    setSelectedParable(parable || selectedParable);
    setSelectedPostId(post.id);
    setPostText(post.text_body || '');
    setTelegramImageUrl(post.final_image_url || '');
    setSocialImageUrl(post.preview_payload?.social_image_url || post.final_image_url || '');
  };

  const handleCreateParable = async (mode: 'manual' | 'link') => {
    try {
      const images = ocrFiles.length ? await Promise.all(ocrFiles.map((file) => toDataUrl(file))) : [];
      const payload =
        mode === 'manual'
          ? { mode, title, text_body: text, images }
          : { mode, title, url, text_body: text };
      const parable = await api<ParablePost>('/api/parables', 'POST', payload);
      const post = await api<PostPreview>(`/api/parables/${parable.id}/posts`, 'POST');
      toast.success('Притча создана');
      syncPreviewState(post, parable);
      setCreateDialogOpen(false);
      setGenerateDialogOpen(true);
      resetCreateForm();
      await loadParables(search);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleRecognizeFromUrl = async () => {
    if (!url) return toast.error('Введите ссылку');
    setRecognizing(true);
    try {
      const res = await api<any>('/api/phrases/import-image-url', 'POST', { image_url: url });
      setTitle((prev) => prev || 'Притча');
      setText(String(res.ocr_text || '').trim());
      toast.success('Текст распознан');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setRecognizing(false);
    }
  };

  const handleRecognizeFromFiles = async (files: FileList | null) => {
    const nextFiles = Array.from(files || []).filter((file) => file.type.startsWith('image/'));
    setOcrFiles(nextFiles);
    await runOcrForFiles(nextFiles);
  };

  const runOcrForFiles = async (files: File[]) => {
    if (!files.length) return;
    setRecognizing(true);
    try {
      const images = await Promise.all(files.map((file) => toDataUrl(file)));
      const res = await api<any>('/api/parables/ocr-images-base64', 'POST', { images });
      if (!title.trim()) setTitle('Притча');
      setText(String(res.ocr_text || '').trim());
      toast.success(`Распознано изображений: ${res.images_processed || files.length}`);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setRecognizing(false);
    }
  };

  const handleRerunOcr = async () => {
    if (!ocrFiles.length) return toast.error('Сначала выбери изображения');
    await runOcrForFiles(ocrFiles);
  };

  const handleOpenGenerate = (parable: ParablePost) => {
    api<PostPreview>(`/api/parables/${parable.id}/posts`, 'POST')
      .then((post) => {
        syncPreviewState(post, parable);
        setGenerateDialogOpen(true);
      })
      .catch((e: any) => {
        toast.error(String(e?.message || e));
      });
  };

  const handleDeleteParable = async (parable: ParablePost) => {
    if (!window.confirm(`Удалить притчу "${parable.title}"?`)) return;
    try {
      await api(`/api/parables/${parable.id}`, 'DELETE');
      if (selectedParable?.id === parable.id) {
        setGenerateDialogOpen(false);
        setSelectedParable(null);
        setSelectedPostId(null);
      }
      toast.success('Притча удалена');
      await loadParables(search);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const savePostDraft = async () => {
    if (!selectedPostId) return;
    setSavingPost(true);
    try {
      const post = await api<PostPreview>(`/api/posts/${selectedPostId}`, 'PUT', {
        title: selectedParable?.title || '',
        text_body: postText,
      });
      syncPreviewState(post, selectedParable);
    } finally {
      setSavingPost(false);
    }
  };

  const handleGenerateImage = async () => {
    if (!selectedPostId) return;
    setGeneratingImage(true);
    try {
      await savePostDraft();
      const post = await api<PostPreview>(`/api/posts/${selectedPostId}/preview`, 'POST', {
        scenario: 'Кинематографичный, спокойный, реалистичный фон для длинной притчи',
        regen_instruction: '',
      });
      syncPreviewState(post, selectedParable);
      toast.success('Изображение сгенерировано');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingImage(false);
    }
  };

  const handlePublish = async () => {
    if (!selectedPostId) return;
    const targets = Object.entries(channels)
      .filter(([_, v]) => v)
      .map(([k]) => k);
    if (!targets.length) return toast.error('Выберите хотя бы один канал для публикации');
    try {
      await savePostDraft();
      if (scheduleType === 'scheduled') {
        const iso = mskIso(scheduleDate, scheduleTime);
        if (!iso) return toast.error('Заполни дату и время');
        await api(`/api/posts/${selectedPostId}/publish`, 'POST', { mode: 'schedule', scheduled_for: iso });
      } else if (targets.length === 1 && targets[0] === 'telegram') {
        await api(`/api/posts/${selectedPostId}/publish`, 'POST', { mode: 'now' });
      } else {
        await api(`/api/posts/${selectedPostId}/publish/multi`, 'POST', { targets });
      }
      toast.success('Публикация отправлена');
      setGenerateDialogOpen(false);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400">
        <span>Главная</span>
        <span>/</span>
        <span className="text-zinc-200">Притчи</span>
      </div>

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-3xl font-bold text-zinc-50">Притчи</h1>
          <p className="mt-1 text-zinc-400">Показано притч: {parables.length}</p>
        </div>
        <div className="w-full sm:max-w-md">
          <Input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Поиск по заголовку, тексту, категории..."
            className="border-zinc-700 bg-zinc-800 text-zinc-100"
          />
        </div>
        <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
          <DialogTrigger asChild>
            <Button className="bg-blue-600 text-white hover:bg-blue-700">
              <Plus className="mr-2 h-4 w-4" />
              Создать притчу
            </Button>
          </DialogTrigger>
          <DialogContent className="max-h-[90vh] max-w-2xl overflow-y-auto border-zinc-800 bg-zinc-900">
            <DialogHeader>
              <DialogTitle className="text-zinc-50">Создание притчи</DialogTitle>
            </DialogHeader>
            <Tabs defaultValue="manual" className="w-full">
              <TabsList className="border border-zinc-700 bg-zinc-800">
                <TabsTrigger value="manual" className="data-[state=active]:bg-zinc-700 data-[state=active]:text-zinc-100">
                  Ручной ввод
                </TabsTrigger>
                <TabsTrigger value="images" className="data-[state=active]:bg-zinc-700 data-[state=active]:text-zinc-100">
                  Фото и картинки
                </TabsTrigger>
                <TabsTrigger value="url" className="data-[state=active]:bg-zinc-700 data-[state=active]:text-zinc-100">
                  Распознать по ссылке
                </TabsTrigger>
              </TabsList>

              <TabsContent value="manual" className="mt-4 space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="parable-title" className="text-zinc-200">Заголовок</Label>
                  <Input id="parable-title" value={title} onChange={(e) => setTitle(e.target.value)} placeholder="Введите заголовок притчи..." className="border-zinc-700 bg-zinc-800 text-zinc-100" />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="parable-text" className="text-zinc-200">Текст</Label>
                  <Textarea id="parable-text" value={text} onChange={(e) => setText(e.target.value)} rows={12} className="resize-none border-zinc-700 bg-zinc-800 text-zinc-100" />
                </div>
                <div className="flex justify-end">
                  <Button onClick={() => handleCreateParable('manual')} className="bg-blue-600 text-white hover:bg-blue-700">Создать</Button>
                </div>
              </TabsContent>

              <TabsContent value="images" className="mt-4 space-y-4">
                <div className="space-y-2">
                  <Label className="text-zinc-200">Заголовок</Label>
                  <Input value={title} onChange={(e) => setTitle(e.target.value)} placeholder="Введите заголовок притчи..." className="border-zinc-700 bg-zinc-800 text-zinc-100" />
                </div>
                <div className="space-y-2">
                  <Label className="text-zinc-200">Изображения притчи</Label>
                  <Input type="file" multiple accept="image/*" onChange={(e) => handleRecognizeFromFiles(e.target.files)} className="border-zinc-700 bg-zinc-800 text-zinc-100" />
                  <p className="text-xs text-zinc-500">Можно выбрать несколько страниц. Они будут распознаны как один текст.</p>
                </div>
                {ocrFiles.length ? <p className="text-sm text-zinc-400">Файлов выбрано: {ocrFiles.length}</p> : null}
                <div className="space-y-2">
                  <Label className="text-zinc-200">Текст</Label>
                  <Textarea value={text} onChange={(e) => setText(e.target.value)} rows={12} className="resize-none border-zinc-700 bg-zinc-800 text-zinc-100" />
                </div>
                <div className="flex justify-between">
                  <Button type="button" variant="outline" onClick={handleRerunOcr} disabled={recognizing} className="border-zinc-700 bg-zinc-900 text-zinc-200 hover:bg-zinc-800">
                    {recognizing ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <ScanText className="mr-2 h-4 w-4" />}
                    Распознать заново
                  </Button>
                  <Button onClick={() => handleCreateParable('manual')} className="bg-blue-600 text-white hover:bg-blue-700">Создать</Button>
                </div>
              </TabsContent>

              <TabsContent value="url" className="mt-4 space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="parable-url" className="text-zinc-200">Ссылка на источник</Label>
                  <div className="flex gap-2">
                    <Input id="parable-url" value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://example.com/parable" className="border-zinc-700 bg-zinc-800 text-zinc-100" />
                    <Button onClick={handleRecognizeFromUrl} disabled={recognizing} className="bg-blue-600 text-white hover:bg-blue-700">
                      {recognizing ? <Loader2 className="h-4 w-4 animate-spin" /> : <LinkIcon className="h-4 w-4" />}
                    </Button>
                  </div>
                </div>
                <div className="space-y-2">
                  <Label className="text-zinc-200">Заголовок</Label>
                  <Input value={title} onChange={(e) => setTitle(e.target.value)} className="border-zinc-700 bg-zinc-800 text-zinc-100" />
                </div>
                <div className="space-y-2">
                  <Label className="text-zinc-200">Текст</Label>
                  <Textarea value={text} onChange={(e) => setText(e.target.value)} rows={12} className="resize-none border-zinc-700 bg-zinc-800 text-zinc-100" />
                </div>
                <div className="flex justify-end">
                  <Button onClick={() => handleCreateParable('link')} className="bg-blue-600 text-white hover:bg-blue-700">Создать</Button>
                </div>
              </TabsContent>
            </Tabs>
          </DialogContent>
        </Dialog>
      </div>

      <div className="grid grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-3">
        {parables.map((parable) => (
          <Card key={parable.id} className="space-y-4 border-zinc-800 bg-zinc-900 p-6 transition-colors hover:border-zinc-700">
            <div className="flex items-start justify-between">
              <BookOpen className="h-5 w-5 text-zinc-500" />
              <span className="text-xs text-zinc-500">{(parable.created_at || '').slice(0, 10)}</span>
            </div>
            <div>
              <h3 className="mb-2 text-lg font-semibold text-zinc-100">{parable.title}</h3>
              <p className="line-clamp-4 text-sm text-zinc-400">{parable.text_body}</p>
              {parable.category ? <p className="mt-2 text-xs text-zinc-500">{parable.category}</p> : null}
            </div>
            <div className="grid grid-cols-2 gap-2">
              <Button onClick={() => handleOpenGenerate(parable)} className="bg-blue-600 text-white hover:bg-blue-700">Генерировать</Button>
              <Button onClick={() => handleDeleteParable(parable)} variant="outline" className="border-red-900 bg-zinc-950 text-red-300 hover:bg-red-950/40">
                <Trash2 className="mr-2 h-4 w-4" />
                Удалить
              </Button>
            </div>
          </Card>
        ))}
      </div>

      <Dialog open={generateDialogOpen} onOpenChange={setGenerateDialogOpen}>
        <DialogContent className="max-h-[90vh] max-w-5xl overflow-y-auto border-zinc-800 bg-zinc-900">
          <DialogHeader>
            <DialogTitle className="text-zinc-50">{selectedParable?.title}</DialogTitle>
          </DialogHeader>
          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <div className="space-y-4">
              <div className="space-y-2">
                <Label className="text-zinc-200">Текст поста</Label>
                <Textarea value={postText} onChange={(e) => setPostText(e.target.value)} rows={14} className="resize-none border-zinc-700 bg-zinc-800 text-zinc-100" />
              </div>
              <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
                <Button onClick={handleGenerateImage} disabled={generatingImage || savingPost} className="bg-blue-600 text-white hover:bg-blue-700">
                  {generatingImage ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Генерация...</> : <><ImageIcon className="mr-2 h-4 w-4" />Сгенерировать изображение</>}
                </Button>
                <Button onClick={savePostDraft} disabled={savingPost || !selectedPostId} variant="outline" className="border-zinc-700 bg-zinc-900 text-zinc-100 hover:bg-zinc-800">
                  {savingPost ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                  Сохранить текст
                </Button>
              </div>

              {telegramImageUrl ? (
                <Card className="space-y-3 border-zinc-700 bg-zinc-800 p-4">
                  <Label className="block text-zinc-200">Telegram</Label>
                  <img src={telegramImageUrl} alt="Telegram preview" className="w-full rounded-lg object-contain" />
                </Card>
              ) : null}

              {socialImageUrl ? (
                <Card className="space-y-3 border-zinc-700 bg-zinc-800 p-4">
                  <Label className="block text-zinc-200">Instagram и другие соцсети</Label>
                  <img src={socialImageUrl} alt="Social preview" className="aspect-square w-full rounded-lg object-cover" />
                </Card>
              ) : null}
            </div>

            <div className="space-y-4">
              <Card className="border-zinc-700 bg-zinc-800 p-4">
                <Label className="mb-3 block text-zinc-200">Полный пост</Label>
                <div className="space-y-4 rounded-lg bg-zinc-900 p-4">
                  {telegramImageUrl ? <img src={telegramImageUrl} alt="Post preview" className="w-full rounded-lg object-contain" /> : null}
                  <div className="space-y-3">
                    <h3 className="text-xl font-semibold text-zinc-50">{selectedParable?.title}</h3>
                    <p className="whitespace-pre-wrap text-sm leading-7 text-zinc-300">{postText}</p>
                  </div>
                </div>
              </Card>

              <Card className="space-y-4 border-zinc-700 bg-zinc-800 p-4">
                <Label className="text-zinc-200">Каналы</Label>
                <div className="space-y-2">
                  {Object.entries(channels).map(([channel, checked]) => (
                    <div key={channel} className="flex items-center space-x-2">
                      <Checkbox id={`channel-${channel}`} checked={checked} onCheckedChange={(c) => setChannels({ ...channels, [channel]: c as boolean })} className="border-zinc-600" />
                      <Label htmlFor={`channel-${channel}`} className="cursor-pointer capitalize text-zinc-300">{channel}</Label>
                    </div>
                  ))}
                </div>

                <div className="space-y-2">
                  <div className="flex items-center space-x-2">
                    <Checkbox id="schedule-now" checked={scheduleType === 'now'} onCheckedChange={(c) => c && setScheduleType('now')} className="border-zinc-600" />
                    <Label htmlFor="schedule-now" className="cursor-pointer text-zinc-300">Опубликовать сейчас</Label>
                  </div>
                  <div className="flex items-center space-x-2">
                    <Checkbox id="schedule-later" checked={scheduleType === 'scheduled'} onCheckedChange={(c) => c && setScheduleType('scheduled')} className="border-zinc-600" />
                    <Label htmlFor="schedule-later" className="cursor-pointer text-zinc-300">Запланировать</Label>
                  </div>
                  {scheduleType === 'scheduled' ? (
                    <div className="grid grid-cols-2 gap-2 pt-2">
                      <Input type="date" value={scheduleDate} onChange={(e) => setScheduleDate(e.target.value)} className="border-zinc-600 bg-zinc-900 text-zinc-100" />
                      <Input type="time" value={scheduleTime} onChange={(e) => setScheduleTime(e.target.value)} className="border-zinc-600 bg-zinc-900 text-zinc-100" />
                    </div>
                  ) : null}
                </div>

                <Button onClick={handlePublish} className="w-full bg-green-600 text-white hover:bg-green-700">
                  {scheduleType === 'now' ? <><Send className="mr-2 h-4 w-4" />Опубликовать</> : <><Calendar className="mr-2 h-4 w-4" />Запланировать</>}
                </Button>
              </Card>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
