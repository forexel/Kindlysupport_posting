import { useEffect, useState } from 'react';
import { Link, useSearchParams } from 'react-router';
import { Button } from '../components/ui/button';
import { Card } from '../components/ui/card';
import { Label } from '../components/ui/label';
import { Checkbox } from '../components/ui/checkbox';
import { Input } from '../components/ui/input';
import { Textarea } from '../components/ui/textarea';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '../components/ui/select';
import { Separator } from '../components/ui/separator';
import { ArrowLeft, RefreshCw, Loader2, Send, Calendar } from 'lucide-react';
import { toast } from 'sonner';
import { api, mskIso } from '../lib/api';

interface Phrase {
  id: number;
  text_body: string;
}

interface Post {
  id: number;
  title: string;
  text_body: string;
  final_image_url?: string | null;
  selected_scenario?: string | null;
  telegram_caption?: string | null;
}

export function GeneratePostPage() {
  const [searchParams] = useSearchParams();
  const preselectedPhraseId = searchParams.get('phraseId') || '';

  const [mode, setMode] = useState<'random' | 'selected'>(preselectedPhraseId ? 'selected' : 'random');
  const [selectedPhrase, setSelectedPhrase] = useState(preselectedPhraseId);
  const [phraseQuery, setPhraseQuery] = useState('');
  const [phrases, setPhrases] = useState<Phrase[]>([]);
  const [postId, setPostId] = useState<number | null>(null);

  const [generatingText, setGeneratingText] = useState(false);
  const [generatingPrompt, setGeneratingPrompt] = useState(false);
  const [generatingImage, setGeneratingImage] = useState(false);

  const [postText, setPostText] = useState('');
  const [imagePrompt, setImagePrompt] = useState('');
  const [imageUrl, setImageUrl] = useState('');

  const [channels, setChannels] = useState({ telegram: true, vk: false, max: false, ok: false, pinterest: false, instagram: false });
  const [scheduleType, setScheduleType] = useState<'now' | 'scheduled'>('now');
  const [scheduleDate, setScheduleDate] = useState('');
  const [scheduleTime, setScheduleTime] = useState('');

  const filteredPhrases = phrases.filter((phrase) =>
    !phraseQuery.trim() || phrase.text_body.toLowerCase().includes(phraseQuery.trim().toLowerCase()),
  );

  useEffect(() => {
    (async () => {
      try {
        const rows = await api<Phrase[]>('/api/phrases?limit=1000');
        setPhrases(rows);
      } catch (e: any) {
        toast.error(String(e?.message || e));
      }
    })();
  }, []);

  const syncFromPost = (p: Post) => {
    setPostId(p.id);
    setPostText(p.text_body || '');
    setImagePrompt(p.selected_scenario || 'Стандартный реалистичный природный фон, спокойный, квадратный');
    setImageUrl(p.final_image_url || '');
  };

  const ensurePost = async (): Promise<number> => {
    if (postId) return postId;
    let p: Post;
    if (mode === 'random') {
      p = await api<Post>('/api/phrases/create-post-random?only_new=true', 'POST', {});
    } else {
      if (!selectedPhrase) throw new Error('Выберите фразу');
      p = await api<Post>(`/api/phrases/${selectedPhrase}/create-post`, 'POST', {});
    }
    syncFromPost(p);
    return p.id;
  };

  const handleGenerateText = async () => {
    setGeneratingText(true);
    try {
      const id = await ensurePost();
      const p = await api<Post>(`/api/posts/${id}/regenerate`, 'POST', { target: 'text', instruction: 'Сделай текст более живым и публикуемым' });
      syncFromPost(p);
      toast.success('Текст обновлён');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingText(false);
    }
  };

  const handleGeneratePrompt = async () => {
    setGeneratingPrompt(true);
    try {
      const id = await ensurePost();
      const res = await api<{ scenarios: string[] }>(`/api/posts/${id}/image-scenarios`, 'POST', { force_default: false });
      const sc = (res.scenarios || [])[0] || '';
      setImagePrompt(sc);
      toast.success('Сценарий изображения сгенерирован');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingPrompt(false);
    }
  };

  const handleGenerateImage = async () => {
    setGeneratingImage(true);
    try {
      const id = await ensurePost();
      const p = await api<Post>(`/api/posts/${id}/preview`, 'POST', { scenario: imagePrompt, regen_instruction: '' });
      syncFromPost(p);
      toast.success('Изображение/превью обновлено');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingImage(false);
    }
  };

  const handleRegenerateAll = async () => {
    setGeneratingText(true);
    setGeneratingImage(true);
    try {
      const id = await ensurePost();
      const p = await api<Post>(`/api/posts/${id}/regenerate`, 'POST', { target: 'both', instruction: 'Освежи текст и картинку для соцсетей' });
      syncFromPost(p);
      const p2 = await api<Post>(`/api/posts/${id}/preview`, 'POST', { scenario: imagePrompt, regen_instruction: 'Обновлённый вариант' });
      syncFromPost(p2);
      toast.success('Пост пересобран');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingText(false);
      setGeneratingImage(false);
    }
  };

  const handlePublish = async () => {
    try {
      const id = await ensurePost();
      const targets = Object.entries(channels).filter(([_, v]) => v).map(([k]) => k);
      if (!targets.length) return toast.error('Выберите хотя бы один канал');

      if (scheduleType === 'scheduled') {
        const iso = mskIso(scheduleDate, scheduleTime);
        if (!iso) return toast.error('Заполни дату и время');
        await api(`/api/posts/${id}/publish`, 'POST', { mode: 'schedule', scheduled_for: iso });
        toast.success(`Запланировано на ${iso}`);
        return;
      }

      if (targets.length === 1 && targets[0] === 'telegram') {
        await api(`/api/posts/${id}/publish`, 'POST', { mode: 'now' });
      } else {
        await api(`/api/posts/${id}/publish/multi`, 'POST', { targets });
      }
      toast.success(`Публикация отправлена: ${targets.join(', ')}`);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><Link to="/phrases" className="hover:text-zinc-200">Фразы</Link><span>/</span><span className="text-zinc-200">Генерация поста</span></div>

      <div className="flex items-center gap-4">
        <Link to="/phrases"><Button variant="ghost" size="sm" className="text-zinc-400 hover:text-zinc-200"><ArrowLeft className="h-4 w-4" /></Button></Link>
        <div><h1 className="text-3xl font-bold text-zinc-50">Генерация поста</h1><p className="text-zinc-400 mt-1">Создайте пост на основе фразы</p></div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="space-y-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-lg font-semibold text-zinc-50">Выбор фразы</h2>
            <div className="space-y-3">
              <div className="flex items-center space-x-2"><Checkbox id="mode-random" checked={mode === 'random'} onCheckedChange={(c) => c && setMode('random')} className="border-zinc-700" /><Label htmlFor="mode-random" className="text-zinc-300 cursor-pointer">Случайная новая фраза</Label></div>
              <div className="flex items-center space-x-2"><Checkbox id="mode-selected" checked={mode === 'selected'} onCheckedChange={(c) => c && setMode('selected')} className="border-zinc-700" /><Label htmlFor="mode-selected" className="text-zinc-300 cursor-pointer">Выбрать конкретную фразу</Label></div>
            </div>
            {mode === 'selected' && (
              <div className="space-y-2">
                <Input
                  value={phraseQuery}
                  onChange={(e) => setPhraseQuery(e.target.value)}
                  placeholder="Поиск фразы..."
                  className="bg-zinc-800 border-zinc-700 text-zinc-100 placeholder:text-zinc-500"
                />
                <Select value={selectedPhrase} onValueChange={setSelectedPhrase}>
                  <SelectTrigger className="bg-zinc-800 border-zinc-700 text-zinc-200"><SelectValue placeholder="Выберите фразу" /></SelectTrigger>
                  <SelectContent className="bg-zinc-900 border-zinc-800">
                    {filteredPhrases.map((phrase) => (
                      <SelectItem
                        key={phrase.id}
                        value={String(phrase.id)}
                        className="text-zinc-200 items-start whitespace-normal break-words py-2 leading-5"
                      >
                        <span className="line-clamp-2">{phrase.text_body}</span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <p className="text-xs text-zinc-500">Найдено: {filteredPhrases.length}</p>
              </div>
            )}
          </Card>

          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-zinc-50">Текст поста</h2>
              <Button size="sm" variant="outline" onClick={handleGenerateText} disabled={generatingText} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">{generatingText ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}</Button>
            </div>
            <Textarea value={postText} onChange={(e) => setPostText(e.target.value)} rows={5} className="bg-zinc-800 border-zinc-700 text-zinc-100 resize-none" />
          </Card>

          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-zinc-50">Сценарий изображения</h2>
              <Button size="sm" variant="outline" onClick={handleGeneratePrompt} disabled={generatingPrompt} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">{generatingPrompt ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}</Button>
            </div>
            <Textarea value={imagePrompt} onChange={(e) => setImagePrompt(e.target.value)} rows={3} className="bg-zinc-800 border-zinc-700 text-zinc-100 resize-none" />
          </Card>

          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold text-zinc-50">Изображение</h2>
              <Button size="sm" variant="outline" onClick={handleGenerateImage} disabled={generatingImage} className="bg-zinc-800 border-zinc-700 text-zinc-300 hover:bg-zinc-700">{generatingImage ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}</Button>
            </div>
            {generatingImage ? <div className="aspect-video bg-zinc-800 rounded-lg flex items-center justify-center"><Loader2 className="h-8 w-8 text-zinc-500 animate-spin" /></div> : imageUrl ? <img src={imageUrl} alt="Generated" className="w-full aspect-video object-cover rounded-lg" /> : <div className="aspect-video bg-zinc-800 rounded-lg" />}
          </Card>

          <Button onClick={handleRegenerateAll} className="w-full bg-blue-600 hover:bg-blue-700 text-white"><RefreshCw className="mr-2 h-4 w-4" />Перегенерировать всё</Button>
        </div>

        <div className="space-y-6">
          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-lg font-semibold text-zinc-50">Превью поста</h2>
            <div className="space-y-4">
              <div className="relative aspect-square rounded-lg overflow-hidden bg-zinc-800">
                {imageUrl && <img src={imageUrl} alt="Post preview" className="w-full h-full object-cover" />}
                <div className="absolute inset-0 bg-gradient-to-b from-transparent to-black/70 flex items-end p-6"><p className="text-white text-lg font-medium leading-relaxed">{postText.split(' ').slice(0, 15).join(' ')}...</p></div>
              </div>
              <div className="bg-zinc-800 p-4 rounded-lg"><p className="text-zinc-200 text-sm leading-relaxed whitespace-pre-wrap">{postText}</p></div>
            </div>
          </Card>

          <Card className="bg-zinc-900 border-zinc-800 p-6 space-y-4">
            <h2 className="text-lg font-semibold text-zinc-50">Публикация</h2>
            <div className="space-y-3">
              <Label className="text-zinc-200">Каналы</Label>
              <div className="space-y-2">
                {Object.entries(channels).map(([key, value]) => (
                  <div key={key} className="flex items-center space-x-2">
                    <Checkbox id={`channel-${key}`} checked={value} onCheckedChange={(c) => setChannels({ ...channels, [key]: c as boolean })} className="border-zinc-700" />
                    <Label htmlFor={`channel-${key}`} className="text-zinc-300 cursor-pointer capitalize">{key}</Label>
                  </div>
                ))}
              </div>
            </div>

            <Separator className="bg-zinc-800" />

            <div className="space-y-3">
              <div className="flex items-center space-x-2"><Checkbox id="schedule-now" checked={scheduleType === 'now'} onCheckedChange={(c) => c && setScheduleType('now')} className="border-zinc-700" /><Label htmlFor="schedule-now" className="text-zinc-300 cursor-pointer">Опубликовать сейчас</Label></div>
              <div className="flex items-center space-x-2"><Checkbox id="schedule-later" checked={scheduleType === 'scheduled'} onCheckedChange={(c) => c && setScheduleType('scheduled')} className="border-zinc-700" /><Label htmlFor="schedule-later" className="text-zinc-300 cursor-pointer">Запланировать</Label></div>
              {scheduleType === 'scheduled' && (
                <div className="grid grid-cols-2 gap-2 pt-2">
                  <div className="space-y-2"><Label className="text-xs text-zinc-400">Дата</Label><Input type="date" value={scheduleDate} onChange={(e) => setScheduleDate(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                  <div className="space-y-2"><Label className="text-xs text-zinc-400">Время (GMT+3)</Label><Input type="time" value={scheduleTime} onChange={(e) => setScheduleTime(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                </div>
              )}
            </div>

            <Button onClick={handlePublish} className="w-full bg-green-600 hover:bg-green-700 text-white">{scheduleType === 'now' ? <><Send className="mr-2 h-4 w-4" />Опубликовать</> : <><Calendar className="mr-2 h-4 w-4" />Запланировать</>}</Button>
          </Card>
        </div>
      </div>
    </div>
  );
}
