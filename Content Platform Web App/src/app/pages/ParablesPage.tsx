import { useEffect, useState } from 'react';
import { Button } from '../components/ui/button';
import { Input } from '../components/ui/input';
import { Textarea } from '../components/ui/textarea';
import { Label } from '../components/ui/label';
import { Card } from '../components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../components/ui/tabs';
import { Checkbox } from '../components/ui/checkbox';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogTrigger } from '../components/ui/dialog';
import { Plus, Link as LinkIcon, Loader2, Image as ImageIcon, Send, Calendar, BookOpen } from 'lucide-react';
import { toast } from 'sonner';
import { api, mskIso } from '../lib/api';

interface ParablePost {
  id: number;
  title: string;
  text_body: string;
  created_at: string;
  final_image_url?: string | null;
}

export function ParablesPage() {
  const [parables, setParables] = useState<ParablePost[]>([]);
  const [createDialogOpen, setCreateDialogOpen] = useState(false);
  const [generateDialogOpen, setGenerateDialogOpen] = useState(false);

  const [title, setTitle] = useState('');
  const [text, setText] = useState('');
  const [url, setUrl] = useState('');
  const [recognizing, setRecognizing] = useState(false);

  const [generatingImage, setGeneratingImage] = useState(false);
  const [postText, setPostText] = useState('');
  const [imageUrl, setImageUrl] = useState('');
  const [selectedParable, setSelectedParable] = useState<ParablePost | null>(null);

  const [channels, setChannels] = useState({ telegram: true, vk: false, vk_channel: false, max: false, ok: false, pinterest: false, instagram: false });
  const [scheduleType, setScheduleType] = useState<'now' | 'scheduled'>('now');
  const [scheduleDate, setScheduleDate] = useState('');
  const [scheduleTime, setScheduleTime] = useState('');

  const loadParables = async () => {
    try {
      const rows = await api<ParablePost[]>('/api/parables?limit=300');
      setParables(rows);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  useEffect(() => { loadParables(); }, []);

  const handleCreateParable = async (mode: 'manual' | 'link') => {
    try {
      const p = await api<ParablePost>('/api/parables', 'POST', mode === 'manual' ? { mode, title, text_body: text } : { mode, title, url, text_body: text });
      toast.success('Притча создана');
      setSelectedParable(p);
      setPostText(p.text_body || '');
      setImageUrl(p.final_image_url || '');
      setCreateDialogOpen(false);
      setGenerateDialogOpen(true);
      setTitle('');
      setText('');
      setUrl('');
      await loadParables();
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  const handleRecognizeFromUrl = async () => {
    if (!url) return toast.error('Введите ссылку');
    setRecognizing(true);
    try {
      const res = await api<any>('/api/phrases/import-image-url', 'POST', { image_url: url });
      const candidate = (res.phrases && res.phrases[0]) || (res.ocr_text || '').split('\n')[0] || '';
      setTitle((prev) => prev || 'Притча');
      setText(candidate || res.ocr_text || '');
      toast.success('Текст распознан');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setRecognizing(false);
    }
  };

  const handleOpenGenerate = (parable: ParablePost) => {
    setSelectedParable(parable);
    setPostText(parable.text_body || '');
    setImageUrl(parable.final_image_url || '');
    setGenerateDialogOpen(true);
  };

  const handleGenerateImage = async () => {
    if (!selectedParable) return;
    setGeneratingImage(true);
    try {
      const p = await api<any>(`/api/posts/${selectedParable.id}/preview`, 'POST', { scenario: 'Кинематографичный, спокойный, реалистичный фон', regen_instruction: '' });
      setImageUrl(p.final_image_url || '');
      setPostText(p.text_body || postText);
      toast.success('Изображение сгенерировано');
    } catch (e: any) {
      toast.error(String(e?.message || e));
    } finally {
      setGeneratingImage(false);
    }
  };

  const handlePublish = async () => {
    if (!selectedParable) return;
    const targets = Object.entries(channels).filter(([_, v]) => v).map(([k]) => k);
    if (!targets.length) return toast.error('Выберите хотя бы один канал для публикации');
    try {
      if (scheduleType === 'scheduled') {
        const iso = mskIso(scheduleDate, scheduleTime);
        if (!iso) return toast.error('Заполни дату и время');
        await api(`/api/posts/${selectedParable.id}/publish`, 'POST', { mode: 'schedule', scheduled_for: iso });
      } else {
        if (targets.length === 1 && targets[0] === 'telegram') await api(`/api/posts/${selectedParable.id}/publish`, 'POST', { mode: 'now' });
        else await api(`/api/posts/${selectedParable.id}/publish/multi`, 'POST', { targets });
      }
      toast.success('Публикация отправлена');
      setGenerateDialogOpen(false);
    } catch (e: any) {
      toast.error(String(e?.message || e));
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-zinc-400"><span>Главная</span><span>/</span><span className="text-zinc-200">Притчи</span></div>

      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div><h1 className="text-3xl font-bold text-zinc-50">Притчи</h1><p className="text-zinc-400 mt-1">Всего притч: {parables.length}</p></div>
        <Dialog open={createDialogOpen} onOpenChange={setCreateDialogOpen}>
          <DialogTrigger asChild><Button className="bg-blue-600 hover:bg-blue-700 text-white"><Plus className="mr-2 h-4 w-4" />Создать притчу</Button></DialogTrigger>
          <DialogContent className="bg-zinc-900 border-zinc-800 max-w-2xl max-h-[90vh] overflow-y-auto">
            <DialogHeader><DialogTitle className="text-zinc-50">Создание притчи</DialogTitle></DialogHeader>
            <Tabs defaultValue="manual" className="w-full">
              <TabsList className="bg-zinc-800 border border-zinc-700">
                <TabsTrigger value="manual" className="data-[state=active]:bg-zinc-700 data-[state=active]:text-zinc-100">Ручной ввод</TabsTrigger>
                <TabsTrigger value="url" className="data-[state=active]:bg-zinc-700 data-[state=active]:text-zinc-100">Распознать по ссылке</TabsTrigger>
              </TabsList>

              <TabsContent value="manual" className="space-y-4 mt-4">
                <div className="space-y-2"><Label htmlFor="parable-title" className="text-zinc-200">Заголовок</Label><Input id="parable-title" value={title} onChange={(e) => setTitle(e.target.value)} placeholder="Введите заголовок притчи..." className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                <div className="space-y-2"><Label htmlFor="parable-text" className="text-zinc-200">Текст</Label><Textarea id="parable-text" value={text} onChange={(e) => setText(e.target.value)} rows={12} className="bg-zinc-800 border-zinc-700 text-zinc-100 resize-none" /></div>
                <div className="flex justify-end"><Button onClick={() => handleCreateParable('manual')} className="bg-blue-600 hover:bg-blue-700 text-white">Создать</Button></div>
              </TabsContent>

              <TabsContent value="url" className="space-y-4 mt-4">
                <div className="space-y-2">
                  <Label htmlFor="parable-url" className="text-zinc-200">Ссылка на источник</Label>
                  <div className="flex gap-2">
                    <Input id="parable-url" value={url} onChange={(e) => setUrl(e.target.value)} placeholder="https://example.com/parable" className="bg-zinc-800 border-zinc-700 text-zinc-100" />
                    <Button onClick={handleRecognizeFromUrl} disabled={recognizing} className="bg-blue-600 hover:bg-blue-700 text-white">{recognizing ? <Loader2 className="h-4 w-4 animate-spin" /> : <LinkIcon className="h-4 w-4" />}</Button>
                  </div>
                </div>
                <div className="space-y-2"><Label className="text-zinc-200">Заголовок</Label><Input value={title} onChange={(e) => setTitle(e.target.value)} className="bg-zinc-800 border-zinc-700 text-zinc-100" /></div>
                <div className="space-y-2"><Label className="text-zinc-200">Текст</Label><Textarea value={text} onChange={(e) => setText(e.target.value)} rows={12} className="bg-zinc-800 border-zinc-700 text-zinc-100 resize-none" /></div>
                <div className="flex justify-end"><Button onClick={() => handleCreateParable('link')} className="bg-blue-600 hover:bg-blue-700 text-white">Создать</Button></div>
              </TabsContent>
            </Tabs>
          </DialogContent>
        </Dialog>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {parables.map((parable) => (
          <Card key={parable.id} className="bg-zinc-900 border-zinc-800 p-6 space-y-4 hover:border-zinc-700 transition-colors">
            <div className="flex items-start justify-between"><BookOpen className="h-5 w-5 text-zinc-500" /><span className="text-xs text-zinc-500">{(parable.created_at || '').slice(0,10)}</span></div>
            <div><h3 className="text-lg font-semibold text-zinc-100 mb-2">{parable.title}</h3><p className="text-zinc-400 text-sm line-clamp-3">{parable.text_body}</p></div>
            <Button onClick={() => handleOpenGenerate(parable)} className="w-full bg-blue-600 hover:bg-blue-700 text-white">Генерировать пост</Button>
          </Card>
        ))}
      </div>

      <Dialog open={generateDialogOpen} onOpenChange={setGenerateDialogOpen}>
        <DialogContent className="bg-zinc-900 border-zinc-800 max-w-4xl max-h-[90vh] overflow-y-auto">
          <DialogHeader><DialogTitle className="text-zinc-50">{selectedParable?.title}</DialogTitle></DialogHeader>
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div className="space-y-2"><Label className="text-zinc-200">Текст поста</Label><Textarea value={postText} onChange={(e) => setPostText(e.target.value)} rows={8} className="bg-zinc-800 border-zinc-700 text-zinc-100 resize-none" /></div>
              <Button onClick={handleGenerateImage} disabled={generatingImage} className="w-full bg-blue-600 hover:bg-blue-700 text-white">{generatingImage ? <><Loader2 className="mr-2 h-4 w-4 animate-spin" />Генерация...</> : <><ImageIcon className="mr-2 h-4 w-4" />Сгенерировать изображение</>}</Button>
              {imageUrl && <img src={imageUrl} alt="Generated" className="w-full aspect-square object-cover rounded-lg" />}
            </div>

            <div className="space-y-4">
              <Card className="bg-zinc-800 border-zinc-700 p-4">
                <Label className="text-zinc-200 mb-3 block">Превью поста</Label>
                {imageUrl ? <div className="relative aspect-square rounded-lg overflow-hidden"><img src={imageUrl} alt="Preview" className="w-full h-full object-cover" /><div className="absolute inset-0 bg-gradient-to-b from-transparent to-black/70 flex items-end p-4"><p className="text-white text-sm leading-relaxed line-clamp-3">{postText}</p></div></div> : <div className="aspect-square bg-zinc-900 rounded-lg flex items-center justify-center"><p className="text-zinc-500 text-sm">Изображение не сгенерировано</p></div>}
              </Card>

              <Card className="bg-zinc-800 border-zinc-700 p-4 space-y-4">
                <Label className="text-zinc-200">Каналы</Label>
                <div className="space-y-2">
                  {Object.entries(channels).map(([channel, checked]) => (
                    <div key={channel} className="flex items-center space-x-2">
                      <Checkbox id={`channel-${channel}`} checked={checked} onCheckedChange={(c) => setChannels({ ...channels, [channel]: c as boolean })} className="border-zinc-600" />
                      <Label htmlFor={`channel-${channel}`} className="text-zinc-300 capitalize cursor-pointer">{channel}</Label>
                    </div>
                  ))}
                </div>

                <div className="space-y-2">
                  <div className="flex items-center space-x-2"><Checkbox id="schedule-now" checked={scheduleType === 'now'} onCheckedChange={(c) => c && setScheduleType('now')} className="border-zinc-600" /><Label htmlFor="schedule-now" className="text-zinc-300 cursor-pointer">Опубликовать сейчас</Label></div>
                  <div className="flex items-center space-x-2"><Checkbox id="schedule-later" checked={scheduleType === 'scheduled'} onCheckedChange={(c) => c && setScheduleType('scheduled')} className="border-zinc-600" /><Label htmlFor="schedule-later" className="text-zinc-300 cursor-pointer">Запланировать</Label></div>
                  {scheduleType === 'scheduled' && <div className="grid grid-cols-2 gap-2 pt-2"><Input type="date" value={scheduleDate} onChange={(e) => setScheduleDate(e.target.value)} className="bg-zinc-900 border-zinc-600 text-zinc-100" /><Input type="time" value={scheduleTime} onChange={(e) => setScheduleTime(e.target.value)} className="bg-zinc-900 border-zinc-600 text-zinc-100" /></div>}
                </div>

                <Button onClick={handlePublish} className="w-full bg-green-600 hover:bg-green-700 text-white">{scheduleType === 'now' ? <><Send className="mr-2 h-4 w-4" />Опубликовать</> : <><Calendar className="mr-2 h-4 w-4" />Запланировать</>}</Button>
              </Card>
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </div>
  );
}
